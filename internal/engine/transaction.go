package engine

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"lamina/internal/datum"
	"lamina/internal/journal"
	"lamina/internal/versions"
	"sort"
	"time"
)

type staged struct {
	Mutation   versions.Mutation
	InsertOnly bool
}
type Transaction struct {
	ID       string `json:"id"`
	Snapshot uint64 `json:"snapshot"`
	ReadOnly bool   `json:"read_only"`
	Started  string `json:"started"`
	writes   map[string]map[string]staged
}
type Commit struct {
	Sequence uint64 `json:"sequence"`
	Writes   int    `json:"writes"`
	// Close is present on every terminal commit response, including empty
	// transactions, so callers can distinguish "nothing staged" from "private
	// writes released" and observe who still pins history.
	Close CloseReport `json:"close"`
}

func (d *DB) Begin(readOnly bool) (Transaction, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return Transaction{}, err
	}
	if len(d.transactions) >= 128 {
		return Transaction{}, fault("capacity", "128 active transactions reached")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return Transaction{}, err
	}
	tx := &Transaction{ID: hex.EncodeToString(id[:]), Snapshot: d.sequence, ReadOnly: readOnly, Started: time.Now().UTC().Format(time.RFC3339Nano), writes: map[string]map[string]staged{}}
	d.transactions[tx.ID] = tx
	out := *tx
	out.writes = nil
	return out, nil
}
func (d *DB) tx(id string) (*Transaction, error) {
	tx, ok := d.transactions[id]
	if !ok {
		return nil, fault("not_found", "transaction not active")
	}
	return tx, nil
}
func (d *DB) Put(id, table string, row datum.Row, insertOnly bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return err
	}
	tx, err := d.tx(id)
	if err != nil {
		return err
	}
	if tx.ReadOnly {
		return fault("conflict", "read-only transaction")
	}
	r, err := d.catalog.Relation(table)
	if err != nil {
		return invalid(err)
	}
	if r.Created > tx.Snapshot {
		return fault("conflict", "relation outside transaction catalog snapshot")
	}
	normalized, err := r.Normalize(row)
	if err != nil {
		return invalid(err)
	}
	key := r.Key(normalized)
	rows := d.store.Scan(table, tx.Snapshot)
	overlay(rows, tx.writes, table)
	if insertOnly {
		if _, ok := rows[key]; ok {
			return fault("conflict", "primary already exists")
		}
	}
	if tx.writes[table] == nil {
		tx.writes[table] = map[string]staged{}
	}
	if err = tx.reserve(table, key); err != nil {
		return err
	}
	tx.writes[table][key] = staged{Mutation: versions.Mutation{Relation: table, Key: key, Row: normalized}, InsertOnly: insertOnly}
	return nil
}
func (tx *Transaction) reserve(table, key string) error {
	if _, exists := tx.writes[table][key]; exists {
		return nil
	}
	count := 0
	for _, m := range tx.writes {
		count += len(m)
	}
	if count >= 1000 {
		return fault("capacity", "transaction mutation limit 1000")
	}
	return nil
}
func (d *DB) Delete(id, table string, value datum.Value) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return err
	}
	tx, err := d.tx(id)
	if err != nil {
		return err
	}
	if tx.ReadOnly {
		return fault("conflict", "read-only transaction")
	}
	r, err := d.catalog.Relation(table)
	if err != nil {
		return invalid(err)
	}
	if r.Created > tx.Snapshot {
		return fault("conflict", "relation outside transaction catalog snapshot")
	}
	col, _ := r.Column(r.Primary)
	if value.Null() || value.Kind != col.Type {
		return fault("invalid", "invalid primary value")
	}
	key := value.Key()
	if tx.writes[table] == nil {
		tx.writes[table] = map[string]staged{}
	}
	if err = tx.reserve(table, key); err != nil {
		return err
	}
	tx.writes[table][key] = staged{Mutation: versions.Mutation{Relation: table, Key: key, Deleted: true}}
	return nil
}

// Rollback closes a transaction with an explicit rolled_back outcome. The
// returned report enumerates the discarded private mutations (with bounded
// key references for immediate retry) and the retention ownership that
// remains after this transaction's snapshot pin is released.
func (d *DB) Rollback(id string) (CloseReport, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return CloseReport{}, err
	}
	tx, err := d.tx(id)
	if err != nil {
		return CloseReport{}, err
	}
	return d.closeLocked(tx, outcomeRolledBack, true), nil
}
func (d *DB) Commit(id string) (Commit, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return Commit{}, err
	}
	tx, err := d.tx(id)
	if err != nil {
		return Commit{}, err
	}
	writes := make([]versions.Mutation, 0)
	for table, mutations := range tx.writes {
		for key, m := range mutations {
			if d.store.Latest(table, key) > tx.Snapshot {
				d.conflicts++
				report := d.closeLocked(tx, outcomeConflict, true)
				return Commit{}, &Error{Code: "conflict", Message: fmt.Sprintf("write conflict on %s; transaction closed, private writes discarded", table), Detail: report}
			}
			writes = append(writes, m.Mutation)
		}
		if err = d.unique(table, tx.writes); err != nil {
			d.conflicts++
			report := d.closeLocked(tx, outcomeConflict, true)
			if domain, ok := err.(*Error); ok {
				domain.Detail = report
				return Commit{}, domain
			}
			return Commit{}, &Error{Code: "conflict", Message: err.Error(), Detail: report}
		}
	}
	if len(writes) == 0 {
		// An empty end is not a rollback: nothing was discarded, but the
		// snapshot pin is released all the same.
		report := d.closeLocked(tx, outcomeEmpty, false)
		return Commit{Sequence: tx.Snapshot, Writes: 0, Close: report}, nil
	}
	sort.Slice(writes, func(i, j int) bool {
		if writes[i].Relation == writes[j].Relation {
			return writes[i].Key < writes[j].Key
		}
		return writes[i].Relation < writes[j].Relation
	})
	if err = d.publish(journal.Record{Kind: "commit", Transaction: id, Writes: writes}); err != nil {
		// The durable outcome is uncertain. The instance is poisoned and will
		// reject further requests; this transaction cannot be reused. Its
		// private writes are not declared discarded because they may have been
		// made durable before the failure. Restart and reconcile.
		report := d.closeLocked(tx, outcomeDurableUncertain, false)
		if domain, ok := err.(*Error); ok {
			domain.Detail = report
		}
		return Commit{}, err
	}
	d.commits++
	report := d.closeLocked(tx, outcomeCommitted, false)
	return Commit{Sequence: d.sequence, Writes: len(writes), Close: report}, nil
}
