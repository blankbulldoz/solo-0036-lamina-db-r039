package engine

import (
	"sort"
	"time"
)

// Terminal transaction outcomes. They are deliberately distinct so a rollback,
// an empty end, a failed commit and a durable-success commit are never reported
// with one ambiguous "released" label.
const (
	outcomeRolledBack       = "rolled_back"
	outcomeEmpty            = "empty"
	outcomeCommitted        = "committed"
	outcomeConflict         = "commit_rejected"
	outcomeDurableUncertain = "durable_outcome_uncertain"
)

// Diagnostics are bounded: close history keeps only the most recent events and
// retained events never carry row payloads or primary-key material.
const (
	maxCloseHistory  = 32
	maxReportKeys    = 64
	maxRetainedPins  = 8
	maxRelationNotes = 16
)

// KeyRef names a private mutation without retaining the row payload. Primary
// keys are returned only in the immediate close response to the transaction
// owner, never in the shared diagnostic history.
type KeyRef struct {
	Relation string `json:"relation"`
	Key      string `json:"key"`
	Kind     string `json:"kind"`
}

// RelationDiscard counts discarded private mutations per relation.
type RelationDiscard struct {
	Relation string `json:"relation"`
	Puts     int    `json:"puts"`
	Deletes  int    `json:"deletes"`
}

// DiscardSummary explains which private writes were dropped. Row data is never
// retained here; key material appears only in the immediate response and is
// capped.
type DiscardSummary struct {
	Relations     []RelationDiscard `json:"relations"`
	Mutations     int               `json:"mutations"`
	Puts          int               `json:"puts"`
	Deletes       int               `json:"deletes"`
	Keys          []KeyRef          `json:"keys,omitempty"`
	KeysTruncated bool              `json:"keys_truncated"`
}

// ReaderPin identifies an in-flight read whose snapshot still protects
// historical versions after the closed transaction's own pin is gone.
type ReaderPin struct {
	Transaction string `json:"transaction"`
	Snapshot    uint64 `json:"snapshot"`
	ReadOnly    bool   `json:"read_only"`
}

// RetentionView describes the real ownership of historical versions at the
// instant a transaction closes: which commit sequence vacuum may reach and
// which readers are still pinning older history.
type RetentionView struct {
	RetentionFloor   uint64      `json:"retention_floor"`
	RetainedInterval [2]uint64   `json:"retained_interval"`
	ActiveReaders    int         `json:"active_readers"`
	PinningReaders   []ReaderPin `json:"pinning_readers"`
}

// CloseReport is the resource feedback for one transaction end. It is built
// inside the same critical section that removes the transaction, so the
// discarded-write summary and the retention view can never describe
// contradictory states.
type CloseReport struct {
	Transaction string         `json:"transaction"`
	Outcome     string         `json:"outcome"`
	ClosedAt    string         `json:"closed_at"`
	Snapshot    uint64         `json:"snapshot"`
	ReadOnly    bool           `json:"read_only"`
	Discarded   DiscardSummary `json:"discarded"`
	Retention   RetentionView  `json:"retention"`
}

// CloseEvent is the bounded, sanitized copy kept for diagnostics. It retains
// counts only: no row data and no key material survives past the response.
type CloseEvent struct {
	Transaction string         `json:"transaction"`
	Outcome     string         `json:"outcome"`
	ClosedAt    string         `json:"closed_at"`
	Snapshot    uint64         `json:"snapshot"`
	ReadOnly    bool           `json:"read_only"`
	Discarded   DiscardSummary `json:"discarded"`
	Retention   RetentionView  `json:"retention"`
}

func summarizeDiscard(tx *Transaction, includeKeys bool) DiscardSummary {
	summary := DiscardSummary{Relations: []RelationDiscard{}}
	names := make([]string, 0, len(tx.writes))
	for table := range tx.writes {
		names = append(names, table)
	}
	sort.Strings(names)
	for _, table := range names {
		mutations := tx.writes[table]
		note := RelationDiscard{Relation: table}
		for key, m := range mutations {
			summary.Mutations++
			if m.Mutation.Deleted {
				note.Deletes++
				summary.Deletes++
			} else {
				note.Puts++
				summary.Puts++
			}
			if includeKeys && len(summary.Keys) < maxReportKeys {
				kind := "put"
				if m.Mutation.Deleted {
					kind = "delete"
				}
				summary.Keys = append(summary.Keys, KeyRef{Relation: table, Key: key, Kind: kind})
			}
		}
		if len(summary.Relations) < maxRelationNotes {
			summary.Relations = append(summary.Relations, note)
		}
	}
	summary.KeysTruncated = includeKeys && summary.Mutations > len(summary.Keys)
	if len(summary.Relations) == 0 {
		summary.Relations = []RelationDiscard{}
	}
	if includeKeys && len(summary.Keys) == 0 {
		summary.Keys = []KeyRef{}
	}
	return summary
}

// retentionLocked reports who still owns history after the closing
// transaction. Callers must hold d.mu (the same lock that closes transactions
// and advances vacuum), which makes the view atomic with respect to state
// changes: an outcome and a retention view from one call can never form an
// impossible combination.
func (d *DB) retentionLocked(excludeID string) RetentionView {
	view := RetentionView{
		RetentionFloor:   d.sequence,
		RetainedInterval: [2]uint64{d.horizon, d.sequence},
		PinningReaders:   []ReaderPin{},
	}
	ids := make([]string, 0, len(d.transactions))
	for id := range d.transactions {
		if id == excludeID {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	view.ActiveReaders = len(ids)
	for _, id := range ids {
		other := d.transactions[id]
		if other.Snapshot < view.RetentionFloor {
			view.RetentionFloor = other.Snapshot
		}
		if len(view.PinningReaders) < maxRetainedPins {
			view.PinningReaders = append(view.PinningReaders, ReaderPin{
				Transaction: other.ID,
				Snapshot:    other.Snapshot,
				ReadOnly:    other.ReadOnly,
			})
		}
	}
	return view
}

// closeLocked removes the transaction and records a sanitized diagnostic
// event. includeKeys controls whether the returned report carries bounded
// primary-key references for the owner's immediate retry; the retained event
// always strips them.
func (d *DB) closeLocked(tx *Transaction, outcome string, includeKeys bool) CloseReport {
	discarded := summarizeDiscard(tx, includeKeys)
	retention := d.retentionLocked(tx.ID)
	report := CloseReport{
		Transaction: tx.ID,
		Outcome:     outcome,
		ClosedAt:    time.Now().UTC().Format(time.RFC3339Nano),
		Snapshot:    tx.Snapshot,
		ReadOnly:    tx.ReadOnly,
		Discarded:   discarded,
		Retention:   retention,
	}
	retained := CloseEvent{
		Transaction: report.Transaction,
		Outcome:     report.Outcome,
		ClosedAt:    report.ClosedAt,
		Snapshot:    report.Snapshot,
		ReadOnly:    report.ReadOnly,
		Discarded:   discarded,
		Retention:   retention,
	}
	// Never persist key material or row payloads in shared diagnostics.
	retained.Discarded.Keys = nil
	retained.Discarded.KeysTruncated = retained.Discarded.Mutations > 0
	d.closedHistory = append(d.closedHistory, retained)
	if len(d.closedHistory) > maxCloseHistory {
		d.closedHistory = d.closedHistory[len(d.closedHistory)-maxCloseHistory:]
	}
	delete(d.transactions, tx.ID)
	return report
}
