package engine

import (
	"fmt"
	"lamina/internal/index"
	"lamina/internal/journal"
	"lamina/internal/snapshot"
	"os"
	"path/filepath"
	"sort"
)

func (d *DB) image() snapshot.Image {
	return snapshot.Image{Format: 1, Sequence: d.sequence, Horizon: d.horizon, Catalog: d.catalog.Clone(), Store: d.store.Clone()}
}
func (d *DB) Checkpoint() (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return 0, err
	}
	if err := snapshot.Write(filepath.Join(d.dir, "checkpoint.lmi"), d.image()); err != nil {
		return 0, fault("unavailable", "checkpoint: %s", err)
	}
	return d.sequence, nil
}
func (d *DB) Backup() ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return nil, err
	}
	b, err := snapshot.Encode(d.image())
	if err != nil {
		return nil, err
	}
	if len(b) > snapshot.MaxImage {
		return nil, fault("capacity", "backup exceeds 128MiB")
	}
	return b, nil
}
func Restore(dir string, data []byte) error {
	image, err := snapshot.Decode(data)
	if err != nil {
		return err
	}
	if err = os.Mkdir(dir, 0700); err != nil {
		return fmt.Errorf("restore requires a new directory: %w", err)
	}
	installed := false
	defer func() {
		if !installed {
			os.Remove(filepath.Join(dir, "checkpoint.lmi"))
			os.Remove(filepath.Join(dir, "journal.lmw"))
			os.Remove(filepath.Join(dir, "lamina.lock"))
			os.Remove(dir)
		}
	}()
	w, err := journal.Open(dir, image.Sequence)
	if err != nil {
		return err
	}
	if err = snapshot.Write(filepath.Join(dir, "checkpoint.lmi"), image); err != nil {
		w.Close()
		return err
	}
	if err = w.Close(); err != nil {
		return err
	}
	installed = true
	return nil
}

// VacuumReport extends the version-store counts with the real ownership of
// the retention horizon: when readers block pruning, PinningReaders names the
// in-flight transactions whose snapshots hold history back.
type VacuumReport struct {
	Horizon        uint64      `json:"horizon"`
	Removed        int         `json:"removed"`
	Retained       int         `json:"retained"`
	Pinned         bool        `json:"pinned"`
	Requested      uint64      `json:"requested_horizon"`
	PinningReaders []ReaderPin `json:"pinning_readers"`
}

func (d *DB) Vacuum(requested uint64) (VacuumReport, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return VacuumReport{}, err
	}
	if requested > d.sequence || requested < d.horizon {
		return VacuumReport{}, fault("invalid", "vacuum horizon outside retained interval")
	}
	horizon := requested
	pinners := []ReaderPin{}
	ids := make([]string, 0, len(d.transactions))
	for id := range d.transactions {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		tx := d.transactions[id]
		if tx.Snapshot < horizon {
			horizon = tx.Snapshot
			if len(pinners) < maxRetainedPins {
				pinners = append(pinners, ReaderPin{Transaction: tx.ID, Snapshot: tx.Snapshot, ReadOnly: tx.ReadOnly})
			}
		}
	}
	beforeKeys, beforeVersions := d.store.Count()
	_ = beforeKeys
	if err := d.publish(journal.Record{Kind: "vacuum", Horizon: horizon}); err != nil {
		return VacuumReport{}, err
	}
	_, remaining := d.store.Count()
	d.indexes = index.Build(d.catalog, d.store)
	return VacuumReport{Horizon: horizon, Removed: beforeVersions - remaining, Retained: remaining, Pinned: horizon < requested, Requested: requested, PinningReaders: pinners}, nil
}
