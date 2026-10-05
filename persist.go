package cache

import (
	"fmt"
	"time"
)

// journalSeq assigns strictly increasing sequence numbers to appended
// records. Restore seeds it from the journal so a restarted process
// continues the same sequence.
func (c *Cache) nextSeq() uint64 {
	c.jmu.Lock()
	defer c.jmu.Unlock()
	c.journalSeq++
	return c.journalSeq
}

// appendRecord builds and durably appends one journal record. Journal
// failures never roll back a cache decision; they are remembered and
// reported through JournalErr so the caller can alert on them.
func (c *Cache) appendRecord(kind RecordKind, basis string, order []string,
	items map[string]ViewItem, deps map[string][]string, gens map[string]uint64) {
	if c.journal == nil {
		return
	}
	rec := JournalRecord{
		Seq:   c.nextSeq(),
		Kind:  kind,
		Time:  time.Now(),
		Basis: basis,
	}
	for _, k := range order {
		it := items[k]
		ir := ItemRecord{
			Key:           k,
			Value:         it.Value,
			SourceVersion: it.SourceVersion,
			Gen:           gens[k],
			Deps:          append([]string(nil), deps[k]...),
		}
		switch it.State {
		case ItemReady:
			ir.State = "ready"
		case ItemPending:
			ir.State = "pending"
		case ItemFailed:
			ir.State = "failed"
		}
		if it.Err != nil {
			ir.Err = it.Err.Error()
		}
		rec.Items = append(rec.Items, ir)
	}
	if err := c.journal.Append(rec); err != nil {
		c.jmu.Lock()
		c.journalErr = err
		c.jmu.Unlock()
		return
	}
	if kind == RecordConfirmed {
		c.jmu.Lock()
		cp := rec
		c.confirmed = &cp
		c.jmu.Unlock()
	}
}

// JournalErr reports the most recent journal append failure, if any.
func (c *Cache) JournalErr() error {
	c.jmu.Lock()
	defer c.jmu.Unlock()
	return c.journalErr
}

// Confirmed returns the last confirmed combined view this cache
// published (or restored), together with the basis that confirmed it.
// The returned view is immutable: it keeps the recorded values and
// source versions even if sources have advanced since. ok is false
// when nothing has been confirmed yet.
func (c *Cache) Confirmed() (view *View, basis string, ok bool) {
	c.jmu.Lock()
	rec := c.confirmed
	c.jmu.Unlock()
	if rec == nil {
		return nil, "", false
	}
	items := make(map[string]ViewItem, len(rec.Items))
	deps := make(map[string][]string, len(rec.Items))
	order := make([]string, 0, len(rec.Items))
	for _, ir := range rec.Items {
		order = append(order, ir.Key)
		items[ir.Key] = ViewItem{Key: ir.Key, State: ItemReady,
			Value: ir.Value, SourceVersion: ir.SourceVersion}
		deps[ir.Key] = append([]string(nil), ir.Deps...)
	}
	return &View{c: c, order: order, items: items, deps: deps}, rec.Basis, true
}

// applyConfirmedLocked installs a confirmed record as the public store
// content. Only ever called with a fully validated record; candidate
// records are never applied.
func (c *Cache) applyConfirmedLocked(rec *JournalRecord) {
	for _, ir := range rec.Items {
		e := Entry{
			Value:         ir.Value,
			Version:       ir.Gen,
			SourceVersion: ir.SourceVersion,
			Deps:          append([]string(nil), ir.Deps...),
		}
		if c.ttl > 0 {
			e.ExpiresAt = time.Now().Add(c.ttl)
		}
		c.setDepsLocked(ir.Key, e.Deps)
		c.store.Set(ir.Key, e)
		c.generations[ir.Key] = ir.Gen
		c.sourceVersions[ir.Key] = ir.SourceVersion
		c.touchLocked(ir.Key)
	}
	cp := *rec
	c.confirmed = &cp
	if rec.Seq > c.journalSeq {
		c.journalSeq = rec.Seq
	}
}

// Restore rebuilds a Cache from a journal: the most recent valid
// confirmed view becomes the store content, with every key's source
// version, dependency edges and local generation reinstated, so the
// process can explain exactly which complete combination it last
// published. Candidate records are validated but never published —
// a failed candidate's newer values stay audit-only.
//
// If the journal is corrupt, truncated, or its newest confirmed record
// is incomplete, Restore still returns a cache holding the last trusted
// combination, together with a non-nil error describing the damage.
func Restore(opts Options, j Journal) (*Cache, error) {
	if opts.Journal == nil {
		opts.Journal = j
	}
	recs, readErr := j.ReadAll()
	c := New(opts)
	var lastGood *JournalRecord
	var badConfirmed error
	for i := range recs {
		rec := recs[i]
		if rec.Seq > c.journalSeq {
			c.journalSeq = rec.Seq
		}
		if rec.Kind != RecordConfirmed {
			continue
		}
		if err := validateConfirmed(rec); err != nil {
			badConfirmed = fmt.Errorf("journal: %w", err)
			continue
		}
		cp := rec
		lastGood = &cp
		badConfirmed = nil // a later good record supersedes earlier damage
	}
	if lastGood != nil {
		c.mu.Lock()
		c.applyConfirmedLocked(lastGood)
		c.mu.Unlock()
	}
	if badConfirmed != nil {
		return c, badConfirmed
	}
	return c, readErr
}
