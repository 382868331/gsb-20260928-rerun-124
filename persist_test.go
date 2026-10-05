package cache

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// diamondDeps is the demo graph: leaf L feeds A and B, and R joins them.
var diamondDeps = map[string][]string{
	"L": nil,
	"A": {"L"},
	"B": {"L"},
	"R": {"A", "B"},
}

var diamondKeys = []string{"R", "A", "B", "L"}

// fakeSource is a controllable authoritative source with business
// versions and per-key failure switches.
type fakeSource struct {
	mu       sync.Mutex
	versions map[string]uint64
	fails    map[string]bool
	block    map[string]chan struct{}
}

func newFakeSource(v uint64) *fakeSource {
	s := &fakeSource{
		versions: make(map[string]uint64),
		fails:    make(map[string]bool),
		block:    make(map[string]chan struct{}),
	}
	for k := range diamondDeps {
		s.versions[k] = v
	}
	return s
}

func (s *fakeSource) setVersion(v uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.versions {
		s.versions[k] = v
	}
}

func (s *fakeSource) fail(key string, on bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fails[key] = on
}

func (s *fakeSource) loader(ctx context.Context, key string) (SourceResult, error) {
	s.mu.Lock()
	if ch := s.block[key]; ch != nil {
		s.mu.Unlock()
		<-ch // simulate a source that never answers in time
	} else {
		s.mu.Unlock()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fails[key] {
		return SourceResult{}, fmt.Errorf("source for %s is down", key)
	}
	v := s.versions[key]
	return SourceResult{
		Value:   fmt.Sprintf("%s-v%d", key, v),
		Version: v,
		Deps:    diamondDeps[key],
	}, nil
}

func newJournalCache(src *fakeSource, path string) *Cache {
	return New(Options{SourceLoader: src.loader, Journal: NewFileJournal(path)})
}

func restoreCache(t *testing.T, src *fakeSource, path string) (*Cache, error) {
	t.Helper()
	return Restore(Options{SourceLoader: src.loader}, NewFileJournal(path))
}

func wantItem(t *testing.T, v *View, key, value string, version uint64) {
	t.Helper()
	it, ok := v.Get(key)
	if !ok {
		t.Fatalf("view missing key %s", key)
	}
	if it.State != ItemReady {
		t.Fatalf("key %s not ready: state=%v err=%v", key, it.State, it.Err)
	}
	if it.Value != value || it.SourceVersion != version {
		t.Fatalf("key %s = (%v, v%d), want (%v, v%d)",
			key, it.Value, it.SourceVersion, value, version)
	}
}

func journalRecords(t *testing.T, path string) []JournalRecord {
	t.Helper()
	recs, err := NewFileJournal(path).ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return recs
}

// Stage 1: a confirmed diamond view survives a restart with the source
// completely offline; the restored process can explain exactly which
// combination it last published, per key with source version and deps.
func TestRestoreConfirmedViewAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.log")
	src := newFakeSource(1)

	c1 := newJournalCache(src, path)
	v1 := c1.Snapshot(context.Background(), diamondKeys...)
	if !v1.Complete() {
		t.Fatalf("initial snapshot incomplete: %+v", v1.Items())
	}

	// Restart: the source is now unreachable.
	dead := newFakeSource(1)
	dead.fail("L", true)
	dead.fail("A", true)
	dead.fail("B", true)
	dead.fail("R", true)
	c2, err := restoreCache(t, dead, path)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	cv, basis, ok := c2.Confirmed()
	if !ok || basis != "snapshot" {
		t.Fatalf("Confirmed() = (%q, %v), want basis snapshot", basis, ok)
	}
	for _, k := range diamondKeys {
		wantItem(t, cv, k, k+"-v1", 1)
	}
	// The restored store serves the confirmed combination without the
	// source, keeping dependency edges.
	v2 := c2.Snapshot(context.Background(), diamondKeys...)
	if !v2.Complete() {
		t.Fatalf("restored snapshot incomplete: %+v", v2.Items())
	}
	for _, k := range diamondKeys {
		wantItem(t, v2, k, k+"-v1", 1)
	}
	if got := c2.SourceVersion("R"); got != 1 {
		t.Fatalf("restored SourceVersion(R) = %d, want 1", got)
	}
}

// Stages 2-4: leaf failure keeps the old view stable (and restarts
// still restore it, never the failed candidate's new values); when the
// source recovers, the whole diamond is adopted atomically; a final
// restart explains the adopted combination.
func TestLeafFailureRecoveryAndRestarts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.log")
	src := newFakeSource(1)
	ctx := context.Background()

	c1 := newJournalCache(src, path)
	base := c1.Snapshot(ctx, diamondKeys...)

	// Stage 2: the source moves to v2 but the leaf is down.
	src.setVersion(2)
	src.fail("L", true)
	c1.Invalidate("L") // propagates L -> A,B -> R
	cand := base.Refresh(ctx)
	if cand.State() != CandidateFailed {
		t.Fatalf("candidate state = %v, want CandidateFailed", cand.State())
	}
	if it, _ := cand.Get("L"); it.State != ItemFailed {
		t.Fatalf("candidate L state = %v, want ItemFailed", it.State)
	}
	// The old view is untouched and still explains itself.
	for _, k := range diamondKeys {
		wantItem(t, base, k, k+"-v1", 1)
	}

	// Restart in the middle of the failure window: the last trusted
	// combination is v1, and the failed candidate's new v2 values for
	// A, B and R must not become current.
	c2, err := restoreCache(t, src, path)
	if err != nil {
		t.Fatalf("Restore after failed candidate: %v", err)
	}
	for _, k := range diamondKeys {
		got, gerr := c2.Get(ctx, k)
		if gerr != nil || got != k+"-v1" {
			t.Fatalf("restored Get(%s) = (%v, %v), want %s-v1", k, got, gerr, k)
		}
	}
	// The failed candidate is on record for audit, with its error.
	var foundFailed bool
	for _, rec := range journalRecords(t, path) {
		if rec.Kind == RecordCandidate {
			foundFailed = true
			for _, it := range rec.Items {
				if it.Key == "L" && it.State != "failed" {
					t.Fatalf("recorded candidate L state = %q, want failed", it.State)
				}
				if it.Key == "A" && it.Value != "A-v2" {
					t.Fatalf("candidate audit lost A's proposed value: %v", it.Value)
				}
			}
		}
	}
	if !foundFailed {
		t.Fatal("failed candidate was not persisted")
	}

	// Stage 3: the leaf recovers; the whole diamond is adopted at v2.
	src.fail("L", false)
	c2.Invalidate("L")
	restoredBase, _, _ := c2.Confirmed()
	cand2 := restoredBase.Refresh(ctx)
	if cand2.State() != CandidateReady {
		t.Fatalf("recovered candidate state = %v (%s)", cand2.State(), cand2.Reason())
	}
	adopted, ok := cand2.Adopt()
	if !ok {
		t.Fatal("adopt of recovered candidate failed")
	}
	for _, k := range diamondKeys {
		wantItem(t, adopted, k, k+"-v2", 2)
	}

	// Stage 4: restart again; the adopted v2 combination is what the
	// process last published, with basis "adopt".
	c3, err := restoreCache(t, src, path)
	if err != nil {
		t.Fatalf("Restore after adopt: %v", err)
	}
	cv, basis, ok := c3.Confirmed()
	if !ok || basis != "adopt" {
		t.Fatalf("Confirmed() basis = %q, want adopt", basis)
	}
	for _, k := range diamondKeys {
		wantItem(t, cv, k, k+"-v2", 2)
	}
}

// Interleaved candidates: the newer one adopts, the older one is
// rejected as superseded, and both decisions survive a restart.
func TestInterleavedCandidatesPersisted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.log")
	src := newFakeSource(1)
	ctx := context.Background()

	c1 := newJournalCache(src, path)
	base := c1.Snapshot(ctx, diamondKeys...)

	src.setVersion(2)
	cand1 := base.Refresh(ctx)
	src.setVersion(3)
	cand2 := base.Refresh(ctx)
	if cand1.State() != CandidateReady || cand2.State() != CandidateReady {
		t.Fatalf("candidates not ready: %v / %v", cand1.State(), cand2.State())
	}
	if _, ok := cand2.Adopt(); !ok {
		t.Fatal("adopting newer candidate failed")
	}
	if _, ok := cand1.Adopt(); ok {
		t.Fatal("stale candidate adopted after newer one")
	}
	if cand1.State() != CandidateAborted ||
		!strings.Contains(cand1.Reason(), "superseded") {
		t.Fatalf("stale candidate = %v (%s)", cand1.State(), cand1.Reason())
	}

	c2, err := restoreCache(t, src, path)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	cv, basis, _ := c2.Confirmed()
	if basis != "adopt" {
		t.Fatalf("basis = %q, want adopt", basis)
	}
	for _, k := range diamondKeys {
		wantItem(t, cv, k, k+"-v3", 3)
	}
	var sawSuperseded bool
	for _, rec := range journalRecords(t, path) {
		if rec.Kind == RecordCandidate && strings.Contains(rec.Basis, "superseded") {
			sawSuperseded = true
		}
	}
	if !sawSuperseded {
		t.Fatal("superseded candidate decision not persisted")
	}
}

// A waiter that leaves mid-refresh produces an aborted candidate; the
// decision and reason are persisted and a restart keeps the old
// confirmed combination.
func TestWaiterExitPersisted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.log")
	src := newFakeSource(1)
	ctx := context.Background()

	c1 := newJournalCache(src, path)
	base := c1.Snapshot(ctx, diamondKeys...)

	src.block["L"] = make(chan struct{}) // leaf never answers in time
	defer close(src.block["L"])
	wctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	cand := base.Refresh(wctx)
	if cand.State() != CandidateAborted ||
		!strings.Contains(cand.Reason(), "waiter-left") {
		t.Fatalf("candidate = %v (%s), want aborted waiter-left",
			cand.State(), cand.Reason())
	}

	c2, err := restoreCache(t, src, path)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	cv, _, _ := c2.Confirmed()
	for _, k := range diamondKeys {
		wantItem(t, cv, k, k+"-v1", 1)
	}
	var sawWaiterLeft bool
	for _, rec := range journalRecords(t, path) {
		if rec.Kind == RecordCandidate && strings.HasPrefix(rec.Basis, "waiter-left") {
			sawWaiterLeft = true
		}
	}
	if !sawWaiterLeft {
		t.Fatal("waiter-left decision not persisted")
	}
}

// Corrupt or incomplete journals are reported loudly while the cache
// keeps serving the last trusted combination.
func TestCorruptJournalKeepsLastTrusted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "journal.log")
	src := newFakeSource(1)
	ctx := context.Background()

	c1 := newJournalCache(src, path)
	c1.Snapshot(ctx, diamondKeys...) // confirmed v1

	// Corrupt the tail: append a line that fails its checksum.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("deadbeef {\"Seq\":99}"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	c2, err := restoreCache(t, src, path)
	if err == nil {
		t.Fatal("Restore reported no error for a corrupt journal")
	}
	if !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("error does not name the corruption: %v", err)
	}
	cv, _, ok := c2.Confirmed()
	if !ok {
		t.Fatal("last trusted combination lost after corruption")
	}
	for _, k := range diamondKeys {
		wantItem(t, cv, k, k+"-v1", 1)
	}

	// A confirmed record whose members are not all ready is incomplete
	// and must never become the trusted combination.
	path2 := filepath.Join(dir, "incomplete.log")
	j := NewFileJournal(path2)
	bad := JournalRecord{
		Seq: 1, Kind: RecordConfirmed, Time: time.Now(), Basis: "snapshot",
		Items: []ItemRecord{
			{Key: "R", State: "ready", Value: "R-v1", SourceVersion: 1},
			{Key: "L", State: "failed", Err: "source down"},
		},
	}
	if err := j.Append(bad); err != nil {
		t.Fatal(err)
	}
	c3, err := Restore(Options{SourceLoader: src.loader}, j)
	if err == nil {
		t.Fatal("Restore accepted an incomplete confirmed record")
	}
	if _, _, ok := c3.Confirmed(); ok {
		t.Fatal("incomplete confirmed record became the trusted combination")
	}
	if c3.Len() != 0 {
		t.Fatalf("incomplete record populated the store: %d entries", c3.Len())
	}
	if !errors.Is(err, nil) && !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// A restored old view keeps its recorded facts: later source versions
// loaded after the restart never rewrite what the old view observed.
func TestRestoredViewIgnoresLaterSourceFacts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.log")
	src := newFakeSource(1)
	ctx := context.Background()

	c1 := newJournalCache(src, path)
	c1.Snapshot(ctx, diamondKeys...)

	// Restart into a world where the source has moved on to v5.
	src.setVersion(5)
	c2, err := restoreCache(t, src, path)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	old, _, _ := c2.Confirmed()

	// A fresh read confirms the new combination.
	c2.Invalidate("L")
	fresh := c2.Snapshot(ctx, diamondKeys...)
	if !fresh.Complete() {
		t.Fatalf("fresh snapshot incomplete: %+v", fresh.Items())
	}
	wantItem(t, fresh, "L", "L-v5", 5)

	// The restored old view still reports v1, not the reloaded facts.
	for _, k := range diamondKeys {
		wantItem(t, old, k, k+"-v1", 1)
	}
}
