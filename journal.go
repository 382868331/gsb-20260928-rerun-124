package cache

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
	"time"
)

// RecordKind identifies what a journal record describes.
type RecordKind string

const (
	// RecordConfirmed is a fully confirmed combined view: the last
	// combination that was published to the public store as one
	// atomic set. Only confirmed records are ever restored into the
	// store.
	RecordConfirmed RecordKind = "confirmed"
	// RecordCandidate is a candidate decision (failed, aborted or
	// abandoned). It is kept for audit: its values are inspectable
	// after a restart but are never published as current values.
	RecordCandidate RecordKind = "candidate"
)

// ItemRecord is one key's persisted slice of a record. Values must be
// JSON-marshalable for the FileJournal to round-trip them.
type ItemRecord struct {
	Key           string
	State         string // "ready", "pending" or "failed"
	Value         any
	SourceVersion uint64
	// Gen is the local invalidation generation the value was captured
	// under, so a restore can keep rejecting stale in-flight results.
	Gen  uint64
	Deps []string
	Err  string
}

// JournalRecord is one appended decision. Confirmed records carry the
// whole published combination plus the basis that confirmed it;
// candidate records carry the would-be members and the reason the
// candidate was not (or not yet) adopted.
type JournalRecord struct {
	Seq  uint64
	Kind RecordKind
	Time time.Time
	// Basis explains the decision: "snapshot", "adopt", or the
	// candidate's failure/abort reason.
	Basis string
	Items []ItemRecord
}

// Journal persists confirmed views and candidate decisions so a
// restart can explain (and restore) the last published combination.
type Journal interface {
	// Append durably adds one record. Seq is assigned by the cache in
	// strictly increasing order.
	Append(JournalRecord) error
	// ReadAll returns the valid records in order. If the journal is
	// corrupt or truncated, it returns the valid prefix together with
	// an error describing the first bad record.
	ReadAll() ([]JournalRecord, error)
}

// FileJournal is a Journal backed by an append-only file. Every record
// is one line: an 8-digit CRC32 of the JSON payload, a space, then the
// JSON. A line whose checksum or payload does not parse is corruption.
type FileJournal struct {
	path string
	mu   sync.Mutex
}

// NewFileJournal opens (creating if needed) a journal file at path.
func NewFileJournal(path string) *FileJournal { return &FileJournal{path: path} }

// Append writes one checksummed line and fsyncs it.
func (j *FileJournal) Append(rec JournalRecord) error {
	payload, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("journal: marshal record %d: %w", rec.Seq, err)
	}
	line := fmt.Sprintf("%08x %s\n", crc32.ChecksumIEEE(payload), payload)
	j.mu.Lock()
	defer j.mu.Unlock()
	f, err := os.OpenFile(j.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("journal: open: %w", err)
	}
	defer f.Close()
	if _, err := f.WriteString(line); err != nil {
		return fmt.Errorf("journal: write record %d: %w", rec.Seq, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("journal: sync record %d: %w", rec.Seq, err)
	}
	return nil
}

// ReadAll returns every valid record in order. The first corrupt or
// incomplete line stops the scan: the valid prefix is returned together
// with an error naming the offending line, so the caller can keep the
// last trusted combination and still report the damage.
func (j *FileJournal) ReadAll() ([]JournalRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	f, err := os.Open(j.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("journal: open: %w", err)
	}
	defer f.Close()
	var recs []JournalRecord
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 16<<20)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		if len(line) < 10 || line[8] != ' ' {
			return recs, fmt.Errorf("journal: line %d: malformed record framing", lineNo)
		}
		var want uint32
		if _, err := fmt.Sscanf(string(line[:8]), "%08x", &want); err != nil {
			return recs, fmt.Errorf("journal: line %d: bad checksum: %v", lineNo, err)
		}
		payload := line[9:]
		if crc32.ChecksumIEEE(payload) != want {
			return recs, fmt.Errorf("journal: line %d: checksum mismatch (corrupt record)", lineNo)
		}
		var rec JournalRecord
		if err := json.Unmarshal(payload, &rec); err != nil {
			return recs, fmt.Errorf("journal: line %d: undecodable record: %v", lineNo, err)
		}
		recs = append(recs, rec)
	}
	if err := sc.Err(); err != nil {
		return recs, fmt.Errorf("journal: read: %w", err)
	}
	return recs, nil
}

// validateConfirmed checks that a confirmed record is internally
// complete: every member ready. An incomplete confirmed record can
// never be restored as the trusted combination.
func validateConfirmed(rec JournalRecord) error {
	if len(rec.Items) == 0 {
		return fmt.Errorf("record %d: confirmed view has no members", rec.Seq)
	}
	for _, it := range rec.Items {
		if it.State != "ready" {
			return fmt.Errorf("record %d: member %q is %s, not ready", rec.Seq, it.Key, it.State)
		}
	}
	return nil
}
