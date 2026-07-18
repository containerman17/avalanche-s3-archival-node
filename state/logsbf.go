package state

import (
	"fmt"
	"os"
	"path/filepath"
)

// LogsBackfill is the derived event-log store for blocks BELOW logs.start
// (the live exec captures the 'logs' bucketLog only from that height up).
// Separate namespace so the running exec's files are never opened for
// writing: logsbf_NNNNN.log + logsbf_idx_NNNNN.log, record format identical
// to the live capture (exec encodeLogsFrame: uvarint nAddr + 20B addrs +
// uvarint nTopic + 32B topics, unique within block, first-appearance
// order; blocks without logs get NO record).
//
// MERGE RULE for any reader (e.g. the future posting-list cook): for block
// N, read the live 'logs' bucketLog when N >= logs.start, read 'logsbf'
// when N < logs.start. Records never overlap; both sides omit no-log
// blocks, so "no record" always means "no logs in this block" once the
// block's bucket is fully derived (see BucketDone) or executed.
//
// Resume model: appends are idempotent per block, but no-log blocks leave
// no trace, so completeness is tracked by an empty logsbf_done_NNNNN
// marker per bucket. No marker = re-derive the whole bucket.
type LogsBackfill struct {
	dir string
	l   *bucketLog
}

// OpenLogsBackfill opens (or creates) the backfill namespace for writing.
func OpenLogsBackfill(dir string) (*LogsBackfill, error) {
	l, err := openBucketLog(dir, "logsbf")
	if err != nil {
		return nil, fmt.Errorf("open logsbf: %w", err)
	}
	return &LogsBackfill{dir: dir, l: l}, nil
}

func (b *LogsBackfill) doneMarker(bucket uint64) string {
	return filepath.Join(b.dir, fmt.Sprintf("logsbf_done_%05d", bucket))
}

// Append stores block's record. No-op if already stored.
func (b *LogsBackfill) Append(block uint64, rec []byte) error { return b.l.Append(block, rec) }

// Get returns the stored record, ok=false if the block has none.
func (b *LogsBackfill) Get(block uint64) ([]byte, bool, error) { return b.l.Get(block) }

// Has reports whether block already has a record.
func (b *LogsBackfill) Has(block uint64) bool { return b.l.Has(block) }

// Bytes returns total record payload bytes on disk.
func (b *LogsBackfill) Bytes() uint64 { return b.l.Bytes() }

// BucketDone reports whether bucket was fully derived by a previous run.
func (b *LogsBackfill) BucketDone(bucket uint64) bool {
	_, err := os.Stat(b.doneMarker(bucket))
	return err == nil
}

// MarkBucketDone fsyncs pending appends and drops the bucket's done marker.
func (b *LogsBackfill) MarkBucketDone(bucket uint64) error {
	if err := b.l.Sync(); err != nil {
		return err
	}
	return os.WriteFile(b.doneMarker(bucket), nil, 0o644)
}

// Sync fsyncs pending appends.
func (b *LogsBackfill) Sync() error { return b.l.Sync() }

// Close flushes and releases file handles.
func (b *LogsBackfill) Close() error { return b.l.Close() }
