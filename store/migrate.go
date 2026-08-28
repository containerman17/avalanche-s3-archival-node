package store

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/containerman17/avalanche-s3-archival-node/dist"
)

// Migrate rewrites a storage v1 or v2 data dir in place as the current
// version. The node must be stopped (the caller holds the dir lock). It is
// RESUMABLE: every run's new name is appended to <dir>/migrate.json as it
// lands, the window logs are rewritten to a sibling file and renamed at the
// end, and a rerun skips what is done. NOTHING IS DELETED: the old runs stay
// in the bucket and in <dir>/runs, only the manifest and the window logs stop
// pointing at them.
//
// Per run: the chain and state sections are byte-identical across the
// versions and are streamed through untouched; the lookup section is rebuilt
// from the run itself. From v1: addr/ rows carry their roles, txh/ and blkh/
// rows copy, and the elog/, tval/ and sig/ entries come out of the rcpt/
// rows. From v2: every lookup row copies. Both then add the set/ rows out of
// the rcpt/ rows. The Prev chain is re-linked as it goes, so the runs are
// done in manifest order.
func Migrate(dir string, cas *dist.Store, logf func(string, ...any)) error {
	raw, err := os.ReadFile(filepath.Join(dir, manifestFile))
	if err != nil {
		return err
	}
	man := new(Manifest)
	if err := json.Unmarshal(raw, man); err != nil {
		return fmt.Errorf("store: manifest: %w", err)
	}
	if man.StorageVersion == StorageVersion {
		logf("manifest is already storage version %d", StorageVersion)
		return nil
	}
	if man.StorageVersion != 1 && man.StorageVersion != 2 {
		return fmt.Errorf("store: manifest is storage version %d, migrate wants 1 or 2", man.StorageVersion)
	}
	from := uint32(man.StorageVersion)
	rootBytes, err := hex.DecodeString(man.ChainRoot)
	if err != nil || len(rootBytes) != 32 {
		return fmt.Errorf("store: manifest chain root %q", man.ChainRoot)
	}
	var root [32]byte
	copy(root[:], rootBytes)

	// Window logs first: they are small, and a migrated log beside an old
	// manifest is harmless (the rewrite recognises it and leaves it).
	for _, name := range []string{frozenLog, "window.log"} {
		path := filepath.Join(dir, "window", name)
		if !fileExists(path) {
			continue
		}
		if err := migrateWindowLog(path, logf); err != nil {
			return fmt.Errorf("store: migrate %s: %w", name, err)
		}
	}

	donePath := filepath.Join(dir, "migrate.json")
	done := map[string]string{} // old name -> new name
	if raw, err := os.ReadFile(donePath); err == nil {
		if err := json.Unmarshal(raw, &done); err != nil {
			return fmt.Errorf("store: %s: %w", donePath, err)
		}
	}
	prev := root
	for i, ref := range man.Runs {
		if newName, ok := done[ref.Name]; ok {
			if b, err := cas.Open(newName); err == nil {
				b.Close()
				hex.Decode(prev[:], []byte(newName))
				man.Runs[i].Name = newName
				logf("run %d/%d %s: done already", i+1, len(man.Runs), RunLabel(ref.Level, ref.FromTx, ref.ToTx))
				continue
			}
		}
		t0 := time.Now()
		var newName RunName
		// A bucket read fails transiently now and then (a 503 in the middle
		// of a 13GB run); the run is retried whole, the writer is aborted.
		for attempt := 1; ; attempt++ {
			newName, err = migrateRun(cas, ref, prev, from)
			if err == nil {
				break
			}
			if attempt == 5 {
				return fmt.Errorf("store: migrate run %s: %w", ref.Name, err)
			}
			logf("run %d/%d: attempt %d failed, retrying in 30s: %v", i+1, len(man.Runs), attempt, err)
			time.Sleep(30 * time.Second)
		}
		done[ref.Name] = newName
		if err := saveJSON(donePath, done); err != nil {
			return err
		}
		hex.Decode(prev[:], []byte(newName))
		man.Runs[i].Name = newName
		if ref.Terminal() && cas.Remote() {
			if _, err := cas.Sync(); err != nil {
				return fmt.Errorf("store: sync after %s: %w", newName, err)
			}
		}
		logf("run %d/%d %s: %s -> %s in %s", i+1, len(man.Runs), RunLabel(ref.Level, ref.FromTx, ref.ToTx), ref.Name[:12], newName[:12], time.Since(t0).Round(time.Second))
	}

	man.StorageVersion = StorageVersion
	if err := man.save(dir); err != nil {
		return err
	}
	// Publish the terminal set the way DB.Publish does: content first,
	// pointer last.
	pub := *man
	pub.Runs = publishable(man.Runs)
	if len(pub.Runs) > 0 {
		raw, err := json.Marshal(pub)
		if err != nil {
			return err
		}
		name, err := cas.Put(raw)
		if err != nil {
			return err
		}
		if err := cas.SetLatest(root, dist.Latest{Manifest: name}); err != nil {
			return err
		}
		if cas.Remote() {
			if _, err := cas.Sync(); err != nil {
				return err
			}
		}
		logf("published manifest %s (%d terminal runs)", name[:12], len(pub.Runs))
	}
	return nil
}

func saveJSON(path string, v any) error {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// migrateRun writes the current-version twin of one run of version from and
// returns its name.
func migrateRun(cas *dist.Store, ref RunRef, prev [32]byte, from uint32) (RunName, error) {
	r, err := OpenRunVersion(cas, ref.Name, from)
	if err != nil {
		return "", err
	}
	defer r.Close()
	f := r.Footer
	dir := cas.LocalDir()
	if ref.Terminal() {
		dir = cas.SpoolDir()
	}
	w, err := NewRunWriter(RunFileName(dir, ref.Level, f.FromTx, f.ToTx)+".migrate", prev, ref.Level)
	if err != nil {
		return "", err
	}
	defer w.Abort()
	for s := SecChain; s <= SecState; s++ {
		if err := w.CopySection(s, r.blob, f.Off[s], f.Len[s]); err != nil {
			return "", err
		}
	}

	// The old lookup section, walked whole (no bloom: it was built under the
	// old split). From v1, addr/ rows are entries, txh/ and blkh/ rows copy and
	// the rest is rebuilt from the receipts below; from v2 every row copies.
	var post postSet
	var rows []kv
	it, err := r.newIter(SecLookup)
	if err != nil {
		return "", err
	}
	for e := it.SeekGE(nil, 0); e != nil; e = it.Next() {
		k := e.K.UserKey
		val, _, err := e.Value(nil)
		if err != nil {
			it.Close()
			return "", err
		}
		switch {
		case from == 1 && bytes.HasPrefix(k, []byte(PrefixAddr)):
			post.add(k[:len(k)-8], TxNumOf(k), val[0])
		case from == 2, bytes.HasPrefix(k, []byte(PrefixTxHash)), bytes.HasPrefix(k, []byte(PrefixBlkHash)):
			rows = append(rows, kv{append([]byte(nil), k...), append([]byte(nil), val...)})
		}
	}
	it.Close()
	sets := map[string]struct{}{}
	lo, hi := RcptKey(0), RcptKey(^uint64(0))
	if err := r.ScanRange(SecChain, lo, hi, func(k, val []byte) bool {
		txnum := NumOf(k)
		_, _, _, logs, derr := DecodeTxReceipt(val)
		if derr != nil {
			err = fmt.Errorf("rcpt/%d: %w", txnum, derr)
			return false
		}
		groups := map[string]byte{}
		for _, l := range logs {
			topics := make([][]byte, len(l.Topics))
			for i := range l.Topics {
				topics[i] = l.Topics[i][:]
			}
			if from == 1 {
				logPostings(groups, l.Address[:], topics)
			}
			for _, sk := range logSets(l.Address[:], topics) {
				sets[string(sk)] = struct{}{}
			}
		}
		for g, pay := range groups {
			post.add([]byte(g), txnum, pay)
		}
		return true
	}); err != nil {
		return "", err
	}
	for k := range sets {
		rows = append(rows, kv{[]byte(k), nil})
	}
	if err := w.Begin(SecLookup); err != nil {
		return "", err
	}
	rows = append(rows, post.chunks()...)
	sortKV(rows)
	for _, row := range rows {
		if err := w.Set(row.key, row.val); err != nil {
			return "", err
		}
	}
	if err := w.End(); err != nil {
		return "", err
	}
	name, _, err := w.Finish(cas, f.FromTx, f.ToTx, f.FromHeight, f.ToHeight)
	return name, err
}

// migrateWindowLog rewrites one window log to the current version. From v1,
// addr/ posting rows become group records, logaddr/ and topic/ rows are
// dropped, and each rcpt/ record is followed by the log postings decoded out
// of it. From either version every rcpt/ record is followed by its set/
// records (any it already had are dropped first, so a rerun is exact). Every
// other record is copied byte for byte. A log that already holds set/
// records is left alone, so the step is idempotent.
func migrateWindowLog(path string, logf func(string, ...any)) error {
	v, err := windowLogVersion(path)
	if err != nil {
		return err
	}
	if v == StorageVersion {
		logf("%s: already v%d", filepath.Base(path), v)
		return nil
	}
	in, err := os.Open(path)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := path + ".migrate"
	out, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer out.Close()
	bw := bufio.NewWriterSize(out, 1<<20)
	write := func(kind byte, num uint64, payload ...[]byte) error {
		var h [recHeader]byte
		h[0] = kind
		binary.BigEndian.PutUint64(h[1:9], num)
		n := 0
		for _, p := range payload {
			n += len(p)
		}
		binary.BigEndian.PutUint32(h[9:], uint32(n))
		if _, err := bw.Write(h[:]); err != nil {
			return err
		}
		for _, p := range payload {
			if _, err := bw.Write(p); err != nil {
				return err
			}
		}
		return nil
	}
	r := bufio.NewReaderSize(in, 1<<20)
	var hdr [recHeader]byte
	var records int
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			break // a torn tail is cut here exactly as recover would cut it
		}
		kind, num, n := hdr[0], binary.BigEndian.Uint64(hdr[1:9]), binary.BigEndian.Uint32(hdr[9:])
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			break
		}
		records++
		switch kind {
		case recSet:
			continue // regenerated from the receipt
		case recPost:
			if v != 1 {
				if err := write(kind, num, payload); err != nil {
					return err
				}
				continue
			}
			key := payload[1:]
			if !bytes.HasPrefix(key, []byte(PrefixAddr)) {
				continue // logaddr/ or topic/: rebuilt from the receipt
			}
			if err := write(recPost, num, payload[:1], key[:len(key)-8]); err != nil {
				return err
			}
		case recRcpt:
			if err := write(kind, num, payload); err != nil {
				return err
			}
			_, _, _, logs, err := DecodeTxReceipt(payload)
			if err != nil {
				return fmt.Errorf("rcpt record at tx %d: %w", num, err)
			}
			groups := map[string]byte{}
			var sets [][]byte
			for _, l := range logs {
				topics := make([][]byte, len(l.Topics))
				for i := range l.Topics {
					topics[i] = l.Topics[i][:]
				}
				if v == 1 {
					logPostings(groups, l.Address[:], topics)
				}
				sets = append(sets, logSets(l.Address[:], topics)...)
			}
			for _, g := range sortedKeys(groups) {
				if err := write(recPost, num, []byte{groups[g]}, []byte(g)); err != nil {
					return err
				}
			}
			for _, k := range sets {
				if err := write(recSet, num, k); err != nil {
					return err
				}
			}
		default:
			if err := write(kind, num, payload); err != nil {
				return err
			}
		}
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	if err := out.Sync(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	logf("%s: %d records rewritten", filepath.Base(path), records)
	return nil
}

// windowLogVersion tells the versions apart by their records: a v1 addr/
// posting key carries the TxNum suffix, a v2 group does not, and only a v3
// log holds set/ records. A log with neither a v1 posting nor a set/ record
// reads as v2 (it may be a v3 log with no topic anywhere, whose rewrite is
// a copy).
func windowLogVersion(path string) (uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var hdr [recHeader]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return 2, nil
		}
		n := binary.BigEndian.Uint32(hdr[9:])
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return 2, nil
		}
		switch {
		case hdr[0] == recSet:
			return StorageVersion, nil
		case hdr[0] == recPost && bytes.HasPrefix(payload[1:], []byte(PrefixAddr)) && len(payload[1:]) != len(PrefixAddr)+20+1:
			return 1, nil
		}
	}
}
