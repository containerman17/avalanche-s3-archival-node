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

// Migrate rewrites a storage v1 data dir in place as v2. The node must be
// stopped (the caller holds the dir lock). It is RESUMABLE: every run's new
// name is appended to <dir>/migrate.json as it lands, the window logs are
// rewritten to a sibling file and renamed at the end, and a rerun skips what
// is done. NOTHING IS DELETED: v1 runs stay in the bucket and in <dir>/runs,
// only the manifest and the window logs stop pointing at them.
//
// Per run: the chain and state sections are byte-identical between v1 and v2
// and are streamed through untouched; the lookup section is rebuilt from the
// run itself (addr/ rows carry their roles, txh/ and blkh/ rows copy, and the
// elog/, tval/ and sig/ entries come out of the rcpt/ rows). The Prev chain is
// re-linked as it goes, so the runs are done in manifest order.
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
	if man.StorageVersion != 1 {
		return fmt.Errorf("store: manifest is storage version %d, migrate wants 1", man.StorageVersion)
	}
	rootBytes, err := hex.DecodeString(man.ChainRoot)
	if err != nil || len(rootBytes) != 32 {
		return fmt.Errorf("store: manifest chain root %q", man.ChainRoot)
	}
	var root [32]byte
	copy(root[:], rootBytes)

	// Window logs first: they are small, and a v2 log beside a v1 manifest is
	// harmless (the rewrite recognises a v2 log and leaves it).
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
	done := map[string]string{} // v1 name -> v2 name
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
		newName, err := migrateRun(cas, ref, prev)
		if err != nil {
			return fmt.Errorf("store: migrate run %s: %w", ref.Name, err)
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

// migrateRun writes the v2 twin of one v1 run and returns its name.
func migrateRun(cas *dist.Store, ref RunRef, prev [32]byte) (RunName, error) {
	r, err := OpenRunVersion(cas, ref.Name, 1)
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

	// The v1 lookup section, walked whole (no bloom: they were built under the
	// v1 split). addr/ rows are entries, txh/ and blkh/ rows copy, the rest is
	// rebuilt from the receipts below.
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
		case bytes.HasPrefix(k, []byte(PrefixAddr)):
			post.add(k[:len(k)-8], TxNumOf(k), val[0])
		case bytes.HasPrefix(k, []byte(PrefixTxHash)), bytes.HasPrefix(k, []byte(PrefixBlkHash)):
			rows = append(rows, kv{append([]byte(nil), k...), append([]byte(nil), val...)})
		}
	}
	it.Close()
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
			logPostings(groups, l.Address[:], topics)
		}
		for g, pay := range groups {
			post.add([]byte(g), txnum, pay)
		}
		return true
	}); err != nil {
		return "", err
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

// migrateWindowLog rewrites one window log's posting records to v2: v1 addr/
// rows become group records, logaddr/ and topic/ rows are dropped, and each
// rcpt/ record is followed by the log postings decoded out of it. Every other
// record is copied byte for byte. A log that already holds v2 records is
// left alone, so the step is idempotent.
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
	tmp := path + ".v2"
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
		case recPost:
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
			for _, l := range logs {
				topics := make([][]byte, len(l.Topics))
				for i := range l.Topics {
					topics[i] = l.Topics[i][:]
				}
				logPostings(groups, l.Address[:], topics)
			}
			for _, g := range sortedKeys(groups) {
				if err := write(recPost, num, []byte{groups[g]}, []byte(g)); err != nil {
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

// windowLogVersion tells a v1 log from a v2 one by its first addr/ posting
// record: a v1 key carries the TxNum suffix, a v2 group does not. A log with
// no posting record has no transaction and nothing to rewrite.
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
			return StorageVersion, nil
		}
		n := binary.BigEndian.Uint32(hdr[9:])
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return StorageVersion, nil
		}
		if hdr[0] != recPost || !bytes.HasPrefix(payload[1:], []byte(PrefixAddr)) {
			continue
		}
		if len(payload[1:]) == len(PrefixAddr)+20+1 {
			return StorageVersion, nil
		}
		return 1, nil
	}
}
