package pruned

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
	"golang.org/x/sys/unix"

	"github.com/containerman17/avalanche-s3-archival-node/commit"
	"github.com/containerman17/avalanche-s3-archival-node/latest"
)

type checkpointManifest struct {
	Gen      uint64      `json:"generation"`
	Sequence uint64      `json:"sequence"`
	Height   uint64      `json:"height"`
	Root     common.Hash `json:"root"`
	Block    common.Hash `json:"block"`
}
type journalOperation struct{ Key, Value []byte }
type journalRecord struct {
	Sequence, Height    uint64
	Parent, Block, Root common.Hash
	Operations          []journalOperation
}

func userData(sequence uint64, root common.Hash) [32]byte {
	var data [32]byte
	binary.LittleEndian.PutUint64(data[:8], sequence)
	copy(data[8:], root[:24])
	return data
}

func (d *DB) restore() error {
	var err error
	d.lock, err = os.OpenFile(filepath.Join(d.cfg.Dir, "LOCK"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	if err = unix.Flock(int(d.lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return fmt.Errorf("epochdb: state directory is in use: %w", err)
	}
	data, err := os.ReadFile(filepath.Join(d.cfg.Dir, "MANIFEST"))
	if errors.Is(err, os.ErrNotExist) {
		d.current = &revision{root: types.EmptyRootHash, accepted: true}
		d.history = []*revision{d.current}
		d.blocks[common.Hash{}] = d.current
		d.roots[d.current.root] = []*revision{d.current}
		if err = d.checkpoint(); err != nil {
			return err
		}
	} else {
		if err != nil {
			return err
		}
		var m checkpointManifest
		if err = json.Unmarshal(data, &m); err != nil {
			return fmt.Errorf("epochdb: manifest: %w", err)
		}
		d.gen = m.Gen
		d.run, err = latest.Open(d.runPath(m.Gen))
		if err != nil {
			return err
		}
		d.file, err = commit.Open(d.triePath(m.Gen))
		if err != nil {
			return err
		}
		want := userData(m.Sequence, m.Root)
		if d.run.UserData() != want || d.file.UserData() != want || d.file.Root() != m.Root {
			return errors.New("epochdb: checkpoint files do not match manifest")
		}
		d.dirty = commit.NewDirty(d.file, d.seek)
		d.current = &revision{root: m.Root, block: m.Block, height: m.Height, sequence: m.Sequence, accepted: true}
		d.history = []*revision{d.current}
		d.blocks[m.Block] = d.current
		d.roots[m.Root] = []*revision{d.current}
		d.wal, err = os.OpenFile(filepath.Join(d.cfg.Dir, "JOURNAL"), os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return err
		}
		if err = d.replayJournal(m.Sequence); err != nil {
			return err
		}
	}
	// Only the manifest's pair is reachable after a restart.
	entries, err := os.ReadDir(d.cfg.Dir)
	if err != nil {
		return err
	}
	keep := map[string]bool{"LOCK": true, "MANIFEST": true, "JOURNAL": true, filepath.Base(d.runPath(d.gen)): true, filepath.Base(d.triePath(d.gen)): true}
	for _, entry := range entries {
		if keep[entry.Name()] {
			continue
		}
		name := entry.Name()
		if bytes.HasPrefix([]byte(name), []byte("run.")) || bytes.HasPrefix([]byte(name), []byte("trie.")) || name == "MANIFEST.tmp" || name == "JOURNAL.tmp" {
			if err = os.Remove(filepath.Join(d.cfg.Dir, name)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *DB) seek(prefix []byte) (key, value []byte) {
	it := latest.NewView(nil, d.run).Iter(prefix, nil)
	if !it.Next() {
		return nil, nil
	}
	return it.Key(), it.Value()
}

func (d *DB) appendJournal(r *revision) error {
	record := journalRecord{Sequence: r.sequence, Height: r.height, Parent: r.parent.root, Block: r.block, Root: r.root}
	for _, op := range r.ops {
		record.Operations = append(record.Operations, journalOperation{op.key, op.value})
	}
	data, err := rlp.EncodeToBytes(record)
	if err != nil {
		return err
	}
	if len(data) > 256<<20 {
		return errors.New("epochdb: recovery record exceeds 256 MiB")
	}
	var header [8]byte
	binary.LittleEndian.PutUint32(header[:4], uint32(len(data)))
	binary.LittleEndian.PutUint32(header[4:], crc32.ChecksumIEEE(data))
	frame := append(header[:], data...)
	n, err := d.wal.Write(frame)
	if err != nil {
		return err
	}
	if n != len(frame) {
		return io.ErrShortWrite
	}
	d.walBytes += int64(n)
	if r.sequence%d.cfg.CommitInterval == 0 {
		return d.wal.Sync()
	}
	return nil
}

func (d *DB) replayJournal(checkpointSequence uint64) error {
	var offset int64
	for {
		var header [8]byte
		_, err := io.ReadFull(d.wal, header[:])
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return err
		}
		size := binary.LittleEndian.Uint32(header[:4])
		if size == 0 || size > 256<<20 {
			return fmt.Errorf("epochdb: invalid journal frame length at %d", offset)
		}
		data := make([]byte, size)
		if _, err = io.ReadFull(d.wal, data); errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		} else if err != nil {
			return err
		}
		if crc32.ChecksumIEEE(data) != binary.LittleEndian.Uint32(header[4:]) {
			return fmt.Errorf("epochdb: journal checksum mismatch at %d", offset)
		}
		var record journalRecord
		if err = rlp.DecodeBytes(data, &record); err != nil {
			return fmt.Errorf("epochdb: journal record at %d: %w", offset, err)
		}
		if record.Sequence > checkpointSequence {
			if record.Sequence != d.current.sequence+1 || record.Parent != d.current.root {
				return fmt.Errorf("epochdb: nonconsecutive journal record at %d", offset)
			}
			var ops []operation
			for _, op := range record.Operations {
				ops = append(ops, operation{op.Key, op.Value})
			}
			r, err := d.compute(d.current, ops)
			if err != nil {
				return fmt.Errorf("epochdb: recovery at %d: %w", record.Height, err)
			}
			if r.root != record.Root {
				return fmt.Errorf("epochdb: recovery root mismatch at %d: got %s, want %s", record.Height, r.root, record.Root)
			}
			r.sequence = record.Sequence
			r.height = record.Height
			r.block = record.Block
			d.blocks[r.block] = r
			d.roots[r.root] = append(d.roots[r.root], r)
			if err = d.accept(r); err != nil {
				return err
			}
		}
		offset += int64(8 + size)
	}
	// A process can die between either frame write. Only an incomplete final
	// frame is discarded; a complete frame with a bad checksum is corruption.
	if err := d.wal.Truncate(offset); err != nil {
		return err
	}
	if _, err := d.wal.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	d.walBytes = offset
	return nil
}

func syncDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func replaceSynced(dir, name string, data []byte) error {
	f, err := os.OpenFile(filepath.Join(dir, name+".tmp"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(filepath.Join(dir, name+".tmp"), filepath.Join(dir, name)); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func (d *DB) checkpoint() error {
	gen := d.gen + 1
	ud := userData(d.current.sequence, d.current.root)
	var runs []*latest.Run
	if d.run != nil {
		runs = []*latest.Run{d.run}
	}
	run, err := latest.Merge(d.runPath(gen), latest.NewView(d.overlay, runs...), ud)
	if err != nil {
		return err
	}
	root, _, err := commit.Roll(run.Iter(nil, nil), d.triePath(gen), ud)
	if err != nil {
		run.Close()
		return err
	}
	if root != d.current.root {
		run.Close()
		return fmt.Errorf("epochdb: checkpoint root mismatch: got %s, want %s", root, d.current.root)
	}
	file, err := commit.Open(d.triePath(gen))
	if err != nil {
		run.Close()
		return err
	}
	if d.wal != nil {
		if err = d.wal.Sync(); err != nil {
			run.Close()
			file.Close()
			return err
		}
	}
	m := checkpointManifest{Gen: gen, Sequence: d.current.sequence, Height: d.current.height, Root: root, Block: d.current.block}
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if err = replaceSynced(d.cfg.Dir, "MANIFEST", data); err != nil {
		run.Close()
		file.Close()
		return err
	}
	oldRun, oldFile, oldGen := d.run, d.file, d.gen
	d.run = run
	d.file = file
	d.gen = gen
	d.overlay = latest.NewOverlay()
	clear(d.owners)
	d.dirty = commit.NewDirty(file, d.seek)
	if oldRun != nil {
		oldRun.Close()
	}
	if oldFile != nil {
		oldFile.Close()
	}
	if oldGen != 0 {
		if err = os.Remove(d.runPath(oldGen)); err != nil {
			return err
		}
		if err = os.Remove(d.triePath(oldGen)); err != nil {
			return err
		}
	}
	// Publication precedes rotation. Recovery skips old journal records when
	// a crash leaves the old journal beside the new checkpoint.
	if err = replaceSynced(d.cfg.Dir, "JOURNAL", nil); err != nil {
		return err
	}
	if d.wal != nil {
		d.wal.Close()
	}
	d.wal, err = os.OpenFile(filepath.Join(d.cfg.Dir, "JOURNAL"), os.O_RDWR, 0600)
	d.walBytes = 0
	return err
}
