package store

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"syscall"

	"github.com/containerman17/avalanche-s3-archival-node/dist"
)

// RESIDENT SIDECARS.
//
// The blooms and SST index blocks a run pins at open are the node's largest
// live-heap term (measured on mainnet C: 13.2GB across 217 runs, 11.4GB of it
// bloom bytes), and as anonymous memory they are unevictable: the 2022 bloom
// costs RAM whether or not anyone has asked a historical question all week.
//
// So the pinned bytes live in a FILE, one per run, mmapped read-only. The
// kernel's page LRU then does the policy nobody has to write: a chain serving
// historical traffic keeps its blooms paged in, an idle archive's blooms decay
// to disk within minutes, and the first historical query pages back exactly
// the 4KB pages it probes. Nothing above this file changes: the overlay slices
// point into the mapping instead of the heap, and a probe is the same memory
// read either way.
//
// The sidecar is DERIVED state, disposable by construction: it is rebuilt from
// the artifact when missing, torn, or stale (runs are content-named, so a
// sidecar can only be stale if it was written by a different binary's layout
// walk, which the handle check at open catches). It is written tmp+rename and
// never fsynced; a crash costs one rebuild. Layout:
//
//	magic "EPDBRES1"
//	u32   entry count
//	per entry: u8 section | u64 section-relative offset | u64 length
//	payloads, concatenated in entry order
type resMap struct {
	data []byte
	refs atomic.Int64
}

func (m *resMap) hold() { m.refs.Add(1) }

func (m *resMap) drop() {
	if m != nil && m.refs.Add(-1) == 0 {
		syscall.Munmap(m.data)
	}
}

const resMagic = "EPDBRES1"

type resEntry struct {
	sec Section
	off uint64
	n   uint64
}

// residentPath is the sidecar for one run, beside the spool rather than in the
// chunk cache: casfs owns its cache root and deletes foreign files in it.
func residentPath(cas *dist.Store, name RunName) string {
	return filepath.Join(cas.DataDir(), "resident", string(name))
}

// writeSidecar persists every section's overlay. tmp+rename, no fsync: the
// content is derived and a torn file fails the size check at load.
func writeSidecar(path string, secs [numSections]*sectionReadable) error {
	var entries []resEntry
	var payload int
	for s := Section(0); s < numSections; s++ {
		if secs[s] == nil {
			continue
		}
		for _, r := range secs[s].res {
			entries = append(entries, resEntry{sec: s, off: r.off, n: uint64(len(r.data))})
			payload += len(r.data)
		}
	}
	buf := make([]byte, 0, len(resMagic)+4+len(entries)*17+payload)
	buf = append(buf, resMagic...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(entries)))
	for _, e := range entries {
		buf = append(buf, byte(e.sec))
		buf = binary.BigEndian.AppendUint64(buf, e.off)
		buf = binary.BigEndian.AppendUint64(buf, e.n)
	}
	// The payload pass repeats the entry pass's iteration order exactly, so
	// entry i's bytes are payload i without any lookup.
	for s := Section(0); s < numSections; s++ {
		if secs[s] == nil {
			continue
		}
		for _, r := range secs[s].res {
			buf = append(buf, r.data...)
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// loadSidecar mmaps a sidecar and hands back per-section overlay ranges whose
// bytes alias the mapping. The map's refcount starts at 1 for the caller, who
// drops it once every section that keeps a reference has taken its own hold.
func loadSidecar(path string) (*resMap, [numSections][]residentRange, error) {
	var none [numSections][]residentRange
	f, err := os.Open(path)
	if err != nil {
		return nil, none, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, none, err
	}
	head := len(resMagic) + 4
	if st.Size() < int64(head) {
		return nil, none, fmt.Errorf("store: sidecar %s: %d bytes is too short", path, st.Size())
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(st.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, none, err
	}
	m := &resMap{data: data}
	m.refs.Store(1)
	if string(data[:len(resMagic)]) != resMagic {
		m.drop()
		return nil, none, fmt.Errorf("store: sidecar %s: bad magic", path)
	}
	count := int(binary.BigEndian.Uint32(data[len(resMagic):]))
	if int64(head+count*17) > st.Size() {
		m.drop()
		return nil, none, fmt.Errorf("store: sidecar %s: %d entries do not fit", path, count)
	}
	var res [numSections][]residentRange
	pay := uint64(head + count*17)
	for i := 0; i < count; i++ {
		e := data[head+i*17:]
		s, off, n := Section(e[0]), binary.BigEndian.Uint64(e[1:]), binary.BigEndian.Uint64(e[9:])
		if s >= numSections || pay+n > uint64(st.Size()) {
			m.drop()
			return nil, none, fmt.Errorf("store: sidecar %s: entry %d is torn", path, i)
		}
		res[s] = append(res[s], residentRange{off: off, data: data[pay : pay+n : pay+n]})
		pay += n
	}
	if pay != uint64(st.Size()) {
		m.drop()
		return nil, none, fmt.Errorf("store: sidecar %s: %d trailing bytes", path, uint64(st.Size())-pay)
	}
	for s := range res {
		sort.Slice(res[s], func(i, j int) bool { return res[s][i].off < res[s][j].off })
	}
	return m, res, nil
}

// sweepSidecars deletes sidecars for runs the manifest no longer names: a
// merge replaced them, and derived state for a dead run is pure disk. Called
// once at store open; errors are ignored because the worst case is a stale
// file the next sweep gets.
func sweepSidecars(cas *dist.Store, man *Manifest) {
	dir := filepath.Join(cas.DataDir(), "resident")
	ents, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	live := make(map[string]bool, len(man.Runs))
	for _, ref := range man.Runs {
		live[ref.Name] = true
	}
	for _, e := range ents {
		if !live[e.Name()] {
			os.Remove(filepath.Join(dir, e.Name()))
		}
	}
}
