package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"

	"github.com/klauspost/compress/zstd"
)

// Epoch is one open (mmap'd) sealed epoch file.
type Epoch struct {
	Start   uint64
	Count   uint64 // blocks
	TxCount uint64

	mm  []byte
	sec [epochNumSections][]byte
	dec *zstd.Decoder // registered with this epoch's dict

	txEF      *ef
	txBlk     *packed
	bloomM    uint64
	bloomK    uint32
	bloomBits []byte // word view into the section

	// frameMu guards the shared decoder buffers; decompression is cheap
	// (34us/frame) relative to lock hold times at our request rates.
	// ponytail: single decoder + mutex, per-CPU decoder pool if it shows up.
	decMu sync.Mutex
}

// End returns the last block in the epoch (inclusive).
func (e *Epoch) End() uint64 { return e.Start + e.Count - 1 }

// OpenEpoch mmaps and validates one epoch file.
func OpenEpoch(path string) (*Epoch, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := int(st.Size())
	if size < epochFooterSize {
		return nil, fmt.Errorf("epoch %s: too small", path)
	}
	mm, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	ft := mm[size-epochFooterSize:]
	if !bytes.Equal(ft[0:4], epochMagic[:]) || !bytes.Equal(ft[epochFooterSize-4:], epochMagic[:]) {
		syscall.Munmap(mm)
		return nil, fmt.Errorf("epoch %s: bad footer magic", path)
	}
	if v := binary.LittleEndian.Uint32(ft[4:8]); v != epochVersion {
		syscall.Munmap(mm)
		return nil, fmt.Errorf("epoch %s: version %d, want %d", path, v, epochVersion)
	}
	e := &Epoch{
		Start:   binary.LittleEndian.Uint64(ft[8:16]),
		Count:   binary.LittleEndian.Uint64(ft[16:24]),
		TxCount: binary.LittleEndian.Uint64(ft[24:32]),
		mm:      mm,
	}
	for i := 0; i < epochNumSections; i++ {
		off := binary.LittleEndian.Uint64(ft[32+i*16:])
		ln := binary.LittleEndian.Uint64(ft[40+i*16:])
		if off+ln > uint64(size-epochFooterSize) {
			syscall.Munmap(mm)
			return nil, fmt.Errorf("epoch %s: section %d out of bounds", path, i)
		}
		e.sec[i] = mm[off : off+ln]
	}
	e.dec, err = zstd.NewReader(nil, zstd.WithDecoderDicts(e.sec[secDict]))
	if err != nil {
		syscall.Munmap(mm)
		return nil, err
	}

	// txidx: resident views over the mmap words.
	tx := e.sec[secTxidx]
	if len(tx) < 16 {
		e.Close()
		return nil, fmt.Errorf("epoch %s: truncated txidx", path)
	}
	nTx := binary.LittleEndian.Uint64(tx[0:8])
	efL := uint(binary.LittleEndian.Uint32(tx[8:12]))
	blkBits := uint(binary.LittleEndian.Uint32(tx[12:16]))
	pos := 16
	var secs [5][]uint64
	for i := range secs {
		if secs[i], pos, err = readWords(tx, pos); err != nil {
			e.Close()
			return nil, fmt.Errorf("epoch %s: txidx: %w", path, err)
		}
	}
	e.txEF = &ef{
		n:    int(nTx),
		l:    efL,
		lows: &packed{w: secs[0], bits: efL},
		high: secs[1],
		sel0: secs[2],
		sel1: secs[3],
	}
	e.txEF.highBits = nTx + (uint64(1)<<fpBits)>>efL + 1
	e.txBlk = &packed{w: secs[4], bits: blkBits}

	bl := e.sec[secKeybloom]
	if len(bl) < 16 {
		e.Close()
		return nil, fmt.Errorf("epoch %s: truncated bloom", path)
	}
	e.bloomM = binary.LittleEndian.Uint64(bl[0:8])
	e.bloomK = binary.LittleEndian.Uint32(bl[8:12])
	e.bloomBits = bl[16:]
	return e, nil
}

func (e *Epoch) Close() {
	if e.dec != nil {
		e.dec.Close()
	}
	if e.mm != nil {
		syscall.Munmap(e.mm)
		e.mm = nil
	}
}

func (e *Epoch) decodeAll(frame []byte) ([]byte, error) {
	e.decMu.Lock()
	defer e.decMu.Unlock()
	return e.dec.DecodeAll(frame, nil)
}

// framedBlob returns entry (block - Start) from a framed section pair.
func (e *Epoch) framedBlob(data, index []byte, rel uint64) ([]byte, error) {
	frame := rel / framedGroup
	if int(frame+2)*8 > len(index) {
		return nil, fmt.Errorf("frame %d beyond index", frame)
	}
	lo := binary.LittleEndian.Uint64(index[frame*8:])
	hi := binary.LittleEndian.Uint64(index[(frame+1)*8:])
	raw, err := e.decodeAll(data[lo:hi])
	if err != nil {
		return nil, err
	}
	pos := 0
	for i := uint64(0); ; i++ {
		ln, n := binary.Uvarint(raw[pos:])
		if n <= 0 {
			return nil, fmt.Errorf("bad frame payload")
		}
		pos += n
		if i == rel%framedGroup {
			return raw[pos : pos+int(ln)], nil
		}
		pos += int(ln)
	}
}

// Container returns the raw container for block n.
func (e *Epoch) Container(n uint64) ([]byte, error) {
	if n < e.Start || n > e.End() {
		return nil, fmt.Errorf("block %d outside epoch [%d,%d]", n, e.Start, e.End())
	}
	return e.framedBlob(e.sec[secBodies], e.sec[secBodiesIdx], n-e.Start)
}

// HeaderRLP returns the RLP header for block n.
func (e *Epoch) HeaderRLP(n uint64) ([]byte, error) {
	if n < e.Start || n > e.End() {
		return nil, fmt.Errorf("block %d outside epoch [%d,%d]", n, e.Start, e.End())
	}
	return e.framedBlob(e.sec[secHeaders], e.sec[secHeadersIdx], n-e.Start)
}

// MayContainKey is the bloom prefilter for the cross-epoch descent.
func (e *Epoch) MayContainKey(key []byte) bool {
	h1, h2 := bloomHash(key)
	for i := uint64(0); i < uint64(e.bloomK); i++ {
		bit := (h1 + i*h2) % e.bloomM
		w := binary.LittleEndian.Uint64(e.bloomBits[bit/64*8:])
		if w&(1<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

// StateSearch finds the largest write <= n of key inside this epoch.
// val nil with found=true is an explicit delete/zero record.
func (e *Epoch) StateSearch(key []byte, n uint64) (val []byte, blk uint64, found bool, err error) {
	idx := e.sec[secSSTIdx]
	nEntries := len(idx) / sstIdxEntrySize
	if nEntries == 0 {
		return nil, 0, false, nil
	}
	entry := func(i int) []byte { return idx[i*sstIdxEntrySize : (i+1)*sstIdxEntrySize] }
	// first sparse entry > (key, n)
	ub := sort.Search(nEntries, func(i int) bool {
		en := entry(i)
		if c := bytes.Compare(en[:sortedKeySize], key); c != 0 {
			return c > 0
		}
		return binary.BigEndian.Uint64(en[sortedKeySize:sortedKeySize+8]) > n
	})
	bi := ub - 1
	if bi < 0 {
		return nil, 0, false, nil
	}
	lo := binary.LittleEndian.Uint64(entry(bi)[sortedKeySize+8:])
	hi := uint64(len(e.sec[secSST]))
	if bi+1 < nEntries {
		hi = binary.LittleEndian.Uint64(entry(bi + 1)[sortedKeySize+8:])
	}
	raw, err := e.decodeAll(e.sec[secSST][lo:hi])
	if err != nil {
		return nil, 0, false, err
	}
	// scan rows for the largest <= (key, n); rows are sorted.
	pos := 0
	for pos < len(raw) {
		rk := raw[pos : pos+sortedKeySize]
		rb := binary.BigEndian.Uint64(raw[pos+sortedKeySize:])
		vlen, vn := binary.Uvarint(raw[pos+sortedKeySize+8:])
		vstart := pos + sortedKeySize + 8 + vn
		c := bytes.Compare(rk, key)
		if c > 0 || (c == 0 && rb > n) {
			break
		}
		if c == 0 {
			blk = rb
			found = true
			if vlen == 0 {
				val = nil
			} else {
				val = raw[vstart : vstart+int(vlen)]
			}
		}
		pos = vstart + int(vlen)
	}
	return val, blk, found, nil
}

// AccountDeletes calls fn for every account-delete row in this epoch.
func (e *Epoch) AccountDeletes(fn func(key []byte, block uint64)) {
	d := e.sec[secDeletes]
	for pos := 0; pos+deleteEntrySize <= len(d); pos += deleteEntrySize {
		fn(d[pos:pos+sortedKeySize], binary.BigEndian.Uint64(d[pos+sortedKeySize:]))
	}
}

// TxCandidates returns absolute candidate blocks for a tx-hash fingerprint.
func (e *Epoch) TxCandidates(fp uint64) []uint64 {
	lo, hi := e.txEF.lookup(fp)
	var out []uint64
	for i := lo; i < hi; i++ {
		out = append(out, e.Start+e.txBlk.get(i))
	}
	return out
}

// logidxLookup finds a key's posting list. keyLen distinguishes the addr
// (20) and topic (32) tables.
func (e *Epoch) logidxLookup(key []byte) ([]uint64, error) {
	li := e.sec[secLogidx]
	if len(li) < 4 {
		return nil, nil
	}
	nAddr := int(binary.LittleEndian.Uint32(li[0:4]))
	addrTable := li[4 : 4+nAddr*(20+8)]
	topicHdr := 4 + nAddr*(20+8)
	nTopic := int(binary.LittleEndian.Uint32(li[topicHdr : topicHdr+4]))
	topicTable := li[topicHdr+4 : topicHdr+4+nTopic*(32+8)]
	lists := li[topicHdr+4+nTopic*(32+8):]

	var (
		table    []byte
		count    int
		stride   int
	)
	switch len(key) {
	case 20:
		table, count, stride = addrTable, nAddr, 20+8
	case 32:
		table, count, stride = topicTable, nTopic, 32+8
	default:
		return nil, fmt.Errorf("logidx: bad key length %d", len(key))
	}
	i := sort.Search(count, func(i int) bool {
		return bytes.Compare(table[i*stride:i*stride+len(key)], key) >= 0
	})
	if i >= count || !bytes.Equal(table[i*stride:i*stride+len(key)], key) {
		return nil, nil
	}
	off := binary.LittleEndian.Uint64(table[i*stride+len(key):])
	ef, _, err := efUnmarshal(lists[off:], e.Count)
	if err != nil {
		return nil, err
	}
	rel := ef.values()
	out := make([]uint64, len(rel))
	for j, r := range rel {
		out[j] = e.Start + r
	}
	return out, nil
}

// LogAddrBlocks returns the absolute blocks where addr emitted a log.
func (e *Epoch) LogAddrBlocks(addr [20]byte) ([]uint64, error) { return e.logidxLookup(addr[:]) }

// LogTopicBlocks returns the absolute blocks where topic appeared.
func (e *Epoch) LogTopicBlocks(topic [32]byte) ([]uint64, error) { return e.logidxLookup(topic[:]) }

// ---------- epoch set ----------

// EpochSet is every sealed epoch in a directory, ascending and contiguous
// from block 0 upward (gaps are an error: sealing is strictly sequential).
type EpochSet struct {
	Epochs []*Epoch // ascending by Start
}

// OpenEpochSet opens every epoch_*.epoch in dir. An empty set is valid.
func OpenEpochSet(dir string) (*EpochSet, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	s := &EpochSet{}
	for _, en := range entries {
		if _, _, ok := ParseEpochFileName(en.Name()); !ok {
			continue
		}
		e, err := OpenEpoch(filepath.Join(dir, en.Name()))
		if err != nil {
			s.Close()
			return nil, fmt.Errorf("%s: %w", en.Name(), err)
		}
		s.Epochs = append(s.Epochs, e)
	}
	sort.Slice(s.Epochs, func(i, j int) bool { return s.Epochs[i].Start < s.Epochs[j].Start })
	for i, e := range s.Epochs {
		var wantStart uint64
		if i > 0 {
			wantStart = s.Epochs[i-1].End() + 1
		}
		if e.Start != wantStart {
			s.Close()
			return nil, fmt.Errorf("epoch gap: %d follows %d", e.Start, wantStart)
		}
	}
	return s, nil
}

func (s *EpochSet) Close() {
	for _, e := range s.Epochs {
		e.Close()
	}
	s.Epochs = nil
}

// SealedEnd returns the last sealed block, ok=false for an empty set.
func (s *EpochSet) SealedEnd() (uint64, bool) {
	if len(s.Epochs) == 0 {
		return 0, false
	}
	return s.Epochs[len(s.Epochs)-1].End(), true
}

// At returns the epoch containing block n.
func (s *EpochSet) At(n uint64) (*Epoch, bool) {
	i := sort.Search(len(s.Epochs), func(i int) bool { return s.Epochs[i].End() >= n })
	if i == len(s.Epochs) || s.Epochs[i].Start > n {
		return nil, false
	}
	return s.Epochs[i], true
}
