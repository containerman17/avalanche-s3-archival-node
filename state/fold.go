package state

// THE PRUNING NODE'S PRODUCER (DESIGN.md "Our own state sync").
//
// ONE PRODUCTION RULE: at every canonical boundary (cumulative tx count, the
// same for everyone), an ARCHIVAL node emits epoch K and a PRUNING node emits
// snapshot(K) = snapshot(K-1) folded with the period's own captured writes.
// `epochdb fold` is the exact sibling of `epochdb seal`: same babysitter slot,
// same cadence, separate process, safe beside a live fetch+exec because it
// reads only durable immutable data at or below the boundary and holds no
// lock the executor wants.
//
// snapshot(K) is a pure function of chain content at or below B plus the
// binary version, hence bit-identical across nodes. The determinism argument,
// in one place so it can be checked:
//
//  1. Boundary: the period's tx count, counted byte-for-byte as seal counts
//     it (extractTxHashes over innerEthBlock), seeded by the previous
//     snapshot's footer CumTx. Chain content only.
//  2. Row set: last write wins per (key, block, seq); capture is deterministic
//     execution output and cook's sort is total.
//  3. No map reaches the output unordered: the tombstone map is lookup-only
//     and the new code hashes are explicitly sorted before the 'c' merge.
//  4. Emission order: sorted-stream merge over one keyspace; the bottom cursor
//     is sorted on disk (a base file) or sorted in RAM (the genesis alloc).
//  5. zstd: klauspost EncodeAll SpeedBestCompression, no dict, pinned in
//     go.mod. Same policy as epochs: a library bump changes the bytes at the
//     next boundary and shows up loudly as two producers disagreeing.
//  6. SST block boundaries: flush at >= sstBlockTarget raw bytes, a function
//     of the row bytes in order, shared with WriteBase by construction.
//  7. Bloom: m from the exact row count, bits OR-accumulated. Headers are
//     verbatim RLP. The footer holds only B, hdrFrom, cumTx, root and the
//     section table: no timestamps, hostnames, paths or wall clock.
//  8. One goroutine start to finish.
//  9. Values are captured post-images VERBATIM, no re-encoding at all.
//
// (9) is the load-bearing one. There is NO StackTrie pass and no storage-root
// reconstruction: firewood-ethhash manages storage roots internally (its
// storage tries hash to the zero hash), captured account RLP therefore embeds
// a zero storage root, the RPC read path substitutes SentinelStorageRoot, and
// graft/evm/firewood's UpdateAccount is literally ffi.Put(keccak(addr),
// rlp(account)). So the rows this file emits are byte-for-byte what a
// consumer's Firewood wants. verify's zero-root unit test pins that premise
// instead of assuming it. There is NO pre-rename gate (deleted 2026-07-29,
// user: "extra validation of already-validated data no one asked for"):
// the producer executed every block root-verified, and every consumer
// recomputes the root as a side effect of loading the file.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"
	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/fetch"
	"github.com/holiman/uint256"
)

// FoldSnapshots emits every snapshot the local data supports, in order.
//
// Loop invariant: nothing is persisted about the fold itself, so every
// restart re-derives (floor, boundary, rows) from the newest base footer, the
// containers and exechead. Idempotence IS determinism here.
func FoldSnapshots(st *dist.Store, alloc types.GenesisAlloc, epochTxs uint64) error {
	dir := st.Dir()
	if err := sweepBases(st); err != nil {
		return err
	}
	// A pruning node never seals. An epoch file here means the directory was
	// mis-deployed (or someone ran seal on it), and folding it would produce
	// a snapshot nobody else can reproduce.
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if _, _, ok := ParseEpochMarkerName(e.Name()); ok {
			return fmt.Errorf("fold: %s holds sealed epochs (%s): a node either seals epochs or folds snapshots, never both", dir, e.Name())
		}
	}
	for {
		// Cook first: the merge reads sorted buckets, and CookIndex is
		// idempotent and safe beside a live exec.
		if err := CookIndex(dir); err != nil {
			return err
		}
		more, err := foldOnce(st, alloc, epochTxs)
		if err != nil || !more {
			return err
		}
	}
}

// sweepBases removes crash leftovers: every base_*.tmp, and every base file
// but the newest (the fold commits by rename and unlinks the old one after,
// so two can coexist for one window).
func sweepBases(st *dist.Store) error {
	dir := st.Dir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	dirty := false
	for _, e := range entries {
		n := e.Name()
		if strings.HasPrefix(n, "base_") && strings.HasSuffix(n, ".tmp") {
			if err := os.Remove(filepath.Join(dir, n)); err != nil && !os.IsNotExist(err) {
				return err
			}
			log.Printf("fold: removed crash leftover %s", n)
			dirty = true
		}
	}
	_, all, ok, err := newestBase(dir)
	if err != nil {
		return err
	}
	if ok && len(all) > 1 {
		keep, err := ReadMarker(dir, all[len(all)-1])
		if err != nil {
			return err
		}
		for _, n := range all[:len(all)-1] {
			// Drop the superseded artifact with its marker, unless it IS the
			// surviving one (two markers can name the same content). A spool
			// file already uploaded and released is simply not there.
			if hash, err := ReadMarker(dir, n); err == nil && hash != keep {
				if err := os.Remove(st.SpoolPath(hash)); err != nil && !os.IsNotExist(err) {
					return err
				}
			}
			if err := os.Remove(filepath.Join(dir, n)); err != nil && !os.IsNotExist(err) {
				return err
			}
			log.Printf("fold: removed superseded %s (newest is %s)", n, all[len(all)-1])
			dirty = true
		}
	}
	if dirty {
		return syncDir(dir)
	}
	return nil
}

// foldOnce produces at most one snapshot. more=true means a boundary was
// crossed and the caller should look for the next one.
func foldOnce(st *dist.Store, alloc types.GenesisAlloc, epochTxs uint64) (more bool, err error) {
	dir := st.Dir()
	store, err := OpenReadOnly(dir)
	if err != nil {
		return false, err
	}
	defer store.Close()
	execHead, ok := store.ExecHead()
	if !ok || execHead == 0 {
		return false, fmt.Errorf("fold: no exec head, nothing replayed yet")
	}
	reader, err := fetch.OpenReader(dir)
	if err != nil {
		return false, err
	}
	defer reader.Close()

	f := &folder{dir: dir, store: store, alloc: alloc}
	if base, ok, err := OpenBase(st); err != nil {
		return false, err
	} else if ok {
		defer base.Close()
		f.base, f.F, f.prevCumTx = base, base.Block(), base.CumTx()
	}

	B, periodTx, ok, err := foldBoundary(reader, f.F, execHead, epochTxs)
	if err != nil {
		return false, err
	}
	if !ok {
		log.Printf("fold: tail %d..%d stays unfolded (below epoch-txs=%d)", f.F+1, execHead, epochTxs)
		return false, nil
	}
	f.B = B

	// RETIREMENT LAG. Folding retires every raw bucket fully at or below B,
	// and exec's crash walk-back re-reads containers down to
	// exechead - walkBackBudget. Waiting for one whole bucket of headroom
	// makes the lowest surviving block strictly above that floor, so a kill -9
	// right after a fold can never leave a node that cannot reconcile. The
	// other half of the guarantee is exec.New refusing a --commit-every whose
	// walk-back budget reaches past one bucket.
	if execHead < B+BucketBlocks {
		log.Printf("fold: boundary at %d (%d txs) reached, waiting for exechead %d to pass %d: one raw bucket of retirement headroom",
			B, periodTx, execHead, B+BucketBlocks)
		return false, nil
	}

	t0 := time.Now()
	meta, err := f.meta(periodTx)
	if err != nil {
		return false, err
	}

	// PASS 1 counts. WriteBase's bloom needs the exact row count up front and
	// buffering every key would be an OOM at full mainnet, so the merge runs
	// twice over the same immutable inputs instead. (Both passes read the same
	// bytes: the retirement guard above also means every bucket at or below B
	// is fully cooked and never re-cooked, so cook cannot move under us.)
	var rowCount uint64
	if err := f.merge(func(key, val []byte) error { rowCount++; return nil }); err != nil {
		return false, err
	}

	w, err := newBaseWriter(st, meta, rowCount)
	if err != nil {
		return false, err
	}
	defer w.Abort()
	if err := f.merge(w.Add); err != nil {
		return false, err
	}
	if _, err := w.Finish(); err != nil {
		return false, err
	}
	if err := w.Commit(); err != nil {
		return false, err
	}

	fi, _ := os.Stat(st.SpoolPath(w.hash))
	log.Printf("fold: %s (%s) rows=%d txs=%d (cum %d) root=%x size=%.1fMB in %s",
		BaseMarkerName(meta.Block), w.hash[:12], rowCount, periodTx, meta.CumTx, meta.Root,
		float64(fi.Size())/1e6, time.Since(t0).Round(time.Millisecond))

	if err := sweepBases(st); err != nil {
		return false, err
	}
	// Single deleter, reused verbatim from seal: idempotent and
	// IsNotExist-tolerant, so a crash before or during it just reruns.
	if err := deleteSealedRaw(dir, B); err != nil {
		return false, err
	}
	return true, nil
}

// foldBoundary is THE PRODUCTION RULE: walk containers above the floor and
// cut at the first block whose cumulative tx count reaches epochTxs. Counted
// byte-for-byte as gatherEpoch counts it (state/seal.go), so a pruning node's
// boundaries are identical to an archival node's epoch cuts by induction.
// ok=false = the period has not filled yet (or staging has a gap).
func foldBoundary(reader *fetch.Reader, F, execHead, epochTxs uint64) (B, txs uint64, ok bool, err error) {
	var hashes []common.Hash
	for n := F + 1; n <= execHead; n++ {
		container, got, err := reader.GetByHeight(n)
		if err != nil {
			return 0, 0, false, err
		}
		if !got {
			// A staging gap below the exec head should not happen; never fold
			// past one either way (seal's rule).
			return 0, 0, false, nil
		}
		hashes, err = extractTxHashes(innerEthBlock(container), hashes[:0])
		if err != nil {
			return 0, 0, false, fmt.Errorf("fold: block %d txs: %w", n, err)
		}
		txs += uint64(len(hashes))
		if txs >= epochTxs {
			return n, txs, true, nil
		}
	}
	return 0, 0, false, nil
}

// ---------- the merge ----------

type folder struct {
	dir   string
	store *Store
	base  *Base // nil when the floor is 0: the genesis alloc is the bottom
	alloc types.GenesisAlloc

	F, B      uint64
	prevCumTx uint64
}

// meta builds the footer and the BLOCKHASH header window [max(0,B-256), B].
//
// SAE SEAM, recorded not built: post-Helicon, header(B).Root is the SETTLED
// root, not the post-execution state of B. When SAE execution lands, this
// root and exec.startFromBase's check both switch to the executor's own root
// ring at B. Pre-SAE the two are equal, so there is nothing to do yet.
func (f *folder) meta(periodTx uint64) (BaseMeta, error) {
	// The window bottoms out at block 1, not 0: block 0 is genesis, it has no
	// container and the executor never appends a header for it, so demanding
	// one would fail every fold whose period starts near genesis. BLOCKHASH(0)
	// reaching into a base_<B<256> is the only thing given up, and no real
	// boundary lands that low.
	m := BaseMeta{Block: f.B, CumTx: f.prevCumTx + periodTx, HdrFrom: 1}
	if f.B > baseHeaderWindow {
		m.HdrFrom = f.B - baseHeaderWindow
	}
	for n := m.HdrFrom; n <= f.B; n++ {
		raw, ok, err := f.store.HeaderRLP(n)
		if err != nil {
			return m, err
		}
		if !ok && f.base != nil {
			// A short period can reach below the previous floor, where the raw
			// headers log was retired: the old base carries its own window.
			raw, ok, err = f.base.HeaderRLP(n)
			if err != nil {
				return m, err
			}
		}
		if !ok {
			return m, fmt.Errorf("fold: header %d is missing, cannot build the BLOCKHASH window of base_%d", n, f.B)
		}
		m.Headers = append(m.Headers, raw)
	}
	var hdr types.Header
	if err := rlp.DecodeBytes(m.Headers[len(m.Headers)-1], &hdr); err != nil {
		return m, fmt.Errorf("fold: decode header %d: %w", f.B, err)
	}
	m.Root = hdr.Root
	return m, nil
}

// rowCursor is a pull cursor over a key-sorted row stream.
type rowCursor interface {
	// next advances; ok=false = exhausted. key and val may alias internal
	// buffers, valid until the following next.
	next() (key, val []byte, ok bool, err error)
	close()
}

// merge runs the whole key-ordered fold and calls emit per output row, in
// strictly ascending key order. Called twice per snapshot (count, then
// write); both runs read the same immutable inputs and emit the same rows.
func (f *folder) merge(emit func(key, val []byte) error) error {
	bottom, err := f.newBottom()
	if err != nil {
		return err
	}
	defer bottom.close()
	delta, err := f.newDelta()
	if err != nil {
		return err
	}
	defer delta.close()

	var (
		bKey, bVal []byte
		bOK        bool
		// tombstones[addr] = the highest block in (F, B] at which the account
		// was explicitly deleted. Collected over ALL in-window 'a' delete
		// records, not just the winners, because a delete followed by a
		// recreate still kills the storage written before it. This mirrors
		// History.lastAccountDelete byte-for-byte, which is what makes a
		// folded read answer identically to a descent read.
		tombstones = map[common.Address]uint64{}
		newCode    = map[common.Hash]bool{}
		codeAdd    []common.Hash
		ci         int
		codeReady  bool
	)
	advBottom := func() error {
		var err error
		bKey, bVal, bOK, err = bottom.next()
		return err
	}
	if err := advBottom(); err != nil {
		return err
	}
	if err := delta.advance(); err != nil {
		return err
	}

	for {
		// 'a' < 'c' < 's' in the one sorted keyspace, so the account region is
		// over the moment neither pending row is an account row: the tombstone
		// map and the new code set are complete exactly when they are first
		// needed.
		if !codeReady && !(bOK && bKey[0] == recKindAccount) && !(delta.ok && delta.key[0] == recKindAccount) {
			codeAdd = sortedHashes(newCode)
			codeReady = true
		}

		var best []byte
		if bOK {
			best = bKey
		}
		if delta.ok && (best == nil || bytes.Compare(delta.key[:], best) < 0) {
			best = delta.key[:]
		}
		var ck [sortedKeySize]byte
		if codeReady && ci < len(codeAdd) {
			ck = epochCodeKey(codeAdd[ci])
			if best == nil || bytes.Compare(ck[:], best) < 0 {
				best = ck[:]
			}
		}
		if best == nil {
			return nil
		}
		var k [baseKeySize]byte
		copy(k[:], best)

		// Consume every stream sitting on k. The delta shadows the bottom (it
		// is strictly newer); a code row present in both is the same
		// content-addressed blob, so the bottom's copy wins and no code.log
		// read happens at all.
		haveDelta := delta.ok && delta.key == k
		haveBottom := bOK && bytes.Equal(bKey, k[:])
		haveCode := codeReady && ci < len(codeAdd) && ck == k

		switch k[0] {
		case recKindAccount:
			if haveDelta {
				if delta.lastDel > 0 {
					tombstones[common.Address(k[1:21])] = delta.lastDel
				}
				// An account whose winning record is a delete is simply gone:
				// a snapshot is a post-image, it has no tombstones of its own.
				if !delta.del {
					if err := emit(k[:], delta.val); err != nil {
						return err
					}
					if h, ok := accountCodeHash(delta.val); ok && h != types.EmptyCodeHash && h != (common.Hash{}) {
						newCode[h] = true
					}
				}
			} else if haveBottom {
				// The bottom's code is already covered by 'c'(K-1) by
				// induction, so nothing to collect here.
				if err := emit(k[:], bVal); err != nil {
					return err
				}
			}

		case recKindCodeUse:
			// 'c'(K) = 'c'(K-1) union the code of every surviving account row:
			// a grow-only union, so whichever snapshot answers an account read
			// also carries its code.
			// ponytail: grow-only means the blobs of destructed contracts ride
			// along forever (~500MB at full mainnet). Mark-and-sweep against
			// the surviving account rows if the 'c' section ever matters.
			switch {
			case haveBottom:
				if err := emit(k[:], bVal); err != nil {
					return err
				}
			case haveCode:
				// Resolved lazily, only for hashes the bottom does not already
				// carry: that is also what keeps genesis-alloc code working,
				// since alloc code is never deployed by a block and therefore
				// never lands in code.log.
				blob, ok, err := f.store.code.Get(common.BytesToHash(k[1:33]))
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("fold: code %x referenced by an account row is not in code.log", k[1:33])
				}
				if err := emit(k[:], blob); err != nil {
					return err
				}
			}

		case recKindStorage:
			tomb, dead := tombstones[common.Address(k[1:21])]
			switch {
			case haveDelta:
				// STRICT inequality, exactly History.StorageAt's rule: a slot
				// written in the same block that destroyed the account is the
				// recreated contract's, and survives.
				if !delta.del && !(dead && tomb > delta.blk) {
					if err := emit(k[:], delta.val); err != nil {
						return err
					}
				}
			case haveBottom:
				// A base row's write block counts as F, which is how
				// History.search reports base hits, and every tombstone is
				// above F by construction: any delete at all kills it.
				if !dead {
					if err := emit(k[:], bVal); err != nil {
						return err
					}
				}
			}

		default:
			return fmt.Errorf("fold: unknown row kind %q", k[0])
		}

		if haveDelta {
			if err := delta.advance(); err != nil {
				return err
			}
		}
		if haveBottom {
			if err := advBottom(); err != nil {
				return err
			}
		}
		if haveCode {
			ci++
		}
	}
}

func sortedHashes(set map[common.Hash]bool) []common.Hash {
	out := make([]common.Hash, 0, len(set))
	for h := range set {
		out = append(out, h)
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i][:], out[j][:]) < 0 })
	return out
}

// ---------- bottom cursor ----------

func (f *folder) newBottom() (rowCursor, error) {
	if f.base != nil {
		return &baseCursor{it: f.base.iter()}, nil
	}
	rows, err := allocRows(f.alloc)
	if err != nil {
		return nil, err
	}
	return &sliceCursor{rows: rows}, nil
}

type baseCursor struct{ it *baseIter }

func (c *baseCursor) next() ([]byte, []byte, bool, error) { return c.it.next() }
func (c *baseCursor) close()                              {}

type sliceCursor struct {
	rows []BaseRow
	i    int
}

func (c *sliceCursor) next() ([]byte, []byte, bool, error) {
	if c.i >= len(c.rows) {
		return nil, nil, false, nil
	}
	r := &c.rows[c.i]
	c.i++
	return r.Key[:], r.Val, true, nil
}
func (c *sliceCursor) close() {}

// allocRows renders the genesis alloc as base rows: snapshot(0) is the alloc
// and is hardcoded in the client, so it never exists as a file, but the first
// fold has to merge against it.
//
// Account values use the SAME encoding as the write capture, zero storage
// root included, so a row is indistinguishable from a captured one and the
// consumer's Firewood load treats them uniformly.
func allocRows(alloc types.GenesisAlloc) ([]BaseRow, error) {
	var rows []BaseRow
	code := map[common.Hash][]byte{}
	for addr, ga := range alloc {
		if len(ga.Storage) > 0 {
			// The recorded gap (DESIGN.md): dead on both networks today,
			// whose single alloc account with code has no storage. Fail loud
			// rather than emit a snapshot missing state.
			return nil, fmt.Errorf("fold: genesis alloc account %x carries %d storage slots, which the fold has never had to render", addr, len(ga.Storage))
		}
		acc := types.StateAccount{
			Nonce:    ga.Nonce,
			Balance:  new(uint256.Int),
			Root:     common.Hash{},
			CodeHash: types.EmptyCodeHash.Bytes(),
		}
		if ga.Balance != nil {
			acc.Balance = uint256.MustFromBig(ga.Balance)
		}
		if len(ga.Code) > 0 {
			h := crypto.Keccak256Hash(ga.Code)
			acc.CodeHash = h.Bytes()
			code[h] = ga.Code
		}
		val, err := rlp.EncodeToBytes(&acc)
		if err != nil {
			return nil, err
		}
		var r BaseRow
		copy(r.Key[:], accountKey(addr))
		r.Val = val
		rows = append(rows, r)
	}
	for h, blob := range code {
		rows = append(rows, BaseRow{Key: epochCodeKey(h), Val: blob})
	}
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].Key[:], rows[j].Key[:]) < 0 })
	return rows, nil
}

// ---------- delta cursor ----------

// deltaCursor merges the cooked sorted buckets overlapping (F, B] into one
// key-ordered stream of winners. Buckets are block-disjoint and each is
// sorted by (key, block) with same-(key, block) duplicates already collapsed
// to the last write by cook, so the winner for a key is simply its highest
// in-window block.
//
// ponytail: linear min-scan over the cursors instead of a heap. A period is
// ~13 buckets at full mainnet, where log2(13) buys nothing; swap in
// container/heap if a deployment ever folds hundreds of buckets at once.
type deltaCursor struct {
	cs     []*bucketCursor
	lo, hi uint64 // window (lo, hi]
	buf    []byte

	key     [sortedKeySize]byte
	val     []byte // nil for a delete / zero write
	blk     uint64
	del     bool
	lastDel uint64 // highest in-window delete block for this key, 0 = none
	ok      bool
}

type bucketCursor struct {
	sb *sortedBucket
	i  int
}

func (f *folder) newDelta() (*deltaCursor, error) {
	d := &deltaCursor{lo: f.F, hi: f.B}
	for b := f.F / BucketBlocks; b*BucketBlocks <= f.B; b++ {
		if _, err := os.Stat(filepath.Join(f.dir, sortedName(b))); err != nil {
			if os.IsNotExist(err) {
				continue // a bucket with no captured writes has no sorted file
			}
			d.close()
			return nil, err
		}
		sb, _, err := openSortedBucket(f.dir, b)
		if err != nil {
			d.close()
			return nil, fmt.Errorf("fold: open sorted bucket %05d: %w", b, err)
		}
		d.cs = append(d.cs, &bucketCursor{sb: sb})
	}
	return d, nil
}

func (d *deltaCursor) close() {
	for _, c := range d.cs {
		c.sb.close()
	}
	d.cs = nil
}

// advance moves to the next key that has any record inside the window.
func (d *deltaCursor) advance() error {
	for {
		var best []byte
		for _, c := range d.cs {
			if c.i >= c.sb.n {
				continue
			}
			k := c.sb.rec(c.i)[:sortedKeySize]
			if best == nil || bytes.Compare(k, best) < 0 {
				best = k
			}
		}
		if best == nil {
			d.ok = false
			return nil
		}
		var k [sortedKeySize]byte
		copy(k[:], best)

		var (
			winSB   *sortedBucket
			winBlk  uint64
			winOff  uint64
			winLen  uint32
			have    bool
			lastDel uint64
		)
		for _, c := range d.cs {
			for c.i < c.sb.n {
				r := c.sb.rec(c.i)
				if !bytes.Equal(r[:sortedKeySize], k[:]) {
					break
				}
				blk := binary.BigEndian.Uint64(r[53:61])
				// Per RECORD, not per bucket: a bucket straddles the floor and
				// the boundary at both ends of the period.
				if blk > d.lo && blk <= d.hi {
					vlen := binary.BigEndian.Uint32(r[69:73])
					if !have || blk > winBlk {
						winSB, winBlk, winOff, winLen, have = c.sb, blk, binary.BigEndian.Uint64(r[61:69]), vlen, true
					}
					if k[0] == recKindAccount && vlen == 0 && blk > lastDel {
						lastDel = blk
					}
				}
				c.i++
			}
		}
		if !have {
			continue // every record for this key is outside the window
		}
		d.val = nil
		if winLen > 0 {
			if uint32(cap(d.buf)) < winLen {
				d.buf = make([]byte, winLen)
			}
			d.val = d.buf[:winLen]
			if _, err := winSB.wl.ReadAt(d.val, int64(winOff)); err != nil {
				return fmt.Errorf("fold: writelog bucket %05d read at %d: %w", winSB.bucket, winOff, err)
			}
		}
		d.key, d.blk, d.del, d.lastDel, d.ok = k, winBlk, winLen == 0, lastDel, true
		return nil
	}
}
