package verify

// CheckBase is the snapshot producer's pre-rename gate (DESIGN.md "Our own
// state sync"). For CLIENTS, verification is the load: dumping a snapshot
// into Firewood computes the root as a side effect, so there is nothing extra
// to run. This is the same work, done by the PRODUCER, before the file gets a
// name a manifest could publish. It is the only defense against shipping a
// snapshot whose rows do not rebuild header(B).Root, and because it uses the
// consumer's exact load path it proves the artifact LOADS, not merely that it
// hashes.

import (
	"fmt"
	"os"
	"path/filepath"

	ffi "github.com/ava-labs/firewood-go-ethhash/ffi"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/epochdb/state"
)

// checkBaseBatch matches exec's baseLoadBatch: bounded so a full-mainnet base
// does not build one giant cgo batch.
const checkBaseBatch = 200_000

// CheckBase loads one base file (by path, so base_<B>.tmp works) into a
// throwaway Firewood and requires the resulting root to equal the footer's.
// Mirrors exec.startFromBase exactly, code rows included: they are not trie
// content and are skipped.
//
// ponytail: the throwaway Firewood lands beside the file and is as large as
// the state itself (~125GB at full mainnet), so a fold transiently doubles
// the merkle store. Point it at a scratch mount if that ever bites.
func CheckBase(path string) error {
	b, err := state.OpenBaseFile(path)
	if err != nil {
		return err
	}
	defer b.Close()

	tmp, err := os.MkdirTemp(filepath.Dir(path), ".checkbase-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	tdb, fw, _, err := newThrowawayFirewood(tmp)
	if err != nil {
		return err
	}
	defer tdb.Close()

	var (
		batch            []ffi.BatchOp
		root             ffi.Hash
		nAcct, nSlot, nC uint64
	)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		r, err := fw.Firewood.Update(batch)
		if err != nil {
			return fmt.Errorf("check base %s: firewood update: %w", path, err)
		}
		root, batch = r, batch[:0]
		return nil
	}
	err = b.WalkRows(func(key, val []byte) error {
		switch key[0] {
		case 'a': // keccak(addr) | 32 pad bytes
			nAcct++
			batch = append(batch, ffi.Put(cloneBytes(key[1:33]), cloneBytes(val)))
		case 's': // keccak(addr) | keccak(slot)
			nSlot++
			enc, err := rlp.EncodeToBytes(val)
			if err != nil {
				return err
			}
			batch = append(batch, ffi.Put(cloneBytes(key[1:65]), enc))
		case 'c': // code blob: not trie content
			nC++
			return nil
		default:
			return fmt.Errorf("check base %s: unknown row kind %q", path, key[0])
		}
		if len(batch) >= checkBaseBatch {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}
	if got := common.Hash(root); got != b.StateRoot() {
		return fmt.Errorf("base %s: rows rebuild root %x, footer claims header(%d).Root %x (%d accounts, %d slots, %d code blobs)",
			filepath.Base(path), got, b.Block(), b.StateRoot(), nAcct, nSlot, nC)
	}
	return nil
}

// cloneBytes copies a row slice that aliases the base file's decode buffer;
// the batch outlives the callback.
func cloneBytes(b []byte) []byte { return append([]byte(nil), b...) }
