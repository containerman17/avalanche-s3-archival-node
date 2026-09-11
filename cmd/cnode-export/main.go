// cnode-export reads the database of a cleanly STOPPED avalanchego node
// (pebbledb, mainnet C-chain, state synced) and writes the flat EVM state held
// by coreth's snapshot disk layer plus the blocks from that state's block to
// the head into simple files a Rust node loads in one pass. Never writes to
// the node database.
//
// Database nesting (avalanchego chains/manager.go, coreth plugin/evm/vm_database.go):
//
//	pebbledb at <data>/db/mainnet/pebble
//	  prefixdb.New(chainID[:])          sha256(chainID)
//	    prefixdb.New("vm")              joined: sha256(sha256(chainID) || "vm")
//	      prefixdb.NewNested("ethdb")   sha256("ethdb") appended, not joined
//	        libevm rawdb keys
//
// Output files in -out:
//
//	meta.json     {"height","hash","state_root","head_height","accounts","slots","codes"}
//	header.rlp    header RLP at height S verbatim as stored (coreth header, extra fields included)
//	accounts.bin  105 B records: [32 hash][8 nonce LE][32 balance BE][32 code hash][1 multicoin 0/1], key order
//	storage.bin   96 B records: [32 account hash][32 slot hash][32 value BE left padded], key order
//	code.bin      [32 code hash][4 len LE][code], one per distinct non-empty code hash, hash order
//	blocks.bin    [8 height LE][4 len LE][block RLP] for S..head, block RLP =
//	              list(header, txs, uncles, version, extData), the bytes
//	              rlp.EncodeToBytes(*types.Block) yields with coreth's extras registered,
//	              built from the stored header RLP and body RLP without re-encoding.
package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"sort"

	"github.com/ava-labs/avalanchego/database/pebbledb"
	"github.com/ava-labs/avalanchego/database/prefixdb"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/logging"
	evmdb "github.com/ava-labs/avalanchego/vms/evm/database"
	"github.com/ava-labs/avalanchego/vms/evm/sync/customrawdb"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/rlp"
)

const (
	cchainID = "2q9e4r6Mu3U68nU1fYjgbR6JvwrRx36CohpAX5UQxse55x1Q5"
	progress = 5_000_000
)

var (
	vmDBPrefix  = []byte("vm")    // avalanchego chains/manager.go VMDBPrefix
	ethDBPrefix = []byte("ethdb") // coreth plugin/evm/vm.go ethDBPrefix
)

// journalGenerator mirrors graft/evm core/state/snapshot/journal.go.
type journalGenerator struct {
	Wiping   bool
	Done     bool
	Marker   []byte
	Accounts uint64
	Slots    uint64
	Storage  uint64
}

// slimAccount is libevm types.SlimAccount without the registered-extras
// dependency: coreth appends an IsMultiCoin bool (false = 0x80) as Rest[0].
type slimAccount struct {
	Nonce    uint64
	Balance  *big.Int
	Root     []byte
	CodeHash []byte
	Rest     []rlp.RawValue `rlp:"tail"`
}

// headerHead is the leading fields of a header RLP, enough to walk parents
// and compare state roots without decoding coreth's extra fields.
type headerHead struct {
	ParentHash common.Hash
	UncleHash  common.Hash
	Coinbase   common.Address
	Root       common.Hash
	Rest       []rlp.RawValue `rlp:"tail"`
}

type meta struct {
	Height     uint64      `json:"height"`
	Hash       common.Hash `json:"hash"`
	StateRoot  common.Hash `json:"state_root"`
	HeadHeight uint64      `json:"head_height"`
	Accounts   uint64      `json:"accounts"`
	Slots      uint64      `json:"slots"`
	Codes      uint64      `json:"codes"`
}

func main() {
	data := flag.String("data", "", "avalanchego data dir (db at <data>/db/mainnet/pebble)")
	out := flag.String("out", "", "output dir")
	flag.Parse()
	if *data == "" || *out == "" {
		log.Fatal("-data and -out are required")
	}
	path := filepath.Join(*data, "db", "mainnet", "pebble")
	if _, err := os.Stat(filepath.Join(path, "CURRENT")); err != nil {
		path = filepath.Join(*data, "db", "mainnet")
	}
	// ponytail: pebbledb.New has no read-only option, we open normally and never write.
	base, err := pebbledb.New(path, nil, logging.NoLog{}, nil)
	if err != nil {
		log.Fatalf("open %s: %v", path, err)
	}
	defer base.Close()
	chainID := ids.FromStringOrPanic(cchainID)
	db := rawdb.NewDatabase(evmdb.New(prefixdb.NewNested(ethDBPrefix, prefixdb.New(vmDBPrefix, prefixdb.New(chainID[:], base)))))
	if err := export(db, *out); err != nil {
		log.Fatal(err)
	}
}

func export(db ethdb.Database, out string) error {
	blob := rawdb.ReadSnapshotGenerator(db)
	if len(blob) == 0 {
		return errors.New("missing snapshot generator marker, no usable snapshot")
	}
	var gen journalGenerator
	if err := rlp.DecodeBytes(blob, &gen); err != nil {
		return fmt.Errorf("decode snapshot generator: %w", err)
	}
	if !gen.Done {
		return fmt.Errorf("snapshot generation not finished (marker %x), start the node and let it finish", gen.Marker)
	}
	root := rawdb.ReadSnapshotRoot(db)
	if root == (common.Hash{}) {
		return errors.New("missing snapshot root")
	}
	snapHash, err := customrawdb.ReadSnapshotBlockHash(db)
	if err != nil {
		return fmt.Errorf("read snapshot block hash: %w", err)
	}

	// Walk headers back from the head until the state root matches.
	head := rawdb.ReadHeadHeaderHash(db)
	headNum := rawdb.ReadHeaderNumber(db, head)
	if headNum == nil {
		return fmt.Errorf("head header %s has no number", head)
	}
	type ref struct {
		num  uint64
		hash common.Hash
	}
	var chain []ref // head first
	var snapRaw rlp.RawValue
	for h, n := head, *headNum; ; n-- {
		raw := rawdb.ReadHeaderRLP(db, h, n)
		if len(raw) == 0 {
			return fmt.Errorf("missing header %d %s", n, h)
		}
		var hh headerHead
		if err := rlp.DecodeBytes(raw, &hh); err != nil {
			return fmt.Errorf("decode header %d: %w", n, err)
		}
		chain = append(chain, ref{n, h})
		if hh.Root == root {
			if h != snapHash {
				return fmt.Errorf("snapshot block hash %s does not match block %d %s with the snapshot root", snapHash, n, h)
			}
			snapRaw = raw
			break
		}
		if n == 0 {
			return fmt.Errorf("no header below head %d has snapshot root %s", *headNum, root)
		}
		h = hh.ParentHash
	}
	s := chain[len(chain)-1]
	log.Printf("snapshot at %d %s root %s, head %d", s.num, s.hash, root, *headNum)

	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "header.rlp"), snapRaw, 0o644); err != nil {
		return err
	}

	m := meta{Height: s.num, Hash: s.hash, StateRoot: root, HeadHeight: *headNum}
	codes := map[common.Hash]struct{}{}

	// accounts.bin
	err = withFile(filepath.Join(out, "accounts.bin"), func(w *bufio.Writer) error {
		it := db.NewIterator(rawdb.SnapshotAccountPrefix, nil)
		defer it.Release()
		var rec [105]byte
		for it.Next() {
			if len(it.Key()) != 1+common.HashLength {
				continue
			}
			var a slimAccount
			if err := rlp.DecodeBytes(it.Value(), &a); err != nil {
				return fmt.Errorf("decode account %x: %w", it.Key()[1:], err)
			}
			copy(rec[:32], it.Key()[1:])
			binary.LittleEndian.PutUint64(rec[32:40], a.Nonce)
			clear(rec[40:72])
			if a.Balance != nil {
				a.Balance.FillBytes(rec[40:72])
			}
			code := types.EmptyCodeHash
			if len(a.CodeHash) != 0 {
				code = common.BytesToHash(a.CodeHash)
				codes[code] = struct{}{}
			}
			copy(rec[72:104], code[:])
			rec[104] = 0
			if len(a.Rest) > 0 {
				var multi bool
				if err := rlp.DecodeBytes(a.Rest[0], &multi); err != nil {
					return fmt.Errorf("decode multicoin flag of %x: %w", it.Key()[1:], err)
				}
				if multi {
					rec[104] = 1
				}
			}
			if _, err := w.Write(rec[:]); err != nil {
				return err
			}
			m.Accounts++
			if m.Accounts%progress == 0 {
				log.Printf("accounts %d", m.Accounts)
			}
		}
		return it.Error()
	})
	if err != nil {
		return err
	}
	log.Printf("accounts %d done", m.Accounts)

	// storage.bin
	err = withFile(filepath.Join(out, "storage.bin"), func(w *bufio.Writer) error {
		it := db.NewIterator(rawdb.SnapshotStoragePrefix, nil)
		defer it.Release()
		var rec [96]byte
		for it.Next() {
			if len(it.Key()) != 1+2*common.HashLength {
				continue
			}
			val, _, err := rlp.SplitString(it.Value())
			if err != nil || len(val) > 32 {
				return fmt.Errorf("bad storage value at %x: %v", it.Key()[1:], err)
			}
			copy(rec[:64], it.Key()[1:])
			clear(rec[64:])
			copy(rec[96-len(val):], val)
			if _, err := w.Write(rec[:]); err != nil {
				return err
			}
			m.Slots++
			if m.Slots%progress == 0 {
				log.Printf("slots %d", m.Slots)
			}
		}
		return it.Error()
	})
	if err != nil {
		return err
	}
	log.Printf("slots %d done", m.Slots)

	// code.bin
	hashes := make([]common.Hash, 0, len(codes))
	for h := range codes {
		hashes = append(hashes, h)
	}
	sort.Slice(hashes, func(i, j int) bool { return bytes.Compare(hashes[i][:], hashes[j][:]) < 0 })
	err = withFile(filepath.Join(out, "code.bin"), func(w *bufio.Writer) error {
		var n [4]byte
		for _, h := range hashes {
			code := rawdb.ReadCode(db, h)
			if len(code) == 0 {
				return fmt.Errorf("code %s referenced by an account is missing", h)
			}
			binary.LittleEndian.PutUint32(n[:], uint32(len(code)))
			for _, b := range [][]byte{h[:], n[:], code} {
				if _, err := w.Write(b); err != nil {
					return err
				}
			}
			m.Codes++
		}
		return nil
	})
	if err != nil {
		return err
	}
	log.Printf("codes %d done", m.Codes)

	// blocks.bin
	err = withFile(filepath.Join(out, "blocks.bin"), func(w *bufio.Writer) error {
		var hdr [12]byte
		for i := len(chain) - 1; i >= 0; i-- {
			r := chain[i]
			blk, err := blockRLP(db, r.hash, r.num)
			if err != nil {
				return err
			}
			binary.LittleEndian.PutUint64(hdr[:8], r.num)
			binary.LittleEndian.PutUint32(hdr[8:], uint32(len(blk)))
			if _, err := w.Write(hdr[:]); err != nil {
				return err
			}
			if _, err := w.Write(blk); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	log.Printf("blocks %d..%d done", s.num, *headNum)

	js, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(out, "meta.json"), js, 0o644)
}

// blockRLP splices the stored header RLP and body RLP into one list, which is
// byte for byte what rlp.EncodeToBytes(block) produces under coreth's extras:
// list(header, txs, uncles, version, extData).
func blockRLP(db ethdb.Database, hash common.Hash, num uint64) ([]byte, error) {
	header := rawdb.ReadHeaderRLP(db, hash, num)
	body := rawdb.ReadBodyRLP(db, hash, num)
	if len(header) == 0 || len(body) == 0 {
		return nil, fmt.Errorf("missing header or body for block %d %s", num, hash)
	}
	items, _, err := rlp.SplitList(body)
	if err != nil {
		return nil, fmt.Errorf("body %d: %w", num, err)
	}
	w := rlp.NewEncoderBuffer(nil)
	l := w.List()
	w.Write(header)
	w.Write(items)
	w.ListEnd(l)
	return w.ToBytes(), nil
}

func withFile(path string, fn func(*bufio.Writer) error) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	if err := fn(w); err != nil {
		f.Close()
		return err
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
