// commitroot is the real-chain oracle for package commit: it merges a stored
// chain's latest state out of its runs (store.MergeFrontier over the genesis
// trie state), converts every row to the latest/commit contract form, Rolls
// it, and compares the root with the stored header's stateRoot at the same
// height. It opens the corpus read-only, beside the live writer, and writes
// only the node file named by -out.
//
// -sample runs Roll over a statedump file instead (framing [klen u8][vlen u8]
// [key][value], raw addr+'a' or addr+'s'+slot keys, store row values), for
// throughput on the mainnet key distribution; no store is opened.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"time"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"
	"github.com/holiman/uint256"

	"github.com/containerman17/avalanche-s3-archival-node/chain"
	"github.com/containerman17/avalanche-s3-archival-node/commit"
	"github.com/containerman17/avalanche-s3-archival-node/dist"
	"github.com/containerman17/avalanche-s3-archival-node/exec"
	"github.com/containerman17/avalanche-s3-archival-node/store"
)

type row struct{ k, v []byte }

type rowIter struct {
	r []row
	i int
}

func (it *rowIter) Next() bool    { it.i++; return it.i <= len(it.r) }
func (it *rowIter) Key() []byte   { return it.r[it.i-1].k }
func (it *rowIter) Value() []byte { return it.r[it.i-1].v }
func (it *rowIter) Err() error    { return nil }

// storedAccount is the row exec/capture writes: geth's StateAccount with a
// meaningless storage root, plus whatever libevm extras the writer had.
type storedAccount struct {
	Nonce    uint64
	Balance  *uint256.Int
	Root     common.Hash
	CodeHash []byte
	Rest     []rlp.RawValue `rlp:"tail"`
}

type contractAccount struct {
	Nonce    uint64
	Balance  *uint256.Int
	CodeHash []byte
}

type acctState struct {
	row     []byte
	slots   map[common.Hash][]byte
	deleted bool
}

func main() {
	dir := flag.String("data", "/data", "corpus data dir")
	out := flag.String("out", "/data/tmp/commit/nodes", "node file to write")
	height := flag.Uint64("height", 0, "target height (0 = the last flushed run's end)")
	sample := flag.String("sample", "", "statedump file: Roll it and report throughput, no store")
	flag.Parse()
	os.MkdirAll(filepath.Dir(*out), 0o755)
	if *sample != "" {
		runSample(*sample, *out)
		return
	}

	raw, err := os.ReadFile(filepath.Join(*dir, "chain.json"))
	check(err)
	var cj struct {
		NetworkID    uint32 `json:"networkID"`
		BlockchainID string `json:"blockchainID"`
	}
	check(json.Unmarshal(raw, &cj))
	c, err := chain.Resolve(context.Background(), cj.BlockchainID, cj.NetworkID, *dir)
	check(err)
	g, err := exec.ChainGenesis(c)
	check(err)
	cas, err := dist.Open(*dir)
	check(err)
	defer cas.Close()
	db, err := store.OpenReadOnly(*dir, cas, c.Root())
	check(err)
	defer db.Close()

	target := *height
	if target == 0 {
		target = db.FlushedFloor()
	}
	atTx, ok, err := db.TxNumAtEndOf(target)
	check(err)
	if !ok {
		log.Fatalf("no blk row for height %d", target)
	}
	hdr, ok, err := db.HeaderRLP(target)
	check(err)
	if !ok {
		log.Fatalf("no header for height %d", target)
	}
	want := headerRoot(hdr)
	log.Printf("chain %s height %d txnum %d header root %x", cj.BlockchainID, target, atTx, want)

	// The genesis trie state is the floor the runs' rows land on.
	st := map[common.Hash]*acctState{}
	for addr, ga := range g.TrieAlloc {
		a := &acctState{row: contractRow(ga.Nonce, uint256.MustFromBig(ga.Balance), codeHash(ga.Code))}
		for k, v := range ga.Storage {
			if v != (common.Hash{}) {
				if a.slots == nil {
					a.slots = map[common.Hash][]byte{}
				}
				a.slots[crypto.Keccak256Hash(k[:])] = common.TrimLeftZeroes(bytes.Clone(v[:]))
			}
		}
		st[crypto.Keccak256Hash(addr[:])] = a
	}
	log.Printf("genesis: %d accounts", len(st))

	var (
		nRows, nAcct, nDel, nSlot, nClear, orphan uint64
		lastAddr                                  []byte
		lastHash                                  common.Hash
	)
	t0 := time.Now()
	check(db.MergeFrontier(atTx, func(r store.FrontierRow) error {
		nRows++
		kind, addr, slot, err := store.SplitStateKey(r.Key)
		if err != nil {
			return err
		}
		if !bytes.Equal(addr, lastAddr) {
			lastAddr, lastHash = bytes.Clone(addr), crypto.Keccak256Hash(addr)
		}
		a := st[lastHash]
		if a == nil {
			a = &acctState{}
			st[lastHash] = a
		}
		switch kind {
		case 'a':
			if len(r.Val) == 0 {
				nDel++
				*a = acctState{deleted: true}
				return nil
			}
			var sa storedAccount
			if err := rlp.DecodeBytes(r.Val, &sa); err != nil {
				return fmt.Errorf("account %x: %w", addr, err)
			}
			a.deleted = false
			a.row = contractRow(sa.Nonce, sa.Balance, sa.CodeHash)
			nAcct++
		case 's':
			if a.deleted {
				return nil
			}
			h := crypto.Keccak256Hash(slot)
			if len(r.Val) == 0 {
				nClear++
				delete(a.slots, h)
				return nil
			}
			if a.slots == nil {
				a.slots = map[common.Hash][]byte{}
			}
			a.slots[h] = bytes.Clone(r.Val)
			nSlot++
		}
		return nil
	}))
	readWall := time.Since(t0)
	log.Printf("frontier: %d rows in %s: %d accounts, %d deletes, %d slots, %d clears", nRows, readWall, nAcct, nDel, nSlot, nClear)

	var rows []row
	for h, a := range st {
		if a.deleted {
			continue
		}
		if a.row == nil {
			orphan++
			continue
		}
		rows = append(rows, row{append(bytes.Clone(h[:]), 0), a.row})
		for s, v := range a.slots {
			rows = append(rows, row{append(append(bytes.Clone(h[:]), 1), s[:]...), v})
		}
	}
	t1 := time.Now()
	slices.SortFunc(rows, func(a, b row) int { return bytes.Compare(a.k, b.k) })
	log.Printf("flat state: %d rows (%d accounts with slots but no account row skipped), sorted in %s", len(rows), orphan, time.Since(t1))

	root, s := roll(rows, *out, target)
	fmt.Printf("height %d\nheader root %x\nrolled root %x\n", target, want, root)
	if root == want {
		fmt.Println("EQUAL")
	} else {
		fmt.Println("MISMATCH")
		os.Exit(1)
	}
	_ = s
}

// roll runs commit.Roll and prints its cost.
func roll(rows []row, out string, height uint64) (common.Hash, commit.Stats) {
	var user [32]byte
	user[0], user[1], user[2], user[3] = byte(height>>24), byte(height>>16), byte(height>>8), byte(height)
	var ru0, ru1 syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru0)
	t0 := time.Now()
	root, s, err := commit.Roll(&rowIter{r: rows}, out, user)
	check(err)
	wall := time.Since(t0)
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru1)
	cpu := time.Duration(ru1.Utime.Nano()+ru1.Stime.Nano()) - time.Duration(ru0.Utime.Nano()+ru0.Stime.Nano())
	log.Printf("roll: %d keys %d nodes %d bytes in %s wall %s cpu, peak rss %d MB: %.0f keys/s %.0f keccak/s %.1f MB/s",
		s.Keys, s.Nodes, s.Bytes, wall, cpu, ru1.Maxrss/1024,
		float64(s.Keys)/wall.Seconds(), float64(s.Keys+s.Nodes)/wall.Seconds(), float64(s.Bytes)/wall.Seconds()/1e6)
	return root, s
}

// runSample Rolls a statedump file.
func runSample(path, out string) {
	f, err := os.Open(path)
	check(err)
	defer f.Close()
	br := bufio.NewReaderSize(f, 1<<20)
	var (
		rows                []row
		lastAddr            []byte
		lastHash            []byte
		haveAcct            bool
		nAcct, nSlot, nSkip uint64
		hdr                 [2]byte
	)
	t0 := time.Now()
	for {
		if _, err := io.ReadFull(br, hdr[:]); err == io.EOF {
			break
		} else {
			check(err)
		}
		k := make([]byte, hdr[0])
		v := make([]byte, hdr[1])
		_, err = io.ReadFull(br, k)
		check(err)
		_, err = io.ReadFull(br, v)
		check(err)
		if !bytes.Equal(k[:20], lastAddr) {
			lastAddr, lastHash, haveAcct = k[:20], crypto.Keccak256(k[:20]), false
		}
		if len(v) == 0 {
			nSkip++ // a deletion or a clear: not part of a latest state
			continue
		}
		switch {
		case len(k) == 21 && k[20] == 'a':
			var sa storedAccount
			check(rlp.DecodeBytes(v, &sa))
			rows = append(rows, row{append(bytes.Clone(lastHash), 0), contractRow(sa.Nonce, sa.Balance, sa.CodeHash)})
			haveAcct = true
			nAcct++
		case len(k) == 53 && k[20] == 's':
			if !haveAcct {
				// The dump is one run's rows, so a contract's account row can
				// be missing: give it a placeholder so the slots still hash.
				rows = append(rows, row{append(bytes.Clone(lastHash), 0), contractRow(1, uint256.NewInt(0), types.EmptyCodeHash.Bytes())})
				haveAcct = true
				nAcct++
			}
			rows = append(rows, row{append(append(bytes.Clone(lastHash), 1), crypto.Keccak256(k[21:])...), v})
			nSlot++
		default:
			log.Fatalf("bad key %x", k)
		}
	}
	log.Printf("sample: %d accounts %d slots (%d empty rows skipped) read and hashed in %s", nAcct, nSlot, nSkip, time.Since(t0))
	t1 := time.Now()
	slices.SortFunc(rows, func(a, b row) int { return bytes.Compare(a.k, b.k) })
	log.Printf("sorted in %s", time.Since(t1))
	roll(rows, out, 0)
}

func contractRow(nonce uint64, bal *uint256.Int, codeHash []byte) []byte {
	if bal == nil {
		bal = new(uint256.Int)
	}
	v, err := rlp.EncodeToBytes(&contractAccount{Nonce: nonce, Balance: bal, CodeHash: codeHash})
	check(err)
	return v
}

func codeHash(code []byte) []byte {
	if len(code) == 0 {
		return types.EmptyCodeHash.Bytes()
	}
	return crypto.Keccak256(code)
}

// headerRoot is the 4th field of a header RLP: parentHash, uncleHash,
// coinbase, stateRoot. Read raw so no VM's header extras need registering.
func headerRoot(hdr []byte) common.Hash {
	content, _, err := rlp.SplitList(hdr)
	check(err)
	for i := 0; i < 3; i++ {
		_, content, err = rlp.SplitString(content)
		check(err)
	}
	root, _, err := rlp.SplitString(content)
	check(err)
	return common.BytesToHash(root)
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
