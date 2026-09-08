// Command subnetbench-corpus exports immutable local block rows for replay by
// epochdb-host. It does not open or create a contender's state database.
package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"sync"

	"github.com/ava-labs/libevm/common"
	"github.com/containerman17/avalanche-s3-archival-node/chain"
	"github.com/containerman17/avalanche-s3-archival-node/dist"
	"github.com/containerman17/avalanche-s3-archival-node/fetch"
	"github.com/containerman17/avalanche-s3-archival-node/store"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dir := flag.String("source", "", "directory containing the immutable local source")
	manifestPath := flag.String("manifest", "", "source manifest JSON")
	output := flag.String("output", "", "new corpus output file")
	stop := flag.Uint64("stop", 1_000_000, "last height, inclusive")
	version := flag.Uint("version", 4, "source row format version")
	flag.Parse()
	if *dir == "" || *manifestPath == "" || *output == "" || *stop == 0 {
		return fmt.Errorf("source, manifest, output and positive stop are required")
	}
	data, err := os.ReadFile(*manifestPath)
	if err != nil {
		return err
	}
	var manifest store.Manifest
	if err = json.Unmarshal(data, &manifest); err != nil {
		return err
	}
	sort.Slice(manifest.Runs, func(i, j int) bool { return manifest.Runs[i].FromHeight < manifest.Runs[j].FromHeight })
	cas, err := dist.Local(*dir)
	if err != nil {
		return err
	}
	defer cas.Close()
	f, err := os.OpenFile(*output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	complete := false
	defer func() {
		f.Close()
		if !complete {
			os.Remove(*output)
		}
	}()
	hash := sha256.New()
	w := bufio.NewWriterSize(io.MultiWriter(f, hash), 4<<20)
	if _, err = w.WriteString("EPCORP01"); err != nil {
		return err
	}
	fetch.RegisterExtras(chain.SubnetEVM)
	next := uint64(1)
	var parent, firstParent, lastRoot common.Hash
	var gas, transactions uint64
	for _, ref := range manifest.Runs {
		if ref.ToHeight < next {
			continue
		}
		if ref.FromHeight > next {
			return fmt.Errorf("source gap at height %d", next)
		}
		r, err := store.OpenRunVersion(cas, ref.Name, uint32(*version))
		if err != nil {
			return err
		}
		err = scanBlocks(r, next, min(ref.ToHeight, *stop), func(height uint64, hdr, pvm []byte, txs [][]byte) error {
			raw, err := store.Reassemble(pvm, hdr, txs)
			if err != nil {
				return err
			}
			_, block, err := store.SplitContainer(raw)
			if err != nil {
				return fmt.Errorf("block %d: %w", height, err)
			}
			if block.NumberU64() != height || height != next {
				return fmt.Errorf("nonconsecutive block %d, decoded %d, expected %d", height, block.NumberU64(), next)
			}
			if height == 1 {
				firstParent = block.ParentHash()
			} else if block.ParentHash() != parent {
				return fmt.Errorf("parent mismatch at height %d", height)
			}
			if uint64(len(raw)) > uint64(^uint32(0)) {
				return fmt.Errorf("block %d exceeds corpus length field", height)
			}
			var record [12]byte
			binary.BigEndian.PutUint64(record[:8], height)
			binary.BigEndian.PutUint32(record[8:], uint32(len(raw)))
			if _, err = w.Write(record[:]); err != nil {
				return err
			}
			if _, err = w.Write(raw); err != nil {
				return err
			}
			gas += block.GasUsed()
			transactions += uint64(len(block.Transactions()))
			parent = block.Hash()
			lastRoot = block.Root()
			next++
			if height%100000 == 0 {
				log.Printf("exported %d blocks", height)
			}
			return nil
		})
		r.Close()
		if err != nil {
			return err
		}
		if next > *stop {
			break
		}
	}
	if next != *stop+1 {
		return fmt.Errorf("source ended at %d, expected %d", next-1, *stop)
	}
	if err = w.Flush(); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	metadata := struct {
		Format                       string `json:"format"`
		Blocks, Gas, Transactions    uint64
		Bytes                        int64
		SHA256                       string
		Genesis, LastBlock, LastRoot common.Hash
	}{"EPCORP01", *stop, gas, transactions, stat.Size(), fmt.Sprintf("%x", hash.Sum(nil)), firstParent, parent, lastRoot}
	meta, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(*output+".json", append(meta, '\n'), 0644); err != nil {
		return err
	}
	complete = true
	fmt.Println(string(meta))
	return nil
}

type row struct {
	n uint64
	v []byte
}

func blkRow(r *store.Run, h uint64) ([]byte, error) {
	var value []byte
	err := r.ScanRange(store.SecChain, store.BlkKey(h), store.BlkKey(h+1), func(k, v []byte) bool { value = bytes.Clone(v); return false })
	if err != nil {
		return nil, err
	}
	if len(value) != 12 {
		return nil, fmt.Errorf("invalid block row at %d", h)
	}
	return value, nil
}

// Four ordered scans retain only their small channel buffers. This is the
// same row reconstruction used by the existing block source server.
func scanBlocks(r *store.Run, lo, hi uint64, fn func(uint64, []byte, []byte, [][]byte) error) error {
	first, err := blkRow(r, lo)
	if err != nil {
		return err
	}
	last, err := blkRow(r, hi)
	if err != nil {
		return err
	}
	txLo := binary.BigEndian.Uint64(first)
	txHi := binary.BigEndian.Uint64(last) + uint64(binary.BigEndian.Uint32(last[8:]))
	done := make(chan struct{})
	var wg sync.WaitGroup
	defer func() { close(done); wg.Wait() }()
	type stream struct {
		rows chan row
		err  error
	}
	scan := func(a, b []byte) *stream {
		s := &stream{rows: make(chan row, 256)}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(s.rows)
			s.err = r.ScanRange(store.SecChain, a, b, func(k, v []byte) bool {
				select {
				case s.rows <- row{store.NumOf(k), bytes.Clone(v)}:
					return true
				case <-done:
					return false
				}
			})
		}()
		return s
	}
	blk := scan(store.BlkKey(lo), store.BlkKey(hi+1))
	hdr := scan(store.HdrKey(lo), store.HdrKey(hi+1))
	pvm := scan(store.PvmKey(lo), store.PvmKey(hi+1))
	tx := scan(store.TxKey(txLo), store.TxKey(txHi))
	next := func(s *stream, want uint64) ([]byte, error) {
		x, ok := <-s.rows
		if !ok {
			if s.err != nil {
				return nil, s.err
			}
			return nil, fmt.Errorf("missing source row %d", want)
		}
		if x.n != want {
			return nil, fmt.Errorf("unexpected source row %d, want %d", x.n, want)
		}
		return x.v, nil
	}
	for height := lo; height <= hi; height++ {
		b, err := next(blk, height)
		if err != nil {
			return err
		}
		if len(b) != 12 {
			return fmt.Errorf("bad block row %d", height)
		}
		h, err := next(hdr, height)
		if err != nil {
			return err
		}
		p, err := next(pvm, height)
		if err != nil {
			return err
		}
		from, count := binary.BigEndian.Uint64(b), binary.BigEndian.Uint32(b[8:])
		txs := make([][]byte, count)
		for i := range txs {
			if txs[i], err = next(tx, from+uint64(i)); err != nil {
				return err
			}
		}
		if err = fn(height, h, p, txs); err != nil {
			return err
		}
	}
	for _, s := range []*stream{blk, hdr, pvm, tx} {
		if _, ok := <-s.rows; ok {
			return fmt.Errorf("source scan has extra rows")
		}
		if s.err != nil {
			return s.err
		}
	}
	return nil
}
