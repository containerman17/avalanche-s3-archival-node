// epochdb-dump-containers writes the containers of a height window of a
// storage v3 corpus to one flat file, the block source of the Rust node:
//
//	[u64 LE height][u32 LE len][container bytes] ...
//
// heights ascending and contiguous. A container is exactly the bytes a peer
// would serve (store.Reassemble over the blk/hdr/pvm/tx rows). READ-ONLY over
// the corpus, same environment as epochdb-archive-serve: EPOCHDB_S3_* and
// EPOCHDB_CACHE_DIR, and a --dir whose basename is the chain's data dir name
// so the chunk-cache namespace matches.
package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/containerman17/avalanche-s3-archival-node/dist"
	"github.com/containerman17/avalanche-s3-archival-node/store"
)

func main() {
	dir := flag.String("dir", "", "data dir for dist.Open (basename = chunk-cache namespace)")
	manifest := flag.String("manifest", "", "corpus manifest.json")
	version := flag.Uint("version", 3, "storage version the runs were written under")
	from := flag.Uint64("from", 1, "first height (genesis has no container)")
	to := flag.Uint64("to", 0, "last height, inclusive")
	out := flag.String("out", "", "output file")
	flag.Parse()
	if *dir == "" || *manifest == "" || *out == "" || *to < *from {
		log.Fatal("dump-containers: need --dir, --manifest, --out, --from <= --to")
	}

	raw, err := os.ReadFile(*manifest)
	check(err)
	var m store.Manifest
	check(json.Unmarshal(raw, &m))
	cas, err := dist.Open(*dir)
	check(err)
	defer cas.Close()

	f, err := os.Create(*out + ".tmp")
	check(err)
	w := bufio.NewWriterSize(f, 4<<20)
	t0 := time.Now()
	next, n, bytes := *from, uint64(0), uint64(0)
	var hdr [12]byte
	for _, r := range m.Runs {
		if r.ToHeight < *from || r.FromHeight > *to || next > *to {
			continue
		}
		run, err := store.OpenRunVersion(cas, r.Name, uint32(*version))
		check(err)
		lo, hi := max(*from, r.FromHeight), min(*to, r.ToHeight)
		err = scanBlocks(run, lo, hi, func(h uint64, hd, pvm []byte, txs [][]byte) error {
			if h != next {
				return fmt.Errorf("height %d, want %d", h, next)
			}
			c, err := store.Reassemble(pvm, hd, txs)
			if err != nil {
				return err
			}
			binary.LittleEndian.PutUint64(hdr[:8], h)
			binary.LittleEndian.PutUint32(hdr[8:], uint32(len(c)))
			if _, err := w.Write(hdr[:]); err != nil {
				return err
			}
			if _, err := w.Write(c); err != nil {
				return err
			}
			next++
			n++
			bytes += uint64(len(c))
			if n%100_000 == 0 {
				log.Printf("dump-containers: %d blocks, height %d, %d MB, %.0f blk/s", n, h, bytes>>20, float64(n)/time.Since(t0).Seconds())
			}
			return nil
		})
		run.Close()
		if err != nil {
			log.Fatalf("dump-containers: run %s: %v", r.Name[:8], err)
		}
	}
	if next != *to+1 {
		log.Fatalf("dump-containers: stopped at height %d, want through %d", next-1, *to)
	}
	check(w.Flush())
	check(f.Close())
	check(os.Rename(*out+".tmp", *out))
	log.Printf("dump-containers: %s: %d blocks [%d,%d], %d MB, %s", *out, n, *from, *to, bytes>>20, time.Since(t0).Round(time.Second))
}

type row struct {
	n uint64
	v []byte
}

// blkRow is blk/<h> by range scan, not Get: a point read probes the section
// bloom, and an older version's bloom split is not this binary's.
func blkRow(r *store.Run, h uint64) ([]byte, error) {
	var v []byte
	err := r.ScanRange(store.SecChain, store.BlkKey(h), store.BlkKey(h+1), func(_, val []byte) bool {
		v = append([]byte(nil), val...)
		return false
	})
	if err != nil || len(v) != 12 {
		return nil, fmt.Errorf("blk/%d: len=%d err=%v", h, len(v), err)
	}
	return v, nil
}

// scanBlocks streams the blocks [lo, hi] of one run in height order: four
// range scans (blk, hdr, pvm, tx), one goroutine each, zipped here (the same
// walk as epochdb-archive-serve).
func scanBlocks(r *store.Run, lo, hi uint64, fn func(h uint64, hdr, pvm []byte, txs [][]byte) error) error {
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
	defer close(done)
	scan := func(loKey, hiKey []byte) (<-chan row, *error) {
		ch := make(chan row, 256)
		var serr error
		go func() {
			defer close(ch)
			serr = r.ScanRange(store.SecChain, loKey, hiKey, func(k, v []byte) bool {
				select {
				case ch <- row{store.NumOf(k), append([]byte(nil), v...)}:
					return true
				case <-done:
					return false
				}
			})
		}()
		return ch, &serr
	}
	blk, blkErr := scan(store.BlkKey(lo), store.BlkKey(hi+1))
	hdr, hdrErr := scan(store.HdrKey(lo), store.HdrKey(hi+1))
	pvm, pvmErr := scan(store.PvmKey(lo), store.PvmKey(hi+1))
	tx, txErr := scan(store.TxKey(txLo), store.TxKey(txHi))
	next := func(ch <-chan row, want uint64, fam string, errp *error) ([]byte, error) {
		x, ok := <-ch
		if !ok {
			if *errp != nil {
				return nil, *errp
			}
			return nil, fmt.Errorf("%s/%d: missing", fam, want)
		}
		if x.n != want {
			return nil, fmt.Errorf("%s/%d: got %s/%d", fam, want, fam, x.n)
		}
		return x.v, nil
	}
	for h := lo; h <= hi; h++ {
		b, err := next(blk, h, "blk", blkErr)
		if err != nil {
			return err
		}
		hv, err := next(hdr, h, "hdr", hdrErr)
		if err != nil {
			return err
		}
		pv, err := next(pvm, h, "pvm", pvmErr)
		if err != nil {
			return err
		}
		f, n := binary.BigEndian.Uint64(b), binary.BigEndian.Uint32(b[8:])
		txs := make([][]byte, n)
		for i := range txs {
			if txs[i], err = next(tx, f+uint64(i), "tx", txErr); err != nil {
				return err
			}
		}
		if err := fn(h, hv, pv, txs); err != nil {
			return err
		}
	}
	return nil
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
