//go:build exp

// pairs: through the read-through cache, print footers of the given runs
// (mode "footers") or spill the transfer-family (holder, emitter) pairs of
// the given runs' rcpt/ rows to <out>/<run>.pairs (mode "pairs").
package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/crypto"
	"github.com/containerman17/avalanche-s3-archival-node/dist"
	"github.com/containerman17/avalanche-s3-archival-node/store"
)

var (
	sigTransfer = crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))
	sigSingle   = crypto.Keccak256Hash([]byte("TransferSingle(address,address,address,uint256,uint256)"))
	sigBatch    = crypto.Keccak256Hash([]byte("TransferBatch(address,address,address,uint256[],uint256[])"))
)

func addrShaped(h common.Hash) bool {
	for _, b := range h[:12] {
		if b != 0 {
			return false
		}
	}
	return true
}

func main() {
	mode, dir, out := os.Args[1], os.Args[2], os.Args[3]
	names := os.Args[4:]
	cas, err := dist.Open(dir)
	if err != nil {
		panic(err)
	}
	if !cas.Remote() {
		panic("no S3 env")
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	var mu sync.Mutex
	for _, n := range names {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			r, err := store.OpenRun(cas, n)
			if err != nil {
				fmt.Fprintln(os.Stderr, n, err)
				return
			}
			defer r.Close()
			f := r.Footer
			if mode == "footers" {
				mu.Lock()
				fmt.Printf("%d %d %d %d %s %d\n", f.FromTx, f.ToTx, f.FromHeight, f.ToHeight, n, f.Len[store.SecChain]+f.Len[store.SecState]+f.Len[store.SecLookup])
				mu.Unlock()
				return
			}
			fh, err := os.Create(filepath.Join(out, n+".pairs"))
			if err != nil {
				panic(err)
			}
			bw := bufio.NewWriterSize(fh, 4<<20)
			var txs, logs, posts uint64
			err = r.ScanRange(store.SecChain, []byte(store.PrefixRcpt), []byte("rcpt0"), func(k, v []byte) bool {
				txs++
				_, _, _, ls, err := store.DecodeTxReceipt(v)
				if err != nil {
					panic(err)
				}
				for _, l := range ls {
					logs++
					if len(l.Topics) == 0 {
						continue
					}
					t0 := l.Topics[0]
					for i := 1; i < len(l.Topics); i++ {
						if !addrShaped(l.Topics[i]) {
							continue
						}
						if !((t0 == sigTransfer && (i == 1 || i == 2)) || ((t0 == sigSingle || t0 == sigBatch) && (i == 2 || i == 3))) {
							continue
						}
						posts++
						bw.WriteString(hex.EncodeToString(l.Topics[i][12:]))
						bw.WriteString(hex.EncodeToString(l.Address[:]))
						bw.WriteByte('\n')
					}
				}
				return true
			})
			if err != nil {
				panic(err)
			}
			bw.Flush()
			fh.Close()
			mu.Lock()
			fmt.Printf("%s tx[%d,%d) txs=%d logs=%d transfer-postings=%d\n", n[:12], f.FromTx, f.ToTx, txs, logs, posts)
			mu.Unlock()
		}(n)
	}
	wg.Wait()
	st, _ := cas.CacheStats()
	fmt.Fprintf(os.Stderr, "cache stats: %+v\n", st)
}
