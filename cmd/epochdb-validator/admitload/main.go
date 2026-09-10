// Command admitload measures one node's transaction admission: N senders
// funded from the ewoq key, 21k-gas transfers signed ahead of each batch
// and posted as JSON-RPC batches by W workers for D, reporting admitted
// tx/s, refusals and the per-batch round trip p50/p99 (the pool's view via
// txpool_status every 5 s). Point it at a node of `e2e --keep`:
//
//	go run ./cmd/epochdb-validator/admitload --rpc http://127.0.0.1:PORT/ext/bc/CHAIN/rpc --keys 1024 --batch 500 --workers 8 --dur 30s
package main

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ava-labs/avalanchego/genesis"
	ethcommon "github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/ethclient"
	"github.com/ava-labs/libevm/params"
)

type rpcResp struct {
	Result json.RawMessage           `json:"result"`
	Error  *struct{ Message string } `json:"error"`
}

func post(url string, reqs []string) ([]rpcResp, error) {
	resp, err := http.Post(url, "application/json", strings.NewReader("["+strings.Join(reqs, ",")+"]"))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := make([]rpcResp, 0, len(reqs))
	if err := json.Unmarshal(raw, &out); err != nil || len(out) != len(reqs) {
		return nil, fmt.Errorf("bad batch response: %.200s", raw)
	}
	return out, nil
}

func main() {
	rpc := flag.String("rpc", "", "the chain's /rpc URL")
	nkeys := flag.Int("keys", 1024, "senders")
	batch := flag.Int("batch", 500, "eth_sendRawTransaction per JSON-RPC batch")
	workers := flag.Int("workers", 8, "concurrent batch posters")
	dur := flag.Duration("dur", 30*time.Second, "how long to post")
	flag.Parse()
	if *rpc == "" {
		flag.Usage()
		os.Exit(2)
	}
	ctx := context.Background()
	ec, err := ethclient.Dial(*rpc)
	if err != nil {
		panic(err)
	}
	chainID, err := ec.ChainID(ctx)
	if err != nil {
		panic(err)
	}
	signer := types.LatestSignerForChainID(chainID)
	sign := func(key *ecdsa.PrivateKey, nonce uint64, to ethcommon.Address, value *big.Int) string {
		tx := types.MustSignNewTx(key, signer, &types.DynamicFeeTx{ChainID: chainID, Nonce: nonce, To: &to, Value: value, Gas: 21000,
			GasFeeCap: big.NewInt(100 * params.GWei), GasTipCap: big.NewInt(params.GWei)})
		raw, _ := tx.MarshalBinary()
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_sendRawTransaction","params":["0x%x"]}`, nonce, raw)
	}

	// Fund: chunks of 200 from the one funder, each chunk mined before the next.
	funder := genesis.EWOQKey.ToECDSA()
	fnonce, err := ec.PendingNonceAt(ctx, crypto.PubkeyToAddress(funder.PublicKey))
	if err != nil {
		panic(err)
	}
	keys := make([]*ecdsa.PrivateKey, *nkeys)
	for i := 0; i < *nkeys; i += 200 {
		var reqs []string
		var last ethcommon.Hash
		for j := i; j < min(i+200, *nkeys); j++ {
			keys[j], _ = crypto.GenerateKey()
			reqs = append(reqs, sign(funder, fnonce, crypto.PubkeyToAddress(keys[j].PublicKey), new(big.Int).Mul(big.NewInt(params.Ether), big.NewInt(100))))
			fnonce++
		}
		resps, err := post(*rpc, reqs)
		if err != nil {
			panic(err)
		}
		for _, r := range resps {
			if r.Error != nil {
				panic("fund: " + r.Error.Message)
			}
			json.Unmarshal(r.Result, &last)
		}
		for {
			if r, _ := ec.TransactionReceipt(ctx, last); r != nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	fmt.Printf("funded %d senders, chain %s\n", *nkeys, chainID)

	var admitted, refused, batches atomic.Int64
	var mu sync.Mutex
	var lat []time.Duration
	to := ethcommon.HexToAddress("0x3000000000000000000000000000000000000003")
	start := time.Now()
	deadline := start.Add(*dur)
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var mine []*ecdsa.PrivateKey
			for i := w; i < *nkeys; i += *workers {
				mine = append(mine, keys[i])
			}
			nonces := make([]uint64, len(mine))
			for k := 0; time.Now().Before(deadline); {
				reqs := make([]string, 0, *batch)
				owners := make([]int, 0, *batch)
				for i := 0; i < *batch; i++ {
					reqs = append(reqs, sign(mine[k], nonces[k], to, big.NewInt(1)))
					owners = append(owners, k)
					nonces[k]++
					k = (k + 1) % len(mine)
				}
				t0 := time.Now()
				resps, err := post(*rpc, reqs)
				d := time.Since(t0)
				batches.Add(1)
				mu.Lock()
				lat = append(lat, d)
				mu.Unlock()
				if err != nil {
					refused.Add(int64(len(reqs)))
					fmt.Println("batch error:", err)
					continue
				}
				bad := map[int]bool{}
				for i, r := range resps {
					if r.Error != nil {
						refused.Add(1)
						bad[owners[i]] = true
						if refused.Load()%5000 == 1 {
							fmt.Println("refused:", r.Error.Message)
						}
					} else {
						admitted.Add(1)
					}
				}
				for k := range bad {
					if pn, err := ec.PendingNonceAt(ctx, crypto.PubkeyToAddress(mine[k].PublicKey)); err == nil {
						nonces[k] = pn
					}
				}
			}
		}(w)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	tick := time.NewTicker(5 * time.Second)
	prev, prevT := int64(0), start
	for running := true; running; {
		select {
		case <-done:
			running = false
		case <-tick.C:
			now := admitted.Load()
			var st struct{ Pending, Queued hexutil.Uint }
			if resps, err := post(*rpc, []string{`{"jsonrpc":"2.0","id":1,"method":"txpool_status","params":[]}`}); err == nil {
				json.Unmarshal(resps[0].Result, &st)
			}
			head, _ := ec.BlockNumber(ctx)
			fmt.Printf("t=%3.0fs admitted=%d (%.0f tx/s last 5s) refused=%d pool=%d/%d head=%d\n", time.Since(start).Seconds(), now,
				float64(now-prev)/time.Since(prevT).Seconds(), refused.Load(), st.Pending, st.Queued, head)
			prev, prevT = now, time.Now()
		}
	}
	el := time.Since(start)
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	q := func(f float64) time.Duration { return lat[min(int(f*float64(len(lat))), len(lat)-1)] }
	fmt.Printf("admitted %d refused %d in %.1fs: %.0f tx/s admitted, %d batches of %d, batch p50=%v p99=%v max=%v\n",
		admitted.Load(), refused.Load(), el.Seconds(), float64(admitted.Load())/el.Seconds(), batches.Load(), *batch, q(0.5), q(0.99), lat[len(lat)-1])
}
