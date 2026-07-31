package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ava-labs/avalanchego/graft/coreth/consensus"
	corethcore "github.com/ava-labs/avalanchego/graft/coreth/core"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/rpc"
	"github.com/containerman17/epochdb/state"
)

// lockedChainCtx serializes header lookups: the store's headers bucketLog
// mutates LRU state on every read and backfill workers share it.
// ponytail: one mutex; shard it if header reads ever dominate a profile.
type lockedChainCtx struct {
	mu    *sync.Mutex
	inner corethcore.ChainContext
}

func (c lockedChainCtx) Engine() consensus.Engine { return c.inner.Engine() }

func (c lockedChainCtx) GetHeader(h common.Hash, n uint64) *types.Header {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inner.GetHeader(h, n)
}

type bfEnv struct {
	store    *state.Store
	hist     *state.History
	reader   *fetch.Reader
	chainCtx corethcore.ChainContext
	cfg      *corethcore.Genesis
	end      uint64 // last height to derive (logs.start - 1)
}

func openBfEnv(dataDir string, c *chain.Chain) (*bfEnv, func()) {
	g, err := exec.ChainGenesis(c)
	if err != nil {
		log.Fatalf("backfill-logs: genesis: %v", err)
	}
	store, err := state.OpenReadOnly(dataDir)
	if err != nil {
		log.Fatalf("backfill-logs: open state layer: %v", err)
	}
	logsStart, ok := store.LogsStart()
	if !ok || logsStart == 0 {
		log.Fatalf("backfill-logs: no logs.start marker (live capture never ran)")
	}
	hist, err := state.OpenHistory(dataDir, store, g.Alloc)
	if err != nil {
		log.Fatalf("backfill-logs: open history: %v", err)
	}
	if hist.Head() < logsStart-1 {
		log.Fatalf("backfill-logs: overlay head %d below logs.start-1 %d, run cook-index first", hist.Head(), logsStart-1)
	}
	reader, err := fetch.OpenReader(dataDir)
	if err != nil {
		log.Fatalf("backfill-logs: open staging reader: %v", err)
	}
	env := &bfEnv{
		store:    store,
		hist:     hist,
		reader:   reader,
		chainCtx: lockedChainCtx{mu: &sync.Mutex{}, inner: exec.NewChainContext(store)},
		cfg:      g,
		end:      logsStart - 1,
	}
	return env, func() { reader.Close(); hist.Close(); store.Close() }
}

// deriveLogsRecord re-executes block n and returns its capture-format
// record (nil = no logs).
func (e *bfEnv) deriveLogsRecord(n uint64) ([]byte, error) {
	raw, ok, err := e.reader.GetByHeight(n)
	if err != nil || !ok {
		return nil, fmt.Errorf("container %d: ok=%v err=%v", n, ok, err)
	}
	blk, err := exec.ParseEthBlock(raw)
	if err != nil {
		return nil, fmt.Errorf("parse %d: %w", n, err)
	}
	receipts, err := rpc.ReExecuteBlock(e.hist, e.chainCtx, e.cfg.Config, blk)
	if err != nil {
		return nil, fmt.Errorf("re-execute %d: %w", n, err)
	}
	var logs []*types.Log
	for _, r := range receipts {
		logs = append(logs, r.Logs...)
	}
	return exec.EncodeLogsFrame(logs), nil
}

// backfillLogsMain derives capture-format event-log records for every
// block below logs.start by re-execution over the overlay. Read-only on
// everything except the logsbf_* files.
func backfillLogsMain(args []string) {
	fs := flag.NewFlagSet("backfill-logs", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "shared data directory")
	workers := fs.Int("workers", 12, "parallel re-execution workers")
	pprofAddr := fs.String("pprof", "", "pprof listen address (e.g. :6067)")
	network := fs.String("network", "fuji", "network: fuji|mainnet")
	fs.Parse(args)
	if *pprofAddr != "" {
		go func() { log.Println(http.ListenAndServe(*pprofAddr, nil)) }()
	}

	env, cleanup := openBfEnv(*dataDir, mustCChain(*network))
	defer cleanup()
	bf, err := state.OpenLogsBackfill(*dataDir)
	if err != nil {
		log.Fatalf("backfill-logs: %v", err)
	}
	defer bf.Close()

	// Logs come only from regular txs, so only blocks the tx index lists
	// need re-execution; everything else (empty and atomic-only blocks,
	// most of early Fuji) is skipped without touching the container.
	txidx, err := state.OpenTxIndex(*dataDir)
	if err != nil {
		log.Fatalf("backfill-logs: open tx index (run cook-txindex first): %v", err)
	}
	lastBucket := env.end / state.BucketBlocks
	withTxs := make([][]bool, lastBucket+1)
	totalWork := uint64(0)
	for b := uint64(0); b <= lastBucket; b++ {
		withTxs[b] = txidx.BlocksWithTxs(b)
		if bf.BucketDone(b) {
			continue
		}
		for n := max(b*state.BucketBlocks, 1); n <= min((b+1)*state.BucketBlocks-1, env.end); n++ {
			if withTxs[b][n-b*state.BucketBlocks] {
				totalWork++
			}
		}
	}
	log.Printf("backfill-logs: range 1..%d, %d non-empty blocks to derive, %d workers",
		env.end, totalWork, *workers)

	var (
		done  atomic.Uint64
		start = time.Now()
	)
	stopProg := make(chan struct{})
	go func() {
		tick := time.NewTicker(10 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-stopProg:
				return
			case <-tick.C:
				d := done.Load()
				rate := float64(d) / time.Since(start).Seconds()
				var eta time.Duration
				if rate > 0 {
					eta = time.Duration(float64(totalWork-d)/rate) * time.Second
				}
				log.Printf("backfill-logs: %d/%d blocks, %.0f blk/s, eta %s, logsbf=%.1fMB",
					d, totalWork, rate, eta.Round(time.Second), float64(bf.Bytes())/1e6)
			}
		}
	}()

	for b := uint64(0); b <= lastBucket; b++ {
		if bf.BucketDone(b) {
			continue
		}
		lo := max(b*state.BucketBlocks, 1)
		hi := min((b+1)*state.BucketBlocks-1, env.end)

		jobs := make(chan uint64, 256)
		type result struct {
			n   uint64
			rec []byte
			err error
		}
		results := make(chan result, 256)
		var wg sync.WaitGroup
		for w := 0; w < *workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for n := range jobs {
					rec, err := env.deriveLogsRecord(n)
					results <- result{n: n, rec: rec, err: err}
				}
			}()
		}
		go func() {
			for n := lo; n <= hi; n++ {
				if withTxs[b][n-b*state.BucketBlocks] {
					jobs <- n
				}
			}
			close(jobs)
			wg.Wait()
			close(results)
		}()
		for res := range results {
			if res.err != nil {
				log.Fatalf("backfill-logs: %v", res.err)
			}
			if res.rec != nil {
				if err := bf.Append(res.n, res.rec); err != nil {
					log.Fatalf("backfill-logs: append %d: %v", res.n, err)
				}
			}
			done.Add(1)
		}
		if err := bf.MarkBucketDone(b); err != nil {
			log.Fatalf("backfill-logs: mark bucket %05d: %v", b, err)
		}
		log.Printf("backfill-logs: bucket %05d done (%d..%d)", b, lo, hi)
	}
	close(stopProg)
	log.Printf("backfill-logs: complete, %d blocks derived in %s, logsbf=%.1fMB",
		done.Load(), time.Since(start).Round(time.Second), float64(bf.Bytes())/1e6)
}

// verifyLogsMain gates the backfill: derived records vs the archive RPC
// (eth_getLogs at single blocks) over the backfilled range, plus byte
// parity against the LIVE capture by re-deriving blocks above logs.start.
func verifyLogsMain(args []string) {
	fs := flag.NewFlagSet("verify-logs", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "shared data directory")
	remote := fs.String("remote", "", "reference archive RPC (default per --network)")
	n := fs.Int("n", 300, "remote-compared blocks in the backfilled range")
	parityN := fs.Int("parity", 50, "live-capture parity blocks above logs.start")
	seed := fs.Int64("seed", time.Now().UnixNano(), "RNG seed")
	network := fs.String("network", "fuji", "network: fuji|mainnet")
	fs.Parse(args)
	if *remote == "" {
		_, _, *remote = netParams(*network)
	}

	env, cleanup := openBfEnv(*dataDir, mustCChain(*network))
	defer cleanup()
	// Read-only: safe while a backfill writer is running.
	bf, err := state.OpenLogsBackfillRO(*dataDir)
	if err != nil {
		log.Fatalf("verify-logs: %v", err)
	}
	defer bf.Close()
	rng := rand.New(rand.NewSource(*seed))
	log.Printf("verify-logs: end=%d seed=%d remote=%s", env.end, *seed, *remote)

	// GATE A: derived records vs remote eth_getLogs, log-bearing blocks.
	bad := 0
	for i := 0; i < *n; {
		h := 1 + uint64(rng.Int63n(int64(env.end)))
		mine, ok, err := bf.Get(h)
		if err != nil {
			log.Fatalf("verify-logs: %v", err)
		}
		if !ok {
			continue // no logs in this block; sample log-bearing ones
		}
		var remoteLogs []struct {
			Address common.Address `json:"address"`
			Topics  []common.Hash  `json:"topics"`
		}
		res, rerr := rpcCall(*remote, "eth_getLogs",
			[]any{map[string]string{"fromBlock": hexutil.EncodeUint64(h), "toBlock": hexutil.EncodeUint64(h)}}, 6)
		if rerr != nil {
			log.Fatalf("verify-logs: remote getLogs %d: %v", h, rerr)
		}
		if err := json.Unmarshal(res, &remoteLogs); err != nil {
			log.Fatalf("verify-logs: decode remote logs %d: %v", h, err)
		}
		var logs []*types.Log
		for _, l := range remoteLogs {
			logs = append(logs, &types.Log{Address: l.Address, Topics: l.Topics})
		}
		want := exec.EncodeLogsFrame(logs)
		if string(want) != string(mine) {
			bad++
			log.Printf("MISMATCH block %d:\n  mine=%x\n  want=%x", h, mine, want)
		}
		i++
		if i%50 == 0 {
			log.Printf("verify-logs: remote gate %d/%d", i, *n)
		}
	}
	fmt.Printf("gate A (remote): %d log-bearing blocks compared, %d mismatches\n", *n, bad)

	// GATE B: byte parity with the live capture above logs.start.
	logsStart := env.end + 1
	head, _ := env.store.ExecHead()
	parityBad, checked := 0, 0
	for checked < *parityN {
		h := logsStart + uint64(rng.Int63n(int64(head-logsStart)))
		live, ok, err := env.store.LogsRecord(h)
		if err != nil {
			log.Fatalf("verify-logs: %v", err)
		}
		if !ok {
			continue // no logs in this block
		}
		rec, err := env.deriveLogsRecord(h)
		if err != nil {
			log.Fatalf("verify-logs: re-derive %d: %v", h, err)
		}
		if string(rec) != string(live) {
			parityBad++
			log.Printf("PARITY MISMATCH block %d:\n  derived=%x\n  live=%x", h, rec, live)
		}
		checked++
	}
	fmt.Printf("gate B (live-capture parity): %d blocks re-derived, %d mismatches\n", checked, parityBad)
	if bad+parityBad > 0 {
		fmt.Println("RESULT: MISMATCHES")
		os.Exit(1)
	}
	fmt.Println("RESULT: ZERO mismatches")
}
