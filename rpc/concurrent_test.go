package rpc_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"

	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/rpc"
	"github.com/containerman17/epochdb/state"
)

type combinedBlocks struct {
	epochs *state.EpochSet
	reader *fetch.Reader
}

func (c combinedBlocks) GetByHeight(n uint64) ([]byte, bool, error) {
	if raw, ok, err := c.epochs.GetByHeight(n); ok || err != nil {
		return raw, ok, err
	}
	return c.reader.GetByHeight(n)
}

// TestConcurrentRequests hammers one Server from 32 goroutines with a mix
// of eth_call, eth_getBlockByNumber, eth_getTransactionReceipt,
// eth_getLogs, and eth_getBalance. Run with -race against a real replayed
// corpus:
//
//	EPOCHDB_TEST_DATA=$PWD/../data EPOCHDB_TEST_NETWORK=mainnet go test -race -run TestConcurrentRequests ./rpc/
func TestConcurrentRequests(t *testing.T) {
	dir := os.Getenv("EPOCHDB_TEST_DATA")
	if dir == "" {
		t.Skip("set EPOCHDB_TEST_DATA to a replayed data dir")
	}
	netID := uint32(0)
	if os.Getenv("EPOCHDB_TEST_NETWORK") == "mainnet" {
		netID = avaconstants.MainnetID
	}
	g, err := exec.NetworkGenesis(netID)
	if err != nil {
		t.Fatal(err)
	}
	store, err := state.OpenReadOnly(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	hist, err := state.OpenHistory(dir, store, g.Alloc)
	if err != nil {
		t.Fatal(err)
	}
	defer hist.Close()
	reader, err := fetch.OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	rawIdx, err := state.OpenTxIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	blocks := combinedBlocks{epochs: hist.Epochs(), reader: reader}

	srv := rpc.NewServer(hist, rpc.HistoryChainContext(hist), g.Config)
	srv.EnableTxAPIs(state.CombinedTxIndex{Raw: rawIdx, Epochs: hist.Epochs()}, blocks, exec.ParseEthBlock)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	head := hist.Head()
	// sample tx hashes + addresses upfront, single-threaded
	rng := rand.New(rand.NewSource(1))
	var (
		txHashes []common.Hash
		addrs    []common.Address
	)
	for tries := 0; tries < 5000 && len(txHashes) < 64; tries++ {
		n := 1 + uint64(rng.Int63n(int64(head)))
		raw, ok, err := blocks.GetByHeight(n)
		if err != nil || !ok {
			continue
		}
		blk, err := exec.ParseEthBlock(raw)
		if err != nil || len(blk.Transactions()) == 0 {
			continue
		}
		tx := blk.Transactions()[rng.Intn(len(blk.Transactions()))]
		txHashes = append(txHashes, tx.Hash())
		if to := tx.To(); to != nil {
			addrs = append(addrs, *to)
		}
	}
	if len(txHashes) < 16 || len(addrs) < 8 {
		t.Fatalf("sampling too thin: %d txs %d addrs", len(txHashes), len(addrs))
	}

	call := func(t *testing.T, method string, params []any, allowRevert bool) json.RawMessage {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
		resp, err := http.Post(ts.URL, "application/json", bytes.NewReader(body))
		if err != nil {
			t.Errorf("%s: %v", method, err)
			return nil
		}
		defer resp.Body.Close()
		var reply struct {
			Result json.RawMessage `json:"result"`
			Error  *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
			t.Errorf("%s: decode: %v", method, err)
			return nil
		}
		if reply.Error != nil {
			// eth_call with empty calldata legitimately reverts on
			// fallback-less contracts; everything else must succeed.
			if !allowRevert {
				t.Errorf("%s: rpc error: %s", method, reply.Error.Message)
			}
			return nil
		}
		return reply.Result
	}

	const goroutines = 32
	const iters = 20
	var wg sync.WaitGroup
	for gr := 0; gr < goroutines; gr++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for i := 0; i < iters; i++ {
				n := 1 + uint64(r.Int63n(int64(head)))
				tag := hexutil.EncodeUint64(n)
				switch i % 5 {
				case 0:
					call(t, "eth_call", []any{map[string]string{
						"to": addrs[r.Intn(len(addrs))].Hex(), "data": "0x"}, tag}, true)
				case 1:
					if res := call(t, "eth_getBlockByNumber", []any{tag, true}, false); res == nil || string(res) == "null" {
						t.Errorf("block %d: null", n)
					}
				case 2:
					h := txHashes[r.Intn(len(txHashes))]
					if res := call(t, "eth_getTransactionReceipt", []any{h.Hex()}, false); res == nil || string(res) == "null" {
						t.Errorf("receipt %s: null", h)
					}
				case 3:
					from := n
					if from+50 > head {
						from = head - 50
					}
					call(t, "eth_getLogs", []any{map[string]string{
						"fromBlock": hexutil.EncodeUint64(from),
						"toBlock":   hexutil.EncodeUint64(from + 50)}}, false)
				default:
					call(t, "eth_getBalance", []any{addrs[r.Intn(len(addrs))].Hex(), tag}, false)
				}
			}
		}(int64(gr) + 100)
	}
	wg.Wait()
	fmt.Printf("concurrent test: %d requests served\n", goroutines*iters)
}
