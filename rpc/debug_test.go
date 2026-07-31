package rpc

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/state"
)

// corpusDir points at the fixed mainnet corpus (EPOCHDB_CORPUS overrides);
// tracer-plumbing tests skip when it is absent (they need real txs: a
// transfer, a logging call, a creation).
var corpusDir = func() string {
	if d := os.Getenv("EPOCHDB_CORPUS"); d != "" {
		return d
	}
	return "../data"
}()

type corpusEnv struct {
	srv    *Server
	blocks BlockSource
}

func openCorpus(t *testing.T) *corpusEnv {
	t.Helper()
	if _, err := os.Stat(corpusDir + "/exechead"); err != nil {
		t.Skip("fixed corpus not present")
	}
	gm, err := exec.ChainGenesis(mustCChain(t, 1))
	if err != nil {
		t.Fatal(err)
	}
	store, err := state.OpenReadOnly(corpusDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	hist, err := state.OpenHistory(corpusDir, store, gm.Alloc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hist.Close() })
	reader, err := fetch.OpenReader(corpusDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reader.Close() })
	srv := NewServer(hist, HistoryChainContext(hist), gm.Config)
	idx, err := state.OpenTxIndex(corpusDir)
	if err != nil {
		t.Fatal(err)
	}
	srv.EnableTxAPIs(idx, readerBlocks{reader}, exec.ParseEthBlock)
	return &corpusEnv{srv: srv, blocks: readerBlocks{reader}}
}

type readerBlocks struct{ r *fetch.Reader }

func (b readerBlocks) GetByHeight(n uint64) ([]byte, bool, error) { return b.r.GetByHeight(n) }

// findSampleTxs scans forward from a tx-dense region for one plain
// transfer, one logging contract call, and one contract creation.
func findSampleTxs(t *testing.T, env *corpusEnv) (transfer, logging, creation common.Hash) {
	t.Helper()
	var haveT, haveL, haveC bool
	for n := uint64(100_000); n < 1_000_000 && !(haveT && haveL && haveC); n += 1 {
		raw, ok, err := env.blocks.GetByHeight(n)
		if err != nil || !ok {
			t.Fatalf("container %d: ok=%v err=%v", n, ok, err)
		}
		blk, err := exec.ParseEthBlock(raw)
		if err != nil {
			t.Fatal(err)
		}
		if len(blk.Transactions()) == 0 {
			continue
		}
		var receipts []map[string]any
		for i, tx := range blk.Transactions() {
			switch {
			case !haveC && tx.To() == nil:
				creation, haveC = tx.Hash(), true
			case !haveT && tx.To() != nil && len(tx.Data()) == 0:
				transfer, haveT = tx.Hash(), true
			case !haveL && tx.To() != nil && len(tx.Data()) > 0:
				if receipts == nil {
					res, rerr := env.srv.dispatch(&rpcRequest{Method: "eth_getBlockReceipts", Params: mustParams(t, fmt.Sprintf("0x%x", n))})
					if rerr != nil {
						t.Fatal(rerr)
					}
					b, _ := json.Marshal(res)
					json.Unmarshal(b, &receipts)
				}
				if logs, _ := receipts[i]["logs"].([]any); len(logs) > 0 {
					logging, haveL = tx.Hash(), true
				}
			}
		}
	}
	if !(haveT && haveL && haveC) {
		t.Fatalf("samples not found: transfer=%v logging=%v creation=%v", haveT, haveL, haveC)
	}
	return
}

func mustParams(t *testing.T, params ...any) []json.RawMessage {
	t.Helper()
	out := make([]json.RawMessage, len(params))
	for i, p := range params {
		b, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		out[i] = b
	}
	return out
}

func TestDebugTracersOnCorpus(t *testing.T) {
	env := openCorpus(t)
	transfer, logging, creation := findSampleTxs(t, env)
	t.Logf("samples: transfer=%s logging=%s creation=%s", transfer, logging, creation)

	receiptGas := func(h common.Hash) uint64 {
		res, rerr := env.srv.dispatch(&rpcRequest{Method: "eth_getTransactionReceipt", Params: mustParams(t, h)})
		if rerr != nil {
			t.Fatal(rerr)
		}
		b, _ := json.Marshal(res)
		var r struct {
			GasUsed string `json:"gasUsed"`
			Logs    []any  `json:"logs"`
		}
		json.Unmarshal(b, &r)
		var g uint64
		fmt.Sscanf(r.GasUsed, "0x%x", &g)
		return g
	}

	for _, tc := range []struct {
		name string
		hash common.Hash
	}{{"transfer", transfer}, {"logging", logging}, {"creation", creation}} {
		// Default struct logger: gas must equal the receipt's gasUsed.
		res, rerr := env.srv.dispatch(&rpcRequest{Method: "debug_traceTransaction", Params: mustParams(t, tc.hash)})
		if rerr != nil {
			t.Fatalf("%s: structlog: %v", tc.name, rerr)
		}
		var sl struct {
			Gas    uint64 `json:"gas"`
			Failed bool   `json:"failed"`
		}
		if err := json.Unmarshal(res.(json.RawMessage), &sl); err != nil {
			t.Fatalf("%s: decode: %v", tc.name, err)
		}
		if want := receiptGas(tc.hash); sl.Gas != want || sl.Failed {
			t.Fatalf("%s: structlog gas=%d failed=%v, receipt gasUsed=%d", tc.name, sl.Gas, sl.Failed, want)
		}

		// callTracer: top frame gasUsed must also match the receipt.
		res, rerr = env.srv.dispatch(&rpcRequest{Method: "debug_traceTransaction",
			Params: mustParams(t, tc.hash, map[string]string{"tracer": "callTracer"})})
		if rerr != nil {
			t.Fatalf("%s: callTracer: %v", tc.name, rerr)
		}
		var frame struct {
			Type    string `json:"type"`
			GasUsed string `json:"gasUsed"`
		}
		if err := json.Unmarshal(res.(json.RawMessage), &frame); err != nil {
			t.Fatalf("%s: decode call frame: %v", tc.name, err)
		}
		var fg uint64
		fmt.Sscanf(frame.GasUsed, "0x%x", &fg)
		if fg != receiptGas(tc.hash) {
			t.Fatalf("%s: callTracer gasUsed=%d receipt=%d", tc.name, fg, receiptGas(tc.hash))
		}
		if tc.name == "creation" && frame.Type != "CREATE" {
			t.Fatalf("creation frame type %q", frame.Type)
		}
		if tc.name != "creation" && frame.Type != "CALL" {
			t.Fatalf("%s frame type %q", tc.name, frame.Type)
		}

		// prestateTracer must return a non-empty object.
		res, rerr = env.srv.dispatch(&rpcRequest{Method: "debug_traceTransaction",
			Params: mustParams(t, tc.hash, map[string]string{"tracer": "prestateTracer"})})
		if rerr != nil {
			t.Fatalf("%s: prestateTracer: %v", tc.name, rerr)
		}
		var pre map[string]any
		if err := json.Unmarshal(res.(json.RawMessage), &pre); err != nil || len(pre) == 0 {
			t.Fatalf("%s: prestate empty or bad: %v", tc.name, err)
		}
	}
}

func TestCreateAccessListOnCorpus(t *testing.T) {
	env := openCorpus(t)
	_, logging, _ := findSampleTxs(t, env)

	// Rebuild the traced tx's call as an eth_createAccessList request.
	res, rerr := env.srv.dispatch(&rpcRequest{Method: "eth_getTransactionByHash", Params: mustParams(t, logging)})
	if rerr != nil {
		t.Fatal(rerr)
	}
	b, _ := json.Marshal(res)
	var tx struct {
		From        string `json:"from"`
		To          string `json:"to"`
		Input       string `json:"input"`
		BlockNumber string `json:"blockNumber"`
	}
	json.Unmarshal(b, &tx)

	// Replay against the PARENT state: the tx has already run inside its
	// own block, so "at blockNumber" state can legitimately revert it.
	var bn uint64
	fmt.Sscanf(tx.BlockNumber, "0x%x", &bn)
	res, rerr = env.srv.dispatch(&rpcRequest{Method: "eth_createAccessList",
		Params: mustParams(t, map[string]string{"from": tx.From, "to": tx.To, "data": tx.Input}, fmt.Sprintf("0x%x", bn-1))})
	if rerr != nil {
		t.Fatalf("createAccessList: %v", rerr)
	}
	out := res.(map[string]any)
	if _, ok := out["accessList"]; !ok {
		t.Fatalf("no accessList in %v", out)
	}
	if _, ok := out["error"]; ok {
		t.Fatalf("call unexpectedly reverted: %v", out)
	}
}

// TestRawGettersRoundTrip: raw RLP must decode back to the same block, and
// raw receipts must decode with the right count (the public archive
// disables the debug namespace, so self-consistency is the verification).
func TestRawGettersRoundTrip(t *testing.T) {
	env := openCorpus(t)
	_, logging, _ := findSampleTxs(t, env)
	blk, _, _, err := env.srv.findTx(logging)
	if err != nil {
		t.Fatal(err)
	}
	hHex := fmt.Sprintf("0x%x", blk.NumberU64())

	res, rerr := env.srv.dispatch(&rpcRequest{Method: "debug_getRawBlock", Params: mustParams(t, hHex)})
	if rerr != nil {
		t.Fatal(rerr)
	}
	reparsed, err := exec.ParseEthBlock([]byte(res.(hexutil.Bytes)))
	if err != nil {
		t.Fatalf("raw block reparse: %v", err)
	}
	if reparsed.Hash() != blk.Hash() {
		t.Fatalf("raw block round-trip hash %s != %s", reparsed.Hash(), blk.Hash())
	}

	res, rerr = env.srv.dispatch(&rpcRequest{Method: "debug_getRawHeader", Params: mustParams(t, hHex)})
	if rerr != nil {
		t.Fatal(rerr)
	}
	var hdr types.Header
	if err := rlp.DecodeBytes(res.(hexutil.Bytes), &hdr); err != nil || hdr.Hash() != blk.Hash() {
		t.Fatalf("raw header round-trip: err=%v hash=%s want %s", err, hdr.Hash(), blk.Hash())
	}

	res, rerr = env.srv.dispatch(&rpcRequest{Method: "debug_getRawReceipts", Params: mustParams(t, hHex)})
	if rerr != nil {
		t.Fatal(rerr)
	}
	raws := res.([]hexutil.Bytes)
	if len(raws) != len(blk.Transactions()) {
		t.Fatalf("raw receipts: %d for %d txs", len(raws), len(blk.Transactions()))
	}
	var rec types.Receipt
	if err := rec.UnmarshalBinary(raws[0]); err != nil {
		t.Fatalf("raw receipt decode: %v", err)
	}

	res, rerr = env.srv.dispatch(&rpcRequest{Method: "debug_getRawTransaction", Params: mustParams(t, logging)})
	if rerr != nil {
		t.Fatal(rerr)
	}
	var tx types.Transaction
	if err := tx.UnmarshalBinary(res.(hexutil.Bytes)); err != nil || tx.Hash() != logging {
		t.Fatalf("raw tx round-trip: err=%v hash=%s want %s", err, tx.Hash(), logging)
	}
}

// mustCChain is the primary-network C-chain descriptor, what every test here
// runs against.
func mustCChain(t *testing.T, networkID uint32) *chain.Chain {
	t.Helper()
	c, err := chain.CChain(networkID)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
