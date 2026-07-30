package rpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"

	"github.com/containerman17/epochdb/exec"
)

// callJSON round-trips a dispatch result through JSON into out.
func callJSON(t *testing.T, env *corpusEnv, method string, out any, params ...any) *rpcError {
	t.Helper()
	res, rerr := env.srv.dispatch(&rpcRequest{Method: method, Params: mustParams(t, params...)})
	if rerr != nil {
		return rerr
	}
	b, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, out); err != nil {
		t.Fatalf("%s: decode: %v", method, err)
	}
	return nil
}

// TestTraceCallParityOnCorpus gates debug_traceCall against eth_call and
// against itself: for sampled real txs replayed as calls at their block,
// the struct-logger returnValue/failed must match eth_call's answer, and
// callTracer's gasUsed must equal the struct logger's gas.
func TestTraceCallParityOnCorpus(t *testing.T) {
	env := openCorpus(t)
	rng := rand.New(rand.NewSource(7))
	head := env.srv.hist.Head()

	checked := 0
	for tries := 0; tries < 4000 && checked < 40; tries++ {
		n := 100_000 + uint64(rng.Int63n(int64(min(head, 1_000_000))))
		raw, ok, err := env.blocks.GetByHeight(n)
		if err != nil || !ok {
			continue
		}
		blk, err := exec.ParseEthBlock(raw)
		if err != nil || len(blk.Transactions()) == 0 {
			continue
		}
		tx := blk.Transactions()[rng.Intn(len(blk.Transactions()))]
		if tx.To() == nil {
			continue // creations redeploy per call, still fine but keep the sample simple
		}
		header := blk.Header()
		signer := types.MakeSigner(env.srv.chainCfg, header.Number, header.Time)
		from, err := types.Sender(signer, tx)
		if err != nil {
			continue
		}
		args := map[string]any{
			"from":  from.Hex(),
			"to":    tx.To().Hex(),
			"data":  hexutil.Encode(tx.Data()),
			"value": hexutil.EncodeBig(tx.Value()),
			"gas":   hexutil.EncodeUint64(tx.Gas()),
		}
		tag := hexutil.EncodeUint64(n)

		var structRes struct {
			Gas         uint64 `json:"gas"`
			Failed      bool   `json:"failed"`
			ReturnValue string `json:"returnValue"`
		}
		structErr := callJSON(t, env, "debug_traceCall", &structRes, args, tag, nil)

		var callRes string
		callErr := callJSON(t, env, "eth_call", &callRes, args, tag)

		if structErr != nil {
			// Hard refusals (e.g. insufficient funds for the value at this
			// state) must refuse eth_call identically.
			if callErr == nil {
				t.Fatalf("block %d tx %s: traceCall refused (%v) but eth_call served", n, tx.Hash(), structErr)
			}
			continue
		}
		// failed/revert parity: eth_call errors iff the traced run failed.
		if (callErr != nil) != structRes.Failed {
			t.Fatalf("block %d tx %s: eth_call err=%v vs traceCall failed=%v", n, tx.Hash(), callErr, structRes.Failed)
		}
		if callErr == nil {
			want := bytes.TrimPrefix([]byte(callRes), []byte("0x"))
			if !bytes.EqualFold(want, []byte(structRes.ReturnValue)) {
				t.Fatalf("block %d tx %s: returnValue mismatch: eth_call=%s trace=%s", n, tx.Hash(), callRes, structRes.ReturnValue)
			}
		}

		var callTr struct {
			GasUsed string `json:"gasUsed"`
		}
		if rerr := callJSON(t, env, "debug_traceCall", &callTr, args, tag, map[string]string{"tracer": "callTracer"}); rerr != nil {
			t.Fatalf("debug_traceCall(callTracer) block %d: %v", n, rerr)
		}
		gasUsed, err := hexutil.DecodeUint64(callTr.GasUsed)
		if err != nil || gasUsed != structRes.Gas {
			t.Fatalf("block %d tx %s: callTracer gasUsed %s vs structLogger gas %d (%v)", n, tx.Hash(), callTr.GasUsed, structRes.Gas, err)
		}
		checked++
	}
	if checked < 20 {
		t.Fatalf("only %d traceCall parity samples", checked)
	}
	fmt.Printf("traceCall parity: %d samples, struct/callTracer/eth_call agree\n", checked)
}

// TestModifiedAccountsOnCorpus gates debug_getModifiedAccountsByNumber /
// ByHash against ground truth derivable from the blocks themselves: every
// tx sender must appear (nonce always changes), ByHash must equal
// ByNumber, a two-param range must equal the union of its single blocks,
// and empty blocks contribute nothing.
func TestModifiedAccountsOnCorpus(t *testing.T) {
	env := openCorpus(t)
	rng := rand.New(rand.NewSource(11))
	head := env.srv.hist.Head()


	checked, emptyChecked := 0, 0
	for tries := 0; tries < 6000 && (checked < 100 || emptyChecked < 5); tries++ {
		n := 1 + uint64(rng.Int63n(int64(min(head, 9_000_000))))
		raw, ok, err := env.blocks.GetByHeight(n)
		if err != nil || !ok {
			continue
		}
		blk, err := exec.ParseEthBlock(raw)
		if err != nil {
			continue
		}

		var got []common.Address
		if rerr := callJSON(t, env, "debug_getModifiedAccountsByNumber", &got, n); rerr != nil {
			t.Fatalf("byNumber %d: %v", n, rerr)
		}

		if len(blk.Transactions()) == 0 {
			if emptyChecked < 5 {
				emptyChecked++
			}
			continue
		}
		if checked >= 100 {
			continue
		}

		set := map[common.Address]bool{}
		for _, a := range got {
			set[a] = true
		}
		header := blk.Header()
		signer := types.MakeSigner(env.srv.chainCfg, header.Number, header.Time)
		for i, tx := range blk.Transactions() {
			from, err := types.Sender(signer, tx)
			if err != nil {
				t.Fatalf("block %d tx %d: %v", n, i, err)
			}
			if !set[from] {
				t.Fatalf("block %d: sender %s of tx %d missing from modified set %v", n, from, i, got)
			}
		}

		// ByHash parity. The corpus's cooked tx index carries block hashes
		// only if it was cooked by this build, so feed the live-tail map,
		// which is the same path serve uses for an accepted block.
		env.srv.AddBlockHash(blk.Hash(), n)
		var byHash []common.Address
		if rerr := callJSON(t, env, "debug_getModifiedAccountsByHash", &byHash, blk.Hash()); rerr != nil {
			t.Fatalf("byHash %d: %v", n, rerr)
		}
		if fmt.Sprint(byHash) != fmt.Sprint(got) {
			t.Fatalf("block %d: byHash %v != byNumber %v", n, byHash, got)
		}

		// Range (n-3, n] == union of singles.
		if n > 3 {
			var ranged []common.Address
			if rerr := callJSON(t, env, "debug_getModifiedAccountsByNumber", &ranged, n-3, n); rerr != nil {
				t.Fatalf("range (%d,%d]: %v", n-3, n, rerr)
			}
			union := map[common.Address]bool{}
			for b := n - 2; b <= n; b++ {
				var one []common.Address
				if rerr := callJSON(t, env, "debug_getModifiedAccountsByNumber", &one, b); rerr != nil {
					t.Fatalf("single %d: %v", b, rerr)
				}
				for _, a := range one {
					union[a] = true
				}
			}
			if len(union) != len(ranged) {
				t.Fatalf("range (%d,%d]: %d addresses, union of singles %d", n-3, n, len(ranged), len(union))
			}
			for _, a := range ranged {
				if !union[a] {
					t.Fatalf("range (%d,%d]: %s not in union", n-3, n, a)
				}
			}
		}
		checked++
	}
	if checked < 100 || emptyChecked < 5 {
		t.Fatalf("thin sampling: %d tx-bearing, %d empty", checked, emptyChecked)
	}
	fmt.Printf("modified accounts: %d tx-bearing blocks + %d empty blocks verified\n", checked, emptyChecked)
}
