package rpc

import (
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/trie"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/state"
)

// Wrong-answer gates for the read path: a failure that a client cannot tell
// from an authoritative answer (null, "0x", a zero address, a trace over
// zeroed accounts) is the defect class under test here.

// stubCandidates is a hand-built tx index: a fixed hash -> heights map, or a
// hard failure (what a corrupt txidx bucket produces).
type stubCandidates struct {
	at  map[common.Hash][]uint64
	err error
}

func (s stubCandidates) WalkCandidates(h common.Hash, fn func(uint64) (bool, error)) error {
	if s.err != nil {
		return s.err
	}
	for _, n := range s.at[h] {
		stop, err := fn(n)
		if err != nil || stop {
			return err
		}
	}
	return nil
}

// waEnv is a 3-block chain: headers in a real store (so BLOCKHASH and the
// parent lookup work), containers in a map, no epochs and NO COOK, so every
// state read is above the cooked watermark.
type waEnv struct {
	srv    *Server
	blocks mapBlocks
	hashes map[uint64]common.Hash
}

func newWrongAnswerEnv(t *testing.T, txs map[uint64]types.Transactions) *waEnv {
	t.Helper()
	return newChainEnv(t, mustCChain(t, 1), txs)
}

// newChainEnv is the same three blocks on a CHOSEN chain, with an optional
// hook that fills in header fields the caller cares about (fees_test.go needs
// real base fees and fee state; a wrong-answer test needs neither).
func newChainEnv(t *testing.T, c *chain.Chain, txs map[uint64]types.Transactions, hdr ...func(n uint64, h *types.Header)) *waEnv {
	t.Helper()
	dir := t.TempDir()
	gm, err := exec.ChainGenesis(c)
	if err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	env := &waEnv{blocks: mapBlocks{}, hashes: map[uint64]common.Hash{}}
	var parent common.Hash
	for n := uint64(1); n <= 3; n++ {
		h := &types.Header{
			Number:     new(big.Int).SetUint64(n),
			ParentHash: parent,
			Difficulty: big.NewInt(1),
			GasLimit:   8_000_000,
		}
		for _, fn := range hdr {
			fn(n, h)
		}
		blk := types.NewBlock(h, txs[n], nil, nil, trie.NewStackTrie(nil))
		raw, err := rlp.EncodeToBytes(blk)
		if err != nil {
			t.Fatal(err)
		}
		hdrRLP, err := rlp.EncodeToBytes(blk.Header())
		if err != nil {
			t.Fatal(err)
		}
		if err := store.AppendHeader(n, hdrRLP); err != nil {
			t.Fatal(err)
		}
		env.blocks[n] = raw
		env.hashes[n] = blk.Hash()
		parent = blk.Hash()
	}
	if err := store.FlushAndSetExecHead(3); err != nil {
		t.Fatal(err)
	}
	hist, err := state.OpenHistory(dir, store, gm.TrieAlloc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(hist.Close)
	hist.SetHead(3)
	env.srv = NewServer(hist, HistoryChainContext(hist), gm.Config)
	return env
}

var errBrokenIndex = errors.New("tx index unavailable: bad header")

// signedTxFor is a real, recoverable transaction for block n.
func signedTxFor(t *testing.T, n uint64) *types.Transaction {
	t.Helper()
	gm, err := exec.ChainGenesis(mustCChain(t, 1))
	if err != nil {
		t.Fatal(err)
	}
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	to := common.HexToAddress("0xbbbb000000000000000000000000000000000002")
	signer := types.MakeSigner(gm.Config, new(big.Int).SetUint64(n), 0)
	tx, err := types.SignTx(types.NewTx(&types.LegacyTx{
		Nonce: 0, To: &to, Value: big.NewInt(1), Gas: 21000, GasPrice: big.NewInt(1),
	}), signer, key)
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

// TestTxIndexFailureIsNotAnEmptyChain: an index that cannot answer must make
// the tx methods FAIL. Null and "0x" read as "this transaction was never
// mined", which is the answer an indexer acts on.
func TestTxIndexFailureIsNotAnEmptyChain(t *testing.T) {
	env := newWrongAnswerEnv(t, nil)
	env.srv.EnableTxAPIs(stubCandidates{err: errBrokenIndex}, env.blocks, exec.ParseEthBlock)
	hash := common.HexToHash("0xdeadbeef00000000000000000000000000000000000000000000000000000001")

	for _, m := range []string{"eth_getTransactionByHash", "eth_getTransactionReceipt", "eth_getRawTransactionByHash", "eth_getBlockByHash"} {
		res, rerr := env.srv.dispatch(&rpcRequest{Method: m, Params: mustParams(t, hash)})
		if rerr == nil {
			t.Fatalf("%s answered %v with a broken tx index instead of erroring", m, res)
		}
	}
}

// TestTxByHashRefusesUnreadableContainer: the index says the tx is at height
// 2 and that container cannot be read (a missing epoch, raw retired below a
// gap). "Not found" hides the hole AND skips the receipt path's coverage
// refusal, which only runs once a tx has been found.
func TestTxByHashRefusesUnreadableContainer(t *testing.T) {
	tx := signedTxFor(t, 2)
	env := newWrongAnswerEnv(t, map[uint64]types.Transactions{2: {tx}})
	delete(env.blocks, 2) // the container the index points at is gone
	env.srv.EnableTxAPIs(
		stubCandidates{at: map[common.Hash][]uint64{tx.Hash(): {2}}},
		env.blocks, exec.ParseEthBlock,
	)

	for _, m := range []string{"eth_getTransactionByHash", "eth_getTransactionReceipt"} {
		res, rerr := env.srv.dispatch(&rpcRequest{Method: m, Params: mustParams(t, tx.Hash())})
		if rerr == nil {
			t.Fatalf("%s answered %v for a tx whose container is unreadable", m, res)
		}
		if !strings.Contains(rerr.Message, "not readable") {
			t.Fatalf("%s: %v", m, rerr)
		}
	}
}

// TestUnrecoverableSenderIsNotTheZeroAddress: a container whose signature
// does not recover made `from` (and, for a creation, contractAddress) a
// well-formed but entirely wrong address.
func TestUnrecoverableSenderIsNotTheZeroAddress(t *testing.T) {
	to := common.HexToAddress("0xbbbb000000000000000000000000000000000002")
	bad := types.NewTx(&types.LegacyTx{Nonce: 0, To: &to, Gas: 21000, GasPrice: big.NewInt(1)}) // unsigned
	env := newWrongAnswerEnv(t, map[uint64]types.Transactions{2: {bad}})
	env.srv.EnableTxAPIs(
		// The block hash resolves too, so the by-hash paths reach the same
		// container rather than answering "unknown block".
		stubCandidates{at: map[common.Hash][]uint64{bad.Hash(): {2}, env.hashes[2]: {2}}},
		env.blocks, exec.ParseEthBlock,
	)

	// Every path that marshals this transaction, not only the per-tx ones:
	// the BLOCK paths used to marshal the nil as JSON null, so a client saw a
	// transactions array with a hole in it (or `"from": null`) and could not
	// tell a corrupt container from a protocol quirk.
	for _, c := range []struct {
		method string
		params []any
	}{
		{"eth_getTransactionByHash", []any{bad.Hash()}},
		{"eth_getBlockByNumber", []any{"0x2", true}},
		{"eth_getBlockByHash", []any{env.hashes[2], true}},
		{"eth_getTransactionByBlockNumberAndIndex", []any{"0x2", "0x0"}},
		{"eth_getTransactionByBlockHashAndIndex", []any{env.hashes[2], "0x0"}},
	} {
		res, rerr := env.srv.dispatch(&rpcRequest{Method: c.method, Params: mustParams(t, c.params...)})
		if rerr == nil {
			t.Fatalf("%s marshalled an unrecoverable sender as %+v", c.method, res)
		}
		if !strings.Contains(rerr.Message, "does not recover") {
			t.Fatalf("%s: unexpected error: %v", c.method, rerr)
		}
	}

	// The same block WITHOUT full transactions is still answerable: it names
	// hashes only, so no sender is claimed and nothing is being hidden.
	if _, rerr := env.srv.dispatch(&rpcRequest{Method: "eth_getBlockByNumber", Params: mustParams(t, "0x2", false)}); rerr != nil {
		t.Fatalf("hashes-only block refused: %v", rerr)
	}
	if _, rerr := env.srv.dispatch(&rpcRequest{Method: "eth_getHeaderByNumber", Params: mustParams(t, "0x2")}); rerr != nil {
		t.Fatalf("header refused: %v", rerr)
	}
}

// TestTraceAboveCookedWatermarkRefuses: nothing in this env is cooked, so
// every account read below the head fails. The trace used to run on the
// descent alone and get geth's zeroed accounts back (setError + an empty
// account), producing a nonsense "nonce too high" or a complete, plausible,
// entirely fictional trace. It must be the same cook-lag refusal eth_call
// gives at that height.
func TestTraceAboveCookedWatermarkRefuses(t *testing.T) {
	tx := signedTxFor(t, 2)
	env := newWrongAnswerEnv(t, map[uint64]types.Transactions{2: {tx}})
	env.srv.EnableTxAPIs(
		stubCandidates{at: map[common.Hash][]uint64{tx.Hash(): {2}}},
		env.blocks, exec.ParseEthBlock,
	)

	res, rerr := env.srv.dispatch(&rpcRequest{Method: "debug_traceBlockByNumber", Params: mustParams(t, "0x2")})
	if rerr == nil {
		t.Fatalf("trace over uncooked state returned a result: %v", res)
	}
	if !strings.Contains(rerr.Message, "not indexed yet") {
		t.Fatalf("trace failed with %q, not the cook-lag refusal", rerr.Message)
	}
}
