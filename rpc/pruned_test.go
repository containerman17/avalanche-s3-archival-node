package rpc

import (
	"math/big"
	"strings"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
	"github.com/holiman/uint256"

	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/state"
)

// noBlocks / noTxIndex: a pruned node has no containers and no tx index
// entries below its floor, which is exactly the case under test.
type noBlocks struct{}

func (noBlocks) GetByHeight(uint64) ([]byte, bool, error) { return nil, false, nil }

type noTxIndex struct{}

func (noTxIndex) WalkCandidates(common.Hash, func(uint64) (bool, error)) error { return nil }

// newPrunedServer builds a real limited-history node: a tiny captured corpus
// is folded into a base file at `floor`, and the server runs over a dir that
// holds that base file and nothing else.
func newPrunedServer(t *testing.T, floor uint64) *Server {
	t.Helper()
	gm, err := exec.NetworkGenesis(1)
	if err != nil {
		t.Fatal(err)
	}

	accRLP, err := rlp.EncodeToBytes(&types.StateAccount{
		Nonce: 1, Balance: uint256.NewInt(7),
		Root: types.EmptyRootHash, CodeHash: types.EmptyCodeHash.Bytes(),
	})
	if err != nil {
		t.Fatal(err)
	}
	// One live account row in the snapshot's 53-byte preimage keyspace:
	// kind 'a' | address | 32 zero bytes.
	var row state.BaseRow
	row.Key[0] = 'a'
	copy(row.Key[1:21], common.HexToAddress("0xabcd").Bytes())
	row.Val = accRLP

	m := state.BaseMeta{Block: floor, CumTx: 12345, HdrFrom: floor - 256}
	for n := m.HdrFrom; n <= floor; n++ {
		hdr, err := rlp.EncodeToBytes(&types.Header{
			Number: new(big.Int).SetUint64(n), Difficulty: big.NewInt(1),
		})
		if err != nil {
			t.Fatal(err)
		}
		m.Headers = append(m.Headers, hdr)
	}
	dst := t.TempDir()
	cas, err := dist.Local(dst)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.WriteBase(cas, m, []state.BaseRow{row}); err != nil {
		t.Fatal(err)
	}

	pst, err := state.Open(dst)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pst.Close() })
	hist, err := state.OpenHistory(dst, pst, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(hist.Close)
	if hist.Floor() != floor {
		t.Fatalf("floor=%d want %d", hist.Floor(), floor)
	}
	srv := NewServer(hist, HistoryChainContext(hist), gm.Config)
	srv.EnableTxAPIs(noTxIndex{}, noBlocks{}, exec.ParseEthBlock)
	return srv
}

// TestBelowFloorMethodsNamePruning: every method that can be asked about a
// block or tx below the floor must say "pruned", never fabricate an answer
// and never return an empty one.
func TestBelowFloorMethodsNamePruning(t *testing.T) {
	const floor = 5000
	srv := newPrunedServer(t, floor)
	below := hexUint(floor - 1)
	hash := "0x" + strings.Repeat("11", 32)

	for _, tc := range []struct {
		method string
		params []any
	}{
		// eth_call already checked statedb.Error(); createAccessList did not,
		// and returned a fabricated access list plus a gasUsed over all-zero
		// state.
		{"eth_createAccessList", []any{map[string]any{"to": "0x" + strings.Repeat("22", 20)}, below}},
		{"eth_createAccessList", []any{map[string]any{"from": "0x" + strings.Repeat("22", 20)}, below}}, // creation branch: GetNonce
		{"eth_call", []any{map[string]any{"to": "0x" + strings.Repeat("22", 20)}, below}},
		// All four tx-by-hash methods, not just the two that were wired.
		{"eth_getTransactionByHash", []any{hash}},
		{"eth_getTransactionReceipt", []any{hash}},
		{"eth_getRawTransactionByHash", []any{hash}},
		{"debug_getRawTransaction", []any{hash}},
		// Block by number said "container N missing" with no mention of the floor.
		{"eth_getBlockByNumber", []any{below, false}},
	} {
		res, rerr := srv.dispatch(&rpcRequest{Method: tc.method, Params: mustParams(t, tc.params...)})
		if rerr == nil {
			t.Fatalf("%s below the floor returned %v, want a pruned error", tc.method, res)
		}
		if !strings.Contains(rerr.Message, "pruned") {
			t.Fatalf("%s: %q does not name pruning", tc.method, rerr.Message)
		}
	}
}

func hexUint(n uint64) string {
	return "0x" + big.NewInt(int64(n)).Text(16)
}
