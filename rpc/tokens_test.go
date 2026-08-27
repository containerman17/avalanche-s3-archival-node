package rpc

import (
	"math/big"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/store"
)

var (
	tokA   = common.HexToAddress("0xaaaa000000000000000000000000000000000001") // ERC-20
	tokB   = common.HexToAddress("0xbbbb000000000000000000000000000000000002") // ERC-721
	tokC   = common.HexToAddress("0xcccc000000000000000000000000000000000003") // ERC-1155
	hold1  = common.HexToAddress("0x1111000000000000000000000000000000000011")
	hold2  = common.HexToAddress("0x2222000000000000000000000000000000000022")
	hold3  = common.HexToAddress("0x3333000000000000000000000000000000000033")
	asHash = func(a common.Address) common.Hash { return common.BytesToHash(a[:]) }
)

// tokenServer: five blocks of one tx each, every tx one token event.
//
//	block 1: A Transfer(1 -> 2)            erc20
//	block 2: B Transfer(1 -> 3, id 7)      erc721
//	block 3: C TransferSingle(op, 2 -> 1)  erc1155
//	block 4: A Transfer(3 -> 1)            erc20
//	block 5: A Transfer(2 -> 3)            erc20 (holder 1 absent)
func tokenServer(t *testing.T) *Server {
	t.Helper()
	g, err := exec.ChainGenesis(nil)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cas, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(dir, cas, [32]byte{7})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	key, _ := crypto.GenerateKey()
	signer := types.MakeSigner(g.Config, big.NewInt(1), testBlockTime)
	header := func(n uint64) []byte {
		raw, err := rlp.EncodeToBytes(&types.Header{
			Number: new(big.Int).SetUint64(n), Time: testBlockTime, GasLimit: 15_000_000,
			BaseFee: big.NewInt(25_000_000_000), Difficulty: big.NewInt(1), Extra: []byte{},
		})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	events := []*types.Log{
		{Address: tokA, Topics: []common.Hash{SigTransfer, asHash(hold1), asHash(hold2)}, Data: make([]byte, 32)},
		{Address: tokB, Topics: []common.Hash{SigTransfer, asHash(hold1), asHash(hold3), common.HexToHash("0x7")}},
		{Address: tokC, Topics: []common.Hash{SigTransferSingle, asHash(tokC), asHash(hold2), asHash(hold1)}, Data: make([]byte, 64)},
		{Address: tokA, Topics: []common.Hash{SigTransfer, asHash(hold3), asHash(hold1)}, Data: make([]byte, 32)},
		{Address: tokA, Topics: []common.Hash{SigTransfer, asHash(hold2), asHash(hold3)}, Data: make([]byte, 32)},
	}
	if err := db.WriteBlock(&store.BlockWrite{Height: 0, HeaderRLP: header(0)}); err != nil {
		t.Fatal(err)
	}
	for i, ev := range events {
		to := ev.Address
		tx, err := types.SignNewTx(key, signer, &types.LegacyTx{Nonce: uint64(i), To: &to, Gas: 50000, GasPrice: big.NewInt(25_000_000_000)})
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := tx.MarshalBinary()
		lw := store.LogWrite{Emitter: ev.Address.Bytes()}
		for _, tp := range ev.Topics {
			lw.Topics = append(lw.Topics, tp.Bytes())
		}
		if err := db.WriteBlock(&store.BlockWrite{Height: uint64(i + 1), HeaderRLP: header(uint64(i + 1)), Txs: []store.TxWrite{{
			Hash: tx.Hash().Bytes(), RLP: raw, Logs: []store.LogWrite{lw},
			Receipt: store.EncodeTxReceipt(&types.Receipt{Status: 1, GasUsed: 30000, Logs: []*types.Log{ev}}, 30000),
		}}}); err != nil {
			t.Fatal(err)
		}
	}
	return NewServer(db, g.TrieAlloc, StoreChainContext(db), g.Config)
}

func TestTokenReads(t *testing.T) {
	s := tokenServer(t)
	blocks := func(p *PagedLogs) (out []uint64) {
		for _, l := range p.Logs {
			out = append(out, l.BlockNumber)
		}
		return out
	}
	// Holder 1's ERC-20 history, newest first, then the same in pages of one.
	p, err := s.TokenTransfersByHolder(hold1, "erc20", 0, 10, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := blocks(p); len(got) != 2 || got[0] != 4 || got[1] != 1 || p.More {
		t.Fatalf("erc20 of holder 1: %v more=%v", got, p.More)
	}
	p, err = s.TokenTransfersByHolder(hold1, "erc20", 0, 1, false)
	if err != nil || !p.More || blocks(p)[0] != 1 {
		t.Fatalf("page 1: %v %v", p, err)
	}
	p, err = s.TokenTransfersByHolder(hold1, "erc20", p.NextCursor, 1, false)
	if err != nil || p.More || blocks(p)[0] != 4 {
		t.Fatalf("page 2: %v %v", p, err)
	}
	// The 721 event carries the same signature and the same holder but four
	// topics: it is not an ERC-20 transfer and an ERC-20 transfer is not it.
	p, _ = s.TokenTransfersByHolder(hold1, "erc721", 0, 10, true)
	if got := blocks(p); len(got) != 1 || got[0] != 2 {
		t.Fatalf("erc721 of holder 1: %v", got)
	}
	p, _ = s.TokenTransfersByHolder(hold1, "erc1155", 0, 10, true)
	if got := blocks(p); len(got) != 1 || got[0] != 3 {
		t.Fatalf("erc1155 of holder 1: %v", got)
	}
	// The operator of the 1155 event is at position 1 and is not a holder.
	if p, _ = s.TokenTransfersByHolder(tokC, "erc1155", 0, 10, true); len(p.Logs) != 0 {
		t.Fatalf("operator counted as holder: %v", blocks(p))
	}
	p, _ = s.TokenTransfersByContract(tokA, "erc20", 0, 10, false)
	if got := blocks(p); len(got) != 3 || got[0] != 1 || got[2] != 5 {
		t.Fatalf("token A: %v", got)
	}
	if _, err := s.TokenTransfersByContract(tokA, "erc777", 0, 10, false); err == nil {
		t.Fatal("unknown standard accepted")
	}
	cs, err := s.TokenContracts(hold1)
	if err != nil {
		t.Fatal(err)
	}
	if len(cs) != 3 || cs[0].Token != tokA || cs[0].Standard != "erc20" || cs[0].Last <= cs[0].First ||
		cs[1].Token != tokB || cs[1].Standard != "erc721" || cs[2].Token != tokC || cs[2].Standard != "erc1155" {
		t.Fatalf("contracts of holder 1: %+v", cs)
	}
	gs, err := s.TopicGroups(asHash(hold1), nil)
	if err != nil || len(gs) != 3 {
		t.Fatalf("groups: %+v %v", gs, err)
	}
	p, _ = s.LogsByTopicValue(asHash(hold1), &SigTransfer, store.Pos2, 0, 10, false)
	if got := blocks(p); len(got) != 1 || got[0] != 4 {
		t.Fatalf("holder 1 as receiver of Transfer: %v", got)
	}
	p, _ = s.LogsByEmitter(tokA, nil, 0, 10, true)
	if got := blocks(p); len(got) != 3 || got[0] != 5 {
		t.Fatalf("emitter A: %v", got)
	}
	// The JSON-RPC face.
	res, rerr := call(t, s, "edb_getTokenTransfersByHolder", map[string]any{"address": hold1, "standard": "erc20", "limit": 1})
	if rerr != nil {
		t.Fatal(rerr)
	}
	if pg := res.(*PagedLogs); len(pg.Logs) != 1 || !pg.More || pg.Logs[0].BlockNumber != 4 {
		t.Fatalf("edb_: %+v", pg)
	}
	if _, rerr := call(t, s, "edb_getTokenContracts"); rerr == nil {
		t.Fatal("no parameter accepted")
	}
	// eth_getLogs by topic position still agrees with the shortcut.
	logs, err := s.GetLogs(0, 5, nil, [][]common.Hash{{SigTransfer}, nil, {asHash(hold1)}})
	if err != nil || len(logs) != 1 || logs[0].BlockNumber != 4 {
		t.Fatalf("getLogs topic2=holder1: %d %v", len(logs), err)
	}
}
