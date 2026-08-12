package store

import (
	"bytes"
	"math/big"
	"testing"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	proposerblock "github.com/ava-labs/avalanchego/vms/proposervm/block"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"

	sevmcustomtypes "github.com/ava-labs/avalanchego/graft/subnet-evm/plugin/evm/customtypes"
)

// The libevm extras registry is process-global, so this registers ONCE for the
// whole package. subnet-evm's customtypes is the header half of what
// fetch.RegisterExtras(chain.SubnetEVM) installs; the other two registrars
// (core, params) are irrelevant to block encoding and would only drag the VM
// in.
func init() { sevmcustomtypes.Register() }

func testHeader(number int64) *types.Header {
	return &types.Header{
		ParentHash:  common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111"),
		Coinbase:    common.HexToAddress("0x0100000000000000000000000000000000000000"),
		Root:        common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222"),
		Difficulty:  big.NewInt(1),
		Number:      big.NewInt(number),
		GasLimit:    8_000_000,
		GasUsed:     123_456,
		Time:        1_700_000_000,
		Extra:       []byte("epochdb"),
		BaseFee:     big.NewInt(25_000_000_000),
		UncleHash:   types.EmptyUncleHash,
		TxHash:      types.EmptyTxsHash,
		ReceiptHash: types.EmptyReceiptsHash,
	}
}

func testTxs(n int) []*types.Transaction {
	to := common.HexToAddress("0x00000000000000000000000000000000000000ff")
	var out []*types.Transaction
	for i := 0; i < n; i++ {
		if i%2 == 0 {
			out = append(out, types.NewTx(&types.LegacyTx{
				Nonce: uint64(i), To: &to, Value: big.NewInt(int64(i) + 1),
				Gas: 21000, GasPrice: big.NewInt(1_000_000_000), Data: []byte{byte(i)},
				V: big.NewInt(27), R: big.NewInt(0x1234), S: big.NewInt(0x5678),
			}))
			continue
		}
		out = append(out, types.NewTx(&types.DynamicFeeTx{
			ChainID: big.NewInt(43114), Nonce: uint64(i), To: &to, Value: big.NewInt(int64(i)),
			Gas: 42000, GasFeeCap: big.NewInt(2_000_000_000), GasTipCap: big.NewInt(1),
			Data: []byte("hello"),
			V:    big.NewInt(1), R: big.NewInt(0xabcd), S: big.NewInt(0xef01),
		}))
	}
	return out
}

func innerBlockRLP(t *testing.T, nTxs int) []byte {
	t.Helper()
	blk := types.NewBlockWithHeader(testHeader(7)).WithBody(types.Body{Transactions: testTxs(nTxs)})
	raw, err := rlp.EncodeToBytes(blk)
	if err != nil {
		t.Fatalf("encode inner block: %v", err)
	}
	return raw
}

func wrapProposer(t *testing.T, innerRLP []byte) []byte {
	t.Helper()
	sb, err := proposerblock.BuildUnsigned(ids.GenerateTestID(), time.Unix(1_700_000_000, 0), 42, proposerblock.Epoch{}, innerRLP)
	if err != nil {
		t.Fatalf("BuildUnsigned: %v", err)
	}
	return sb.Bytes()
}

// TestContainerRoundTrip is the day-one invariant: whatever epochdb stores for
// a container must reassemble into the exact bytes it was fetched as.
func TestContainerRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		raw     []byte
		wantPvm bool // non-empty pvm row expected
		wantTxs int
	}{
		{name: "pre-proposervm/no txs", raw: innerBlockRLP(t, 0), wantPvm: false, wantTxs: 0},
		{name: "pre-proposervm/three txs", raw: innerBlockRLP(t, 3), wantPvm: false, wantTxs: 3},
		{name: "proposervm unsigned/no txs", raw: wrapProposer(t, innerBlockRLP(t, 0)), wantPvm: true, wantTxs: 0},
		{name: "proposervm unsigned/five txs", raw: wrapProposer(t, innerBlockRLP(t, 5)), wantPvm: true, wantTxs: 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pvm, inner, err := SplitContainer(tc.raw)
			if err != nil {
				t.Fatalf("SplitContainer: %v", err)
			}
			if got := len(pvm) > 0; got != tc.wantPvm {
				t.Fatalf("pvm row non-empty = %v, want %v (%d bytes)", got, tc.wantPvm, len(pvm))
			}
			if n := len(inner.Transactions()); n != tc.wantTxs {
				t.Fatalf("inner has %d txs, want %d", n, tc.wantTxs)
			}

			// Rebuild from the rows epochdb actually stores.
			hdr, err := rlp.EncodeToBytes(inner.Header())
			if err != nil {
				t.Fatalf("encode header: %v", err)
			}
			var txs [][]byte
			for _, tx := range inner.Transactions() {
				enc, err := rlp.EncodeToBytes(tx)
				if err != nil {
					t.Fatalf("encode tx: %v", err)
				}
				txs = append(txs, enc)
			}
			out, err := Reassemble(pvm, hdr, txs)
			if err != nil {
				t.Fatalf("Reassemble: %v", err)
			}
			if !bytes.Equal(out, tc.raw) {
				t.Fatalf("container not byte-identical: got %d bytes, want %d", len(out), len(tc.raw))
			}
		})
	}
}

// TestCorethShapeTemplate covers the reason the pvm row is a TEMPLATE and not
// just the wrapper prefix and suffix: coreth's block body carries Version and
// ExtData AFTER the uncles, so a rebuild from header + txs alone would drop
// them. This process cannot register coreth's extras (subnet-evm's are already
// in, and the registry is process-global and mutually exclusive), so the
// five-field body is hand-built and driven through the same span/template code
// SplitContainer uses.
func TestCorethShapeTemplate(t *testing.T) {
	hdr, err := rlp.EncodeToBytes(testHeader(9))
	if err != nil {
		t.Fatalf("encode header: %v", err)
	}
	var txRLPs [][]byte
	for _, tx := range testTxs(2) {
		enc, err := rlp.EncodeToBytes(tx)
		if err != nil {
			t.Fatalf("encode tx: %v", err)
		}
		txRLPs = append(txRLPs, enc)
	}
	txs := make([]rlp.RawValue, len(txRLPs))
	for i, t := range txRLPs {
		txs[i] = t
	}
	extData := []byte("atomic tx bytes")
	inner, err := rlp.EncodeToBytes(&struct {
		Header  rlp.RawValue
		Txs     []rlp.RawValue
		Uncles  []rlp.RawValue
		Version uint32
		ExtData *[]byte
	}{Header: hdr, Txs: txs, Version: 3, ExtData: &extData})
	if err != nil {
		t.Fatalf("encode coreth-shaped block: %v", err)
	}

	// Fake wrapper bytes around it, standing in for the proposervm prefix and
	// signature suffix.
	raw := append(append([]byte("PREFIX"), inner...), []byte("SIGNATURE")...)
	off := bytes.Index(raw, inner)

	hs, he, ts, te, err := blockSpans(inner)
	if err != nil {
		t.Fatalf("blockSpans: %v", err)
	}
	pvm := pvmTemplate(raw, off+hs, off+he, off+ts, off+te)
	out, err := Reassemble(pvm, hdr, txRLPs)
	if err != nil {
		t.Fatalf("Reassemble: %v", err)
	}
	if !bytes.Equal(out, raw) {
		t.Fatalf("coreth-shaped container not byte-identical: got %d bytes, want %d", len(out), len(raw))
	}
	if !bytes.Contains(pvm, extData) {
		t.Fatal("ExtData did not survive into the pvm template")
	}
}

func TestContainerRejectsGarbage(t *testing.T) {
	if _, _, err := SplitContainer(nil); err == nil {
		t.Fatal("empty container accepted")
	}
	if _, _, err := SplitContainer([]byte{0xff, 0xff, 0xff}); err == nil {
		t.Fatal("garbage container accepted")
	}
}

func TestReassembleRejectsBadPvm(t *testing.T) {
	for _, bad := range [][]byte{
		{1, 2, 3},                            // shorter than the two length fields
		{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0}, // prefix length past the end
		{0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff}, // tx-list header length past the end
	} {
		if _, err := Reassemble(bad, []byte{0x80}, nil); err == nil {
			t.Fatalf("bad pvm row %x accepted", bad)
		}
	}
}

// --- ethdb + misc -----------------------------------------------------------

func TestEthDBCodeAndMisc(t *testing.T) {
	db, dir := testDB(t)
	m, err := OpenMisc(dir)
	if err != nil {
		t.Fatalf("OpenMisc: %v", err)
	}
	kv := EthDB(db, m, nil)

	b := block(1, 1)
	b.Code[string(hash32(5))] = []byte("CODE")
	if err := db.WriteBlock(b); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}

	got, err := kv.Get(append([]byte{'c'}, hash32(5)...))
	if err != nil || string(got) != "CODE" {
		t.Fatalf("code read: %q %v", got, err)
	}

	// Code writes are a no-op: the executor already captured the blob.
	if err := kv.Put(append([]byte{'c'}, hash32(6)...), []byte("OTHER")); err != nil {
		t.Fatalf("code put: %v", err)
	}
	if _, err := kv.Get(append([]byte{'c'}, hash32(6)...)); err == nil {
		t.Fatal("code put was not a no-op")
	}

	// Everything else lands in misc.log and survives a reopen.
	if err := kv.Put([]byte("LastBlock"), []byte("head")); err != nil {
		t.Fatalf("misc put: %v", err)
	}
	if err := m.BindVMKind("subnet-evm"); err != nil {
		t.Fatalf("BindVMKind: %v", err)
	}
	if err := m.SetFrontierFloor(4242); err != nil {
		t.Fatalf("SetFrontierFloor: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("misc close: %v", err)
	}

	m2, err := OpenMisc(dir)
	if err != nil {
		t.Fatalf("reopen misc: %v", err)
	}
	defer m2.Close()
	if v, ok := m2.Get([]byte("LastBlock")); !ok || string(v) != "head" {
		t.Fatalf("misc key lost across reopen: %q %v", v, ok)
	}
	if h, ok := m2.FrontierFloor(); !ok || h != 4242 {
		t.Fatalf("frontier floor = %d %v", h, ok)
	}
	if err := m2.BindVMKind("subnet-evm"); err != nil {
		t.Fatalf("rebinding the same kind: %v", err)
	}
	if err := m2.BindVMKind("coreth"); err == nil {
		t.Fatal("rebinding the other VM kind was accepted")
	}
}

// --- receipt codecs ---------------------------------------------------------

func TestTxReceiptRoundTrip(t *testing.T) {
	addr := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	r := &types.Receipt{
		Status:  types.ReceiptStatusSuccessful,
		GasUsed: 31337,
		Logs: []*types.Log{
			{Address: addr, Topics: []common.Hash{common.HexToHash("0x01"), common.HexToHash("0x02")}, Data: []byte("payload")},
			{Address: addr, Topics: nil, Data: nil},
		},
	}
	status, gas, cum, logs, err := DecodeTxReceipt(EncodeTxReceipt(r, 99_000))
	if err != nil {
		t.Fatalf("DecodeTxReceipt: %v", err)
	}
	if status != 1 || gas != 31337 || cum != 99_000 {
		t.Fatalf("got status=%d gas=%d cum=%d", status, gas, cum)
	}
	if len(logs) != 2 {
		t.Fatalf("got %d logs, want 2", len(logs))
	}
	if logs[0].Address != addr || len(logs[0].Topics) != 2 || string(logs[0].Data) != "payload" {
		t.Fatalf("log 0 mismatch: %+v", logs[0])
	}
	if len(logs[1].Topics) != 0 || len(logs[1].Data) != 0 {
		t.Fatalf("log 1 mismatch: %+v", logs[1])
	}

	// A failed receipt with no logs is the empty-ish case.
	empty := EncodeTxReceipt(&types.Receipt{Status: types.ReceiptStatusFailed, GasUsed: 21000}, 21000)
	status, gas, cum, logs, err = DecodeTxReceipt(empty)
	if err != nil || status != 0 || gas != 21000 || cum != 21000 || len(logs) != 0 {
		t.Fatalf("failed receipt: status=%d gas=%d cum=%d logs=%d err=%v", status, gas, cum, len(logs), err)
	}
}

func TestTxReceiptRejectsTruncated(t *testing.T) {
	full := EncodeTxReceipt(&types.Receipt{
		Status:  1,
		GasUsed: 5,
		Logs:    []*types.Log{{Topics: []common.Hash{{1}}, Data: []byte("abc")}},
	}, 5)
	for i := 1; i < len(full); i++ {
		if _, _, _, _, err := DecodeTxReceipt(full[:i]); err == nil {
			t.Fatalf("truncation at %d accepted", i)
		}
	}
}

func TestStoredLogsRoundTrip(t *testing.T) {
	a1 := common.HexToAddress("0x00000000000000000000000000000000000000a1")
	a2 := common.HexToAddress("0x00000000000000000000000000000000000000a2")
	receipts := types.Receipts{
		{GasUsed: 100, Status: 1, Logs: []*types.Log{{Address: a1, Topics: []common.Hash{{9}}, Data: []byte("x")}}},
		{GasUsed: 200, Status: 0},
		{GasUsed: 300, Status: 1, Logs: []*types.Log{{Address: a2, Data: []byte("yy")}}},
	}

	logs, err := DecodeStoredLogs(EncodeStoredLogs(receipts))
	if err != nil {
		t.Fatalf("DecodeStoredLogs: %v", err)
	}
	if len(logs) != 2 || logs[0].TxIndex != 0 || logs[1].TxIndex != 2 {
		t.Fatalf("logs mismatch: %+v", logs)
	}
	if logs[0].Address != a1 || logs[1].Address != a2 || string(logs[1].Data) != "yy" {
		t.Fatalf("logs mismatch: %+v", logs)
	}

	rc, err := DecodeStoredReceipts(EncodeStoredReceipts(receipts))
	if err != nil {
		t.Fatalf("DecodeStoredReceipts: %v", err)
	}
	want := []StoredRcpt{{100, 100, 1}, {200, 300, 0}, {300, 600, 1}}
	if len(rc) != len(want) {
		t.Fatalf("got %d receipts, want %d", len(rc), len(want))
	}
	for i := range want {
		if rc[i] != want[i] {
			t.Fatalf("receipt %d = %+v, want %+v", i, rc[i], want[i])
		}
	}

	// A block with no logs stores nothing.
	if EncodeStoredLogs(types.Receipts{{GasUsed: 1, Status: 1}}) != nil {
		t.Fatal("log-free block encoded a non-nil record")
	}
}

func TestTailRcptFraming(t *testing.T) {
	if EncodeTailRcpt(nil, nil) != nil {
		t.Fatal("empty tail record is not nil")
	}
	logsRec, rcptRec, err := DecodeTailRcpt(EncodeTailRcpt([]byte("logs"), []byte("rcpt")))
	if err != nil {
		t.Fatalf("DecodeTailRcpt: %v", err)
	}
	if string(logsRec) != "logs" || string(rcptRec) != "rcpt" {
		t.Fatalf("got %q / %q", logsRec, rcptRec)
	}
	if _, _, err := DecodeTailRcpt([]byte{0x7f}); err == nil {
		t.Fatal("bad framing accepted")
	}
}
