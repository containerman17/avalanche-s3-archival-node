package epochdb_test

// THE ADAPTERS ARE PEERS, AND THIS IS WHAT PROVES IT: the same question asked
// through the library, gRPC, plain HTTP and JSON-RPC must come back with the
// same data, because all four are the same core call plus a different
// encoding. A divergence here is a second read path, which is exactly what
// DESIGN's one-core-query-layer ruling forbids.
//
// The equivalence test and the benchmark need a real corpus:
//
//	EPOCHDB_CORPUS_DIR=$PWD/data-numine3 go test -run TestAdapterEquivalence -v .
//	EPOCHDB_CORPUS_DIR=$PWD/data-numine3 go test -run XXX -bench InProcessCall -v .
//
// The stream and parameter-encoding tests build their own two-block store and
// run everywhere.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/avalanche-s3-archival-node"
	"github.com/containerman17/avalanche-s3-archival-node/chain"
	"github.com/containerman17/avalanche-s3-archival-node/grpcapi"
	"github.com/containerman17/avalanche-s3-archival-node/plainhttp"
	"github.com/containerman17/avalanche-s3-archival-node/rpc"
	"github.com/containerman17/avalanche-s3-archival-node/store"
)

// testChain is the descriptor every node in this binary opens. ONE VM KIND PER
// PROCESS (DESIGN): the libevm extras registration is process-global, so the
// synthetic tests use whatever chain the corpus is, and Fuji's C-chain only
// when there is no corpus.
func testChain(t testing.TB) *chain.Chain {
	t.Helper()
	dir := os.Getenv("EPOCHDB_CORPUS_DIR")
	spec := "C"
	if dir != "" {
		raw, err := os.ReadFile(filepath.Join(dir, "chain.json"))
		if err == nil {
			var cached struct {
				BlockchainID string `json:"blockchainID"`
			}
			if json.Unmarshal(raw, &cached) == nil && cached.BlockchainID != "" {
				spec = cached.BlockchainID
			}
		}
	}
	// Offline both ways: "C" comes out of avalanchego's embedded config, and a
	// blockchainID resolves out of the data dir's chain.json cache.
	c, err := chain.Resolve(context.Background(), spec, constants.FujiID, dir)
	if err != nil {
		t.Fatalf("chain %s: %v", spec, err)
	}
	return c
}

// corpusNode opens a finished corpus read-only: no follower, no executor, no
// lock, so it cohabits with whatever else holds that dir.
func corpusNode(t testing.TB) *epochdb.Node {
	t.Helper()
	dir := os.Getenv("EPOCHDB_CORPUS_DIR")
	if dir == "" {
		t.Skip("set EPOCHDB_CORPUS_DIR to a data dir holding storage v0 runs")
	}
	n, err := epochdb.Open(context.Background(), epochdb.Config{
		DataDir: dir, Chain: testChain(t), ReadOnly: true,
	})
	if err != nil {
		t.Fatalf("open %s: %v", dir, err)
	}
	t.Cleanup(n.Close)
	return n
}

// --- the adapters under test ---------------------------------------------------

// grpcClient starts the gRPC adapter on an ephemeral port and dials it.
func grpcClient(t *testing.T, n *epochdb.Node) grpcapi.EpochDBClient {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	grpcapi.RegisterEpochDBServer(g, grpcapi.New(n))
	go g.Serve(ln)
	t.Cleanup(g.Stop)
	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return grpcapi.NewEpochDBClient(conn)
}

// httpTry calls the plain HTTP adapter and returns its answer or its refusal.
func httpTry(t *testing.T, srv *httptest.Server, method string, args url.Values) (map[string]any, string) {
	t.Helper()
	resp, err := http.Get(srv.URL + "/" + method + "?" + args.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if e, ok := out["error"]; ok {
		return nil, fmt.Sprint(e)
	}
	return out, ""
}

// httpGet is httpTry where a refusal is a test failure.
func httpGet(t *testing.T, srv *httptest.Server, method string, args url.Values) map[string]any {
	t.Helper()
	out, errMsg := httpTry(t, srv, method, args)
	if errMsg != "" {
		t.Fatalf("plain HTTP %s: %s", method, errMsg)
	}
	return out
}

// jsonRPC is jsonRPCTry where a refusal is a test failure.
func jsonRPC(t *testing.T, srv *httptest.Server, method string, params ...any) json.RawMessage {
	t.Helper()
	res, errMsg := jsonRPCTry(t, srv, method, params...)
	if errMsg != "" {
		t.Fatalf("JSON-RPC %s: %s", method, errMsg)
	}
	return res
}

// jsonRPCTry calls the JSON-RPC adapter over its real wire path.
func jsonRPCTry(t *testing.T, srv *httptest.Server, method string, params ...any) (json.RawMessage, string) {
	t.Helper()
	if params == nil {
		params = []any{}
	}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(srv.URL, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Error != nil {
		return nil, out.Error.Message
	}
	return out.Result, ""
}

func TestAdapterEquivalence(t *testing.T) {
	n := corpusNode(t)
	gc := grpcClient(t, n)
	plain := httptest.NewServer(plainhttp.Handler(n))
	defer plain.Close()
	jrpc := httptest.NewServer(n.Core())
	defer jrpc.Close()
	ctx := context.Background()

	// --- the head ------------------------------------------------------------
	head, err := n.Head()
	if err != nil {
		t.Fatal(err)
	}
	gh, err := gc.GetHead(ctx, &grpcapi.HeadRequest{})
	if err != nil {
		t.Fatal(err)
	}
	ph := httpGet(t, plain, "head", nil)
	var jn string
	json.Unmarshal(jsonRPC(t, jrpc, "eth_blockNumber"), &jn)
	if gh.Number != head.Number || uint64(ph["number"].(float64)) != head.Number ||
		jn != fmt.Sprintf("0x%x", head.Number) {
		t.Fatalf("head: library %d, gRPC %d, plain %v, JSON-RPC %s", head.Number, gh.Number, ph["number"], jn)
	}
	if !bytes.Equal(gh.Hash, head.Hash[:]) || ph["hash"].(string) != head.Hash.Hex() {
		t.Fatalf("head hash: library %s, gRPC %x, plain %v", head.Hash, gh.Hash, ph["hash"])
	}

	// --- a block that actually carries transactions ---------------------------
	height, txHash := blockWithTxs(t, n, head.Number)
	blk, err := n.Core().BlockAt(height)
	if err != nil {
		t.Fatal(err)
	}
	gb, err := gc.GetBlock(ctx, &grpcapi.BlockRequest{Number: height, Full: true})
	if err != nil {
		t.Fatal(err)
	}
	var gHeader types.Header
	if err := rlp.DecodeBytes(gb.HeaderRlp, &gHeader); err != nil {
		t.Fatal(err)
	}
	pb := httpGet(t, plain, "getBlock", url.Values{"number": {fmt.Sprint(height)}, "full": {"1"}})
	var jb map[string]any
	json.Unmarshal(jsonRPC(t, jrpc, "eth_getBlockByNumber", fmt.Sprintf("0x%x", height), true), &jb)
	if gHeader.Hash() != blk.Hash() || pb["hash"].(string) != blk.Hash().Hex() || jb["hash"].(string) != blk.Hash().Hex() {
		t.Fatalf("block %d hash: library %s, gRPC %s, plain %v, JSON-RPC %v",
			height, blk.Hash(), gHeader.Hash(), pb["hash"], jb["hash"])
	}
	nTxs := len(blk.Transactions())
	if int(gb.TxCount) != nTxs || len(gb.TxRlp) != nTxs ||
		int(pb["txCount"].(float64)) != nTxs || len(jb["transactions"].([]any)) != nTxs {
		t.Fatalf("block %d tx count: library %d, gRPC %d/%d, plain %v, JSON-RPC %d",
			height, nTxs, gb.TxCount, len(gb.TxRlp), pb["txCount"], len(jb["transactions"].([]any)))
	}
	// The gRPC block is by HASH too, and it must be the same block.
	gbh, err := gc.GetBlock(ctx, &grpcapi.BlockRequest{Hash: blk.Hash().Bytes()})
	if err != nil {
		t.Fatal(err)
	}
	if gbh.Number != height {
		t.Fatalf("getBlock by hash: %d, want %d", gbh.Number, height)
	}

	// --- one transaction, with its receipt ------------------------------------
	_, idx, found, err := n.Core().FindTx(txHash)
	if err != nil || !found {
		t.Fatalf("FindTx %s: found=%v err=%v", txHash, found, err)
	}
	gt, err := gc.GetTransaction(ctx, &grpcapi.TransactionRequest{Hash: txHash.Bytes()})
	if err != nil {
		t.Fatal(err)
	}
	var gtx types.Transaction
	if err := gtx.UnmarshalBinary(gt.TxRlp); err != nil {
		t.Fatal(err)
	}
	pt := httpGet(t, plain, "getTransaction", url.Values{"hash": {txHash.Hex()}})
	var jt map[string]any
	json.Unmarshal(jsonRPC(t, jrpc, "eth_getTransactionByHash", txHash), &jt)
	if gtx.Hash() != txHash || pt["transaction"].(map[string]any)["hash"].(string) != txHash.Hex() ||
		jt["hash"].(string) != txHash.Hex() {
		t.Fatalf("tx %s: gRPC %s, plain %v, JSON-RPC %v", txHash, gtx.Hash(), pt["transaction"], jt["hash"])
	}
	if int(gt.Index) != idx || int(pt["index"].(float64)) != idx {
		t.Fatalf("tx index: library %d, gRPC %d, plain %v", idx, gt.Index, pt["index"])
	}
	var jr map[string]any
	json.Unmarshal(jsonRPC(t, jrpc, "eth_getTransactionReceipt", txHash), &jr)
	if gt.Receipt == nil {
		t.Fatal("gRPC returned no receipt")
	}
	if got, want := fmt.Sprintf("0x%x", gt.Receipt.GasUsed), jr["gasUsed"].(string); got != want {
		t.Fatalf("receipt gasUsed: gRPC %s, JSON-RPC %s", got, want)
	}
	if got := len(gt.Receipt.Logs); got != len(jr["logs"].([]any)) {
		t.Fatalf("receipt logs: gRPC %d, JSON-RPC %d", got, len(jr["logs"].([]any)))
	}

	// --- a call, at the head -------------------------------------------------
	// The target is the block's own recipient with ERC20 calldata, i.e. a
	// DeFi-shaped call and not a synthetic one. It may well revert, and a
	// REFUSAL MUST BE EQUIVALENT TOO: an adapter that answers where the core
	// layer refused is the same bug as one that answers differently.
	target := gtx.To()
	if target == nil {
		target = &common.Address{}
	}
	const totalSupply = "0x18160ddd"
	msg := &rpc.CallMsg{
		To: target, Data: common.FromHex(totalSupply), Value: new(big.Int), GasLimit: 100_000,
		GasPrice: new(big.Int), GasFeeCap: new(big.Int), GasTipCap: new(big.Int),
	}
	libOut, libErr := n.CallAt(msg, head.Number)
	gcall, gerr := gc.Call(ctx, &grpcapi.CallRequest{
		To: target.Bytes(), Data: common.FromHex(totalSupply), Gas: 100_000, Height: head.Number})
	pcall, perr := httpTry(t, plain, "call", url.Values{
		"to": {target.Hex()}, "data": {totalSupply}, "gas": {"100000"}})
	jraw, jerr := jsonRPCTry(t, jrpc, "eth_call",
		map[string]any{"to": target, "data": totalSupply, "gas": "0x186a0"}, "latest")
	switch {
	case libErr != nil:
		if gerr == nil || perr == "" || jerr == "" {
			t.Fatalf("call to %s failed in the library (%v) but succeeded through an adapter: gRPC %v, plain %q, JSON-RPC %q",
				target, libErr, gerr, perr, jerr)
		}
		t.Logf("call to %s is refused by all four surfaces (%v), which is equivalence too", target, libErr)
	default:
		if gerr != nil || perr != "" || jerr != "" {
			t.Fatalf("call to %s succeeded in the library but failed through an adapter: gRPC %v, plain %q, JSON-RPC %q",
				target, gerr, perr, jerr)
		}
		var jcall string
		json.Unmarshal(jraw, &jcall)
		want := "0x" + common.Bytes2Hex(libOut)
		if !bytes.Equal(gcall.Output, libOut) || pcall["output"].(string) != want || jcall != want {
			t.Fatalf("call output: library %s, gRPC %x, plain %v, JSON-RPC %s", want, gcall.Output, pcall["output"], jcall)
		}
	}

	// --- state ---------------------------------------------------------------
	sender := common.Address{}
	if s, err := types.Sender(types.LatestSignerForChainID(n.ChainID()), &gtx); err == nil {
		sender = s
	}
	st, err := n.StateAt(head.Number)
	if err != nil {
		t.Fatal(err)
	}
	libBal := st.GetBalance(sender).ToBig()
	libNonce := st.GetNonce(sender)
	gs, err := gc.GetState(ctx, &grpcapi.StateRequest{Address: sender.Bytes(), Height: head.Number})
	if err != nil {
		t.Fatal(err)
	}
	ps := httpGet(t, plain, "getState", url.Values{"address": {sender.Hex()}})
	var jbal string
	json.Unmarshal(jsonRPC(t, jrpc, "eth_getBalance", sender, "latest"), &jbal)
	if new(big.Int).SetBytes(gs.Balance).Cmp(libBal) != 0 || ps["balance"].(string) != libBal.String() ||
		jbal != "0x"+libBal.Text(16) {
		t.Fatalf("balance of %s: library %s, gRPC %x, plain %v, JSON-RPC %s", sender, libBal, gs.Balance, ps["balance"], jbal)
	}
	if gs.Nonce != libNonce || uint64(ps["nonce"].(float64)) != libNonce {
		t.Fatalf("nonce of %s: library %d, gRPC %d, plain %v", sender, libNonce, gs.Nonce, ps["nonce"])
	}

	// --- the address history, keyset-paged ------------------------------------
	hits, _, err := n.Core().SearchByAddress(sender, 0, 5, true)
	if err != nil {
		t.Fatal(err)
	}
	gsr, err := gc.SearchTransactionsByAddress(ctx, &grpcapi.SearchRequest{Address: sender.Bytes(), Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	psr := httpGet(t, plain, "searchTransactionsByAddress", url.Values{"address": {sender.Hex()}, "limit": {"5"}})
	if len(gsr.Hits) != len(hits) || len(psr["hits"].([]any)) != len(hits) {
		t.Fatalf("history of %s: library %d, gRPC %d, plain %d rows",
			sender, len(hits), len(gsr.Hits), len(psr["hits"].([]any)))
	}
	for i, h := range hits {
		if !bytes.Equal(gsr.Hits[i].Hash, h.Hash.Bytes()) {
			t.Fatalf("history row %d: library %s, gRPC %x", i, h.Hash, gsr.Hits[i].Hash)
		}
	}

	// --- logs ----------------------------------------------------------------
	from := height
	if from > 4 {
		from = height - 4
	}
	libLogs, err := n.Core().GetLogs(from, height, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	glogs, err := gc.GetLogs(ctx, &grpcapi.LogsRequest{FromBlock: from, ToBlock: height})
	if err != nil {
		t.Fatal(err)
	}
	plogs := httpGet(t, plain, "getLogs", url.Values{"fromBlock": {fmt.Sprint(from)}, "toBlock": {fmt.Sprint(height)}})
	var jlogs []any
	json.Unmarshal(jsonRPC(t, jrpc, "eth_getLogs", map[string]any{
		"fromBlock": fmt.Sprintf("0x%x", from), "toBlock": fmt.Sprintf("0x%x", height),
	}), &jlogs)
	if len(glogs.Logs) != len(libLogs) || len(plogs["logs"].([]any)) != len(libLogs) || len(jlogs) != len(libLogs) {
		t.Fatalf("logs %d..%d: library %d, gRPC %d, plain %d, JSON-RPC %d",
			from, height, len(libLogs), len(glogs.Logs), len(plogs["logs"].([]any)), len(jlogs))
	}
	t.Logf("equivalent across library/gRPC/plain HTTP/JSON-RPC at head %d: block %d (%d txs), tx %s, %d logs over %d..%d",
		head.Number, height, nTxs, txHash, len(libLogs), from, height)
}

// TestAdapterEquivalenceFullSurface is the same promise as the test above, for
// the REST of the gRPC surface: every capability gRPC gained beyond the first
// eight methods is asked here through gRPC and through JSON-RPC, and the two
// answers must agree. gRPC is the PRIMARY remote API (DESIGN), so a capability
// JSON-RPC has and gRPC answers differently is a divergence, and one gRPC
// cannot answer at all is a gap this test would not catch: the classification
// of what is covered, added and refused lives in the commit message.
func TestAdapterEquivalenceFullSurface(t *testing.T) {
	n := corpusNode(t)
	gc := grpcClient(t, n)
	jrpc := httptest.NewServer(n.Core())
	defer jrpc.Close()
	ctx := context.Background()

	head, err := n.Head()
	if err != nil {
		t.Fatal(err)
	}
	height, txHash := blockWithTxs(t, n, head.Number)
	tag := fmt.Sprintf("0x%x", height)

	// --- node metadata: chainId, client version, syncing, config, ots level ---
	info, err := gc.GetNodeInfo(ctx, &grpcapi.NodeInfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := "0x"+new(big.Int).SetBytes(info.ChainId).Text(16), jsonString(t, jrpc, "eth_chainId"); got != want {
		t.Fatalf("chainId: gRPC %s, JSON-RPC %s", got, want)
	}
	if got, want := info.ClientVersion, jsonString(t, jrpc, "web3_clientVersion"); got != want {
		t.Fatalf("client version: gRPC %s, JSON-RPC %s", got, want)
	}
	var jsyncing any
	json.Unmarshal(jsonRPC(t, jrpc, "eth_syncing"), &jsyncing)
	if syncing := jsyncing != false; syncing != info.Syncing {
		t.Fatalf("syncing: gRPC %v, JSON-RPC %v", info.Syncing, jsyncing)
	}
	if !sameJSON(t, info.ChainConfigJson, jsonRPC(t, jrpc, "eth_getChainConfig")) {
		t.Fatalf("chain config differs between gRPC and JSON-RPC")
	}
	var jlevel uint32
	json.Unmarshal(jsonRPC(t, jrpc, "ots_getApiLevel"), &jlevel)
	if info.OtsApiLevel != jlevel {
		t.Fatalf("ots api level: gRPC %d, JSON-RPC %d", info.OtsApiLevel, jlevel)
	}
	if len(info.Refusals) == 0 {
		t.Fatal("GetNodeInfo publishes no refusals: a client cannot learn what this node will never answer")
	}

	// --- the block, in every form JSON-RPC serves it -------------------------
	gb, err := gc.GetBlock(ctx, &grpcapi.BlockRequest{
		Number: height, TxHashes: true, Receipts: true, Raw: true})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := hexutil.Encode(gb.BlockRlp), jsonString(t, jrpc, "debug_getRawBlock", tag); got != want {
		t.Fatalf("raw block %d: gRPC %s, JSON-RPC %s", height, got, want)
	}
	if got, want := hexutil.Encode(gb.HeaderRlp), jsonString(t, jrpc, "debug_getRawHeader", tag); got != want {
		t.Fatalf("raw header %d: gRPC %s, JSON-RPC %s", height, got, want)
	}
	var jblock map[string]any
	json.Unmarshal(jsonRPC(t, jrpc, "eth_getBlockByNumber", tag, false), &jblock)
	jhashes := jblock["transactions"].([]any)
	if len(gb.TxHash) != len(jhashes) {
		t.Fatalf("tx hashes of %d: gRPC %d, JSON-RPC %d", height, len(gb.TxHash), len(jhashes))
	}
	for i, h := range gb.TxHash {
		if got := common.BytesToHash(h).Hex(); got != jhashes[i].(string) {
			t.Fatalf("tx hash %d of block %d: gRPC %s, JSON-RPC %s", i, height, got, jhashes[i])
		}
	}
	var jreceipts []map[string]any
	json.Unmarshal(jsonRPC(t, jrpc, "eth_getBlockReceipts", tag), &jreceipts)
	if len(gb.Receipts) != len(jreceipts) {
		t.Fatalf("receipts of %d: gRPC %d, JSON-RPC %d", height, len(gb.Receipts), len(jreceipts))
	}
	for i, r := range gb.Receipts {
		if got, want := fmt.Sprintf("0x%x", r.GasUsed), jreceipts[i]["gasUsed"].(string); got != want {
			t.Fatalf("receipt %d gasUsed: gRPC %s, JSON-RPC %s", i, got, want)
		}
		if got, want := len(r.Logs), len(jreceipts[i]["logs"].([]any)); got != want {
			t.Fatalf("receipt %d logs: gRPC %d, JSON-RPC %d", i, got, want)
		}
	}
	var jrawReceipts []string
	json.Unmarshal(jsonRPC(t, jrpc, "debug_getRawReceipts", tag), &jrawReceipts)
	if len(gb.ReceiptRlp) != len(jrawReceipts) {
		t.Fatalf("raw receipts of %d: gRPC %d, JSON-RPC %d", height, len(gb.ReceiptRlp), len(jrawReceipts))
	}
	for i, raw := range gb.ReceiptRlp {
		if got := hexutil.Encode(raw); got != jrawReceipts[i] {
			t.Fatalf("raw receipt %d: gRPC %s, JSON-RPC %s", i, got, jrawReceipts[i])
		}
	}
	var jdetails map[string]any
	json.Unmarshal(jsonRPC(t, jrpc, "ots_getBlockDetails", tag), &jdetails)
	if got, want := hexBig(gb.TotalFees), jdetails["totalFees"].(string); got != want {
		t.Fatalf("total fees of %d: gRPC %s, JSON-RPC %s", height, got, want)
	}
	var juncles string
	json.Unmarshal(jsonRPC(t, jrpc, "eth_getUncleCountByBlockNumber", tag), &juncles)
	if got := fmt.Sprintf("0x%x", gb.UncleCount); got != juncles {
		t.Fatalf("uncle count: gRPC %s, JSON-RPC %s", got, juncles)
	}

	// --- the transaction, named three ways -----------------------------------
	gt, err := gc.GetTransaction(ctx, &grpcapi.TransactionRequest{Hash: txHash.Bytes()})
	if err != nil {
		t.Fatal(err)
	}
	var tx types.Transaction
	if err := tx.UnmarshalBinary(gt.TxRlp); err != nil {
		t.Fatal(err)
	}
	idx := uint32(gt.Index)
	byIndex, err := gc.GetTransaction(ctx, &grpcapi.TransactionRequest{BlockNumber: height, Index: &idx})
	if err != nil {
		t.Fatal(err)
	}
	var jbyIndex map[string]any
	json.Unmarshal(jsonRPC(t, jrpc, "eth_getTransactionByBlockNumberAndIndex", tag, fmt.Sprintf("0x%x", idx)), &jbyIndex)
	if got, want := common.BytesToHash(byIndex.Hash).Hex(), jbyIndex["hash"].(string); got != want {
		t.Fatalf("tx by block+index: gRPC %s, JSON-RPC %s", got, want)
	}
	sender, err := types.Sender(types.LatestSignerForChainID(n.ChainID()), &tx)
	if err != nil {
		t.Fatal(err)
	}
	nonce := tx.Nonce()
	byNonce, err := gc.GetTransaction(ctx, &grpcapi.TransactionRequest{Sender: sender.Bytes(), Nonce: &nonce})
	if err != nil {
		t.Fatal(err)
	}
	jByNonce := jsonString(t, jrpc, "ots_getTransactionBySenderAndNonce", sender, nonce)
	if got := common.BytesToHash(byNonce.Hash).Hex(); got != jByNonce {
		t.Fatalf("tx by sender+nonce: gRPC %s, JSON-RPC %s", got, jByNonce)
	}
	// The stored frames are Otterscan's internal operations, filtered the same
	// way at read time: CREATE/CREATE2, and a CALL that moved value.
	var jops []map[string]any
	json.Unmarshal(jsonRPC(t, jrpc, "ots_getInternalOperations", txHash), &jops)
	ops := 0
	for _, f := range gt.Frames {
		switch {
		case f.Kind == 0xf0 || f.Kind == 0xf5: // CREATE, CREATE2
			ops++
		case f.Kind == 0xf1 && len(f.Value) > 0: // CALL with value
			ops++
		}
	}
	if ops != len(jops) {
		t.Fatalf("internal operations of %s: gRPC frames yield %d, JSON-RPC %d", txHash, ops, len(jops))
	}

	// --- execution: call detail, access list, gas estimate --------------------
	to, data := benchTarget(t, n, head.Number)
	args := map[string]any{"to": to, "data": hexutil.Encode(data), "gas": "0x100000"}
	req := &grpcapi.CallRequest{To: to.Bytes(), Data: data, Gas: 0x100000, Height: head.Number}

	detailed, err := gc.Call(ctx, &grpcapi.CallRequest{
		To: req.To, Data: req.Data, Gas: req.Gas, Height: req.Height, Detailed: true})
	if err != nil {
		t.Fatal(err)
	}
	var jdetailed map[string]any
	json.Unmarshal(jsonRPC(t, jrpc, "eth_callDetailed", args, tag), &jdetailed)
	if got, want := detailed.GasUsed, uint64(jdetailed["gas"].(float64)); got != want {
		t.Fatalf("callDetailed gas: gRPC %d, JSON-RPC %d", got, want)
	}
	if got, want := hexutil.Encode(detailed.Output), jdetailed["returnData"].(string); got != want {
		t.Fatalf("callDetailed returnData: gRPC %s, JSON-RPC %s", got, want)
	}

	al, err := gc.Call(ctx, &grpcapi.CallRequest{
		To: req.To, Data: req.Data, Gas: req.Gas, Height: req.Height, AccessList: true})
	if err != nil {
		t.Fatal(err)
	}
	var jal map[string]any
	json.Unmarshal(jsonRPC(t, jrpc, "eth_createAccessList", args, tag), &jal)
	if got, want := fmt.Sprintf("0x%x", al.GasUsed), jal["gasUsed"].(string); got != want {
		t.Fatalf("createAccessList gas: gRPC %s, JSON-RPC %s", got, want)
	}
	if got, want := len(al.AccessList), len(jal["accessList"].([]any)); got != want {
		t.Fatalf("createAccessList tuples: gRPC %d, JSON-RPC %d", got, want)
	}

	gest, gerr := gc.EstimateGas(ctx, req)
	jest, jerr := jsonRPCTry(t, jrpc, "eth_estimateGas", args, tag)
	switch {
	case gerr != nil || jerr != "":
		if gerr == nil || jerr == "" {
			t.Fatalf("estimateGas: gRPC %v, JSON-RPC %q (one refused, the other answered)", gerr, jerr)
		}
	default:
		var jgas string
		json.Unmarshal(jest, &jgas)
		if got := fmt.Sprintf("0x%x", gest.Gas); got != jgas {
			t.Fatalf("estimateGas: gRPC %s, JSON-RPC %s", got, jgas)
		}
	}

	// --- tracing: a transaction, a block, a call -----------------------------
	tracerArg := map[string]any{"tracer": "callTracer"}
	gtrace, err := gc.Trace(ctx, &grpcapi.TraceRequest{
		Target: &grpcapi.TraceRequest_TxHash{TxHash: txHash.Bytes()}, Tracer: "callTracer"})
	if err != nil {
		t.Fatal(err)
	}
	if !sameJSON(t, gtrace.ResultJson[0], jsonRPC(t, jrpc, "debug_traceTransaction", txHash, tracerArg)) {
		t.Fatalf("traceTransaction %s differs between gRPC and JSON-RPC", txHash)
	}
	gblockTrace, err := gc.Trace(ctx, &grpcapi.TraceRequest{
		Target: &grpcapi.TraceRequest_BlockNumber{BlockNumber: height}, Tracer: "callTracer"})
	if err != nil {
		t.Fatal(err)
	}
	var jblockTrace []struct {
		TxHash string          `json:"txHash"`
		Result json.RawMessage `json:"result"`
	}
	json.Unmarshal(jsonRPC(t, jrpc, "debug_traceBlockByNumber", tag, tracerArg), &jblockTrace)
	if len(gblockTrace.ResultJson) != len(jblockTrace) {
		t.Fatalf("traceBlock %d: gRPC %d results, JSON-RPC %d", height, len(gblockTrace.ResultJson), len(jblockTrace))
	}
	for i, r := range gblockTrace.ResultJson {
		if !sameJSON(t, r, jblockTrace[i].Result) {
			t.Fatalf("traceBlock %d result %d differs between gRPC and JSON-RPC", height, i)
		}
		if got := common.BytesToHash(gblockTrace.TxHash[i]).Hex(); got != jblockTrace[i].TxHash {
			t.Fatalf("traceBlock %d tx %d: gRPC %s, JSON-RPC %s", height, i, got, jblockTrace[i].TxHash)
		}
	}
	gcallTrace, err := gc.Trace(ctx, &grpcapi.TraceRequest{
		Target: &grpcapi.TraceRequest_Call{Call: req}, Tracer: "callTracer"})
	if err != nil {
		t.Fatal(err)
	}
	if !sameJSON(t, gcallTrace.ResultJson[0], jsonRPC(t, jrpc, "debug_traceCall", args, tag, tracerArg)) {
		t.Fatal("traceCall differs between gRPC and JSON-RPC")
	}

	// --- fees ----------------------------------------------------------------
	percentiles := []float64{20, 80}
	gfh, err := gc.GetFeeHistory(ctx, &grpcapi.FeeHistoryRequest{
		BlockCount: 5, NewestBlock: height, RewardPercentiles: percentiles})
	if err != nil {
		t.Fatal(err)
	}
	var jfh struct {
		OldestBlock   string     `json:"oldestBlock"`
		BaseFeePerGas []string   `json:"baseFeePerGas"`
		GasUsedRatio  []float64  `json:"gasUsedRatio"`
		Reward        [][]string `json:"reward"`
	}
	json.Unmarshal(jsonRPC(t, jrpc, "eth_feeHistory", "0x5", tag, percentiles), &jfh)
	if got := fmt.Sprintf("0x%x", gfh.OldestBlock); got != jfh.OldestBlock {
		t.Fatalf("feeHistory oldestBlock: gRPC %s, JSON-RPC %s", got, jfh.OldestBlock)
	}
	if len(gfh.BaseFeePerGas) != len(jfh.BaseFeePerGas) || len(gfh.Reward) != len(jfh.Reward) {
		t.Fatalf("feeHistory shape: gRPC %d fees / %d rewards, JSON-RPC %d / %d",
			len(gfh.BaseFeePerGas), len(gfh.Reward), len(jfh.BaseFeePerGas), len(jfh.Reward))
	}
	for i, f := range gfh.BaseFeePerGas {
		if got := hexBig(f); got != jfh.BaseFeePerGas[i] {
			t.Fatalf("feeHistory baseFee %d: gRPC %s, JSON-RPC %s", i, got, jfh.BaseFeePerGas[i])
		}
	}
	for i, row := range gfh.Reward {
		for j, v := range row.Values {
			if got := hexBig(v); got != jfh.Reward[i][j] {
				t.Fatalf("feeHistory reward %d/%d: gRPC %s, JSON-RPC %s", i, j, got, jfh.Reward[i][j])
			}
		}
	}

	gp, err := gc.GetGasPrice(ctx, &grpcapi.GasPriceRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := hexBig(gp.GasPrice), jsonString(t, jrpc, "eth_gasPrice"); got != want {
		t.Fatalf("gasPrice: gRPC %s, JSON-RPC %s", got, want)
	}
	if got, want := hexBig(gp.MaxPriorityFeePerGas), jsonString(t, jrpc, "eth_maxPriorityFeePerGas"); got != want {
		t.Fatalf("maxPriorityFeePerGas: gRPC %s, JSON-RPC %s", got, want)
	}
	if got, want := hexBig(gp.BaseFee), jsonString(t, jrpc, "eth_baseFee"); got != want {
		t.Fatalf("baseFee: gRPC %s, JSON-RPC %s", got, want)
	}
	var jopts map[string]map[string]string
	json.Unmarshal(jsonRPC(t, jrpc, "eth_suggestPriceOptions"), &jopts)
	for name, got := range map[string]*grpcapi.PriceOption{
		"slow": gp.Slow, "normal": gp.Normal, "fast": gp.Fast} {
		want, ok := jopts[name]
		if !ok != (got == nil) {
			t.Fatalf("price option %s: gRPC %v, JSON-RPC %v", name, got, want)
		}
		if got == nil {
			continue
		}
		if hexBig(got.MaxPriorityFeePerGas) != want["maxPriorityFeePerGas"] ||
			hexBig(got.MaxFeePerGas) != want["maxFeePerGas"] {
			t.Fatalf("price option %s: gRPC tip %s fee %s, JSON-RPC %v",
				name, hexBig(got.MaxPriorityFeePerGas), hexBig(got.MaxFeePerGas), want)
		}
	}

	// --- the contract creator ------------------------------------------------
	gcreator, err := gc.GetContractCreator(ctx, &grpcapi.ContractCreatorRequest{Address: to.Bytes()})
	if err != nil {
		t.Fatal(err)
	}
	var jcreator map[string]string
	json.Unmarshal(jsonRPC(t, jrpc, "ots_getContractCreator", to), &jcreator)
	if gcreator.Found != (jcreator != nil) {
		t.Fatalf("contract creator of %s: gRPC found=%v, JSON-RPC %v", to, gcreator.Found, jcreator)
	}
	if gcreator.Found {
		if got := common.BytesToHash(gcreator.TxHash).Hex(); got != jcreator["hash"] {
			t.Fatalf("contract creator tx: gRPC %s, JSON-RPC %s", got, jcreator["hash"])
		}
		// The JSON encoding of an address is lower-case hex, the Go one is
		// checksummed, so the comparison is on the address itself.
		if got := common.BytesToAddress(gcreator.Creator); got != common.HexToAddress(jcreator["creator"]) {
			t.Fatalf("contract creator: gRPC %s, JSON-RPC %s", got, jcreator["creator"])
		}
	}

	// --- the streams, over a bounded historical range ------------------------
	// A BOUNDED STREAM MUST END BY ITSELF: that is what replaces uninstalling a
	// filter, so a consumer that reads to EOF has the whole range and knows it.
	from := height
	if from > 200 {
		from = height - 200
	}
	want, err := n.Core().GetLogs(from, height, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := gc.StreamLogs(ctx, &grpcapi.LogsRequest{FromBlock: from, ToBlock: height})
	if err != nil {
		t.Fatal(err)
	}
	var streamed []*grpcapi.Log
	for {
		batch, err := stream.Recv()
		if err == io.EOF {
			break // the clean end
		}
		if err != nil {
			t.Fatalf("logs stream: %v", err)
		}
		streamed = append(streamed, batch.Logs...)
	}
	if len(streamed) != len(want) {
		t.Fatalf("logs stream %d..%d: %d logs, GetLogs %d", from, height, len(streamed), len(want))
	}
	for i, l := range streamed {
		if !bytes.Equal(l.TxHash, want[i].TxHash.Bytes()) || l.Index != uint32(want[i].Index) {
			t.Fatalf("logs stream row %d: %x/%d, want %s/%d", i, l.TxHash, l.Index, want[i].TxHash, want[i].Index)
		}
	}

	txStream, err := gc.StreamTransactions(ctx, &grpcapi.StreamTransactionsRequest{
		FromBlock: height, ToBlock: height, Full: true})
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for {
		batch, err := txStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("transaction stream: %v", err)
		}
		if batch.BlockNumber != height {
			t.Fatalf("transaction stream delivered block %d, want %d", batch.BlockNumber, height)
		}
		seen += len(batch.TxHash)
		if len(batch.TxRlp) != len(batch.TxHash) {
			t.Fatalf("transaction stream: %d hashes but %d bodies", len(batch.TxHash), len(batch.TxRlp))
		}
	}
	if seen != len(jhashes) {
		t.Fatalf("transaction stream on block %d: %d transactions, block holds %d", height, seen, len(jhashes))
	}

	// --- the posting-list log reads, across all four peers -------------------
	// Driven by a real log of the range: its emitter, its signature and (when
	// it has one) the value at topic 1, so every read has something to find.
	plain := httptest.NewServer(plainhttp.Handler(n))
	defer plain.Close()
	if len(want) > 0 {
		l := want[0]
		emitter, sig := l.Address, l.Topics[0]
		lib, err := n.Core().LogsByEmitter(emitter, &sig, 0, 5, true)
		if err != nil {
			t.Fatal(err)
		}
		g, err := gc.GetLogsByEmitter(ctx, &grpcapi.LogsByEmitterRequest{Emitter: emitter.Bytes(), Topic0: sig.Bytes(), Page: &grpcapi.Page{Limit: 5}})
		if err != nil {
			t.Fatal(err)
		}
		pl := httpGet(t, plain, "getLogsByEmitter", url.Values{"emitter": {emitter.Hex()}, "topic0": {sig.Hex()}, "limit": {"5"}})
		var jl map[string]any
		json.Unmarshal(jsonRPC(t, jrpc, "edb_getLogsByEmitter", map[string]any{"emitter": emitter, "topic0": sig, "limit": 5}), &jl)
		if len(g.Logs) != len(lib.Logs) || len(pl["logs"].([]any)) != len(lib.Logs) || len(jl["logs"].([]any)) != len(lib.Logs) || g.More != lib.More || jl["more"] != lib.More {
			t.Fatalf("logs by emitter %s: library %d/%v, gRPC %d/%v, plain %d, JSON-RPC %d/%v",
				emitter, len(lib.Logs), lib.More, len(g.Logs), g.More, len(pl["logs"].([]any)), len(jl["logs"].([]any)), jl["more"])
		}
		for i, x := range lib.Logs {
			if !bytes.Equal(g.Logs[i].TxHash, x.TxHash.Bytes()) || g.Logs[i].Index != uint32(x.Index) {
				t.Fatalf("logs by emitter row %d: gRPC %x/%d, library %s/%d", i, g.Logs[i].TxHash, g.Logs[i].Index, x.TxHash, x.Index)
			}
		}
		if len(l.Topics) > 1 {
			value := l.Topics[1]
			libV, err := n.Core().LogsByTopicValue(value, nil, 0, 0, 5, true)
			if err != nil {
				t.Fatal(err)
			}
			gv, err := gc.GetLogsByTopicValue(ctx, &grpcapi.LogsByTopicValueRequest{Value: value.Bytes(), Page: &grpcapi.Page{Limit: 5}})
			if err != nil {
				t.Fatal(err)
			}
			pv := httpGet(t, plain, "getLogsByTopicValue", url.Values{"value": {value.Hex()}, "limit": {"5"}})
			var jv map[string]any
			json.Unmarshal(jsonRPC(t, jrpc, "edb_getLogsByTopicValue", map[string]any{"value": value, "limit": 5}), &jv)
			if len(gv.Logs) != len(libV.Logs) || len(pv["logs"].([]any)) != len(libV.Logs) || len(jv["logs"].([]any)) != len(libV.Logs) {
				t.Fatalf("logs by topic value %s: library %d, gRPC %d, plain %d, JSON-RPC %d",
					value, len(libV.Logs), len(gv.Logs), len(pv["logs"].([]any)), len(jv["logs"].([]any)))
			}
			libG, err := n.Core().TopicGroups(value, nil)
			if err != nil {
				t.Fatal(err)
			}
			gg, err := gc.GetTopicGroups(ctx, &grpcapi.TopicGroupsRequest{Value: value.Bytes()})
			if err != nil {
				t.Fatal(err)
			}
			pg := httpGet(t, plain, "getTopicGroups", url.Values{"value": {value.Hex()}})
			var jg []any
			json.Unmarshal(jsonRPC(t, jrpc, "edb_getTopicGroups", map[string]any{"value": value}), &jg)
			if len(gg.Groups) != len(libG) || len(pg["groups"].([]any)) != len(libG) || len(jg) != len(libG) {
				t.Fatalf("topic groups of %s: library %d, gRPC %d, plain %d, JSON-RPC %d", value, len(libG), len(gg.Groups), len(pg["groups"].([]any)), len(jg))
			}
			// The token shortcuts, on the holder the value spells (a Transfer
			// makes it one; anything else answers empty on every peer alike).
			holder := common.BytesToAddress(value.Bytes())
			for _, std := range []string{"erc20", "erc721", "erc1155"} {
				libT, err := n.Core().TokenTransfersByHolder(holder, std, 0, 5, true)
				if err != nil {
					t.Fatal(err)
				}
				gt, err := gc.GetTokenTransfersByHolder(ctx, &grpcapi.TokenTransfersRequest{Address: holder.Bytes(), Standard: std, Page: &grpcapi.Page{Limit: 5}})
				if err != nil {
					t.Fatal(err)
				}
				pt := httpGet(t, plain, "getTokenTransfersByHolder", url.Values{"address": {holder.Hex()}, "standard": {std}, "limit": {"5"}})
				var jt map[string]any
				json.Unmarshal(jsonRPC(t, jrpc, "edb_getTokenTransfersByHolder", map[string]any{"address": holder, "standard": std, "limit": 5}), &jt)
				if len(gt.Logs) != len(libT.Logs) || len(pt["logs"].([]any)) != len(libT.Logs) || len(jt["logs"].([]any)) != len(libT.Logs) {
					t.Fatalf("%s transfers of %s: library %d, gRPC %d, plain %d, JSON-RPC %d", std, holder, len(libT.Logs), len(gt.Logs), len(pt["logs"].([]any)), len(jt["logs"].([]any)))
				}
				libC, err := n.Core().TokenTransfersByContract(emitter, std, 0, 5, true)
				if err != nil {
					t.Fatal(err)
				}
				gcs, err := gc.GetTokenTransfersByContract(ctx, &grpcapi.TokenTransfersRequest{Address: emitter.Bytes(), Standard: std, Page: &grpcapi.Page{Limit: 5}})
				if err != nil {
					t.Fatal(err)
				}
				pc := httpGet(t, plain, "getTokenTransfersByContract", url.Values{"token": {emitter.Hex()}, "standard": {std}, "limit": {"5"}})
				if len(gcs.Logs) != len(libC.Logs) || len(pc["logs"].([]any)) != len(libC.Logs) {
					t.Fatalf("%s transfers by %s: library %d, gRPC %d, plain %d", std, emitter, len(libC.Logs), len(gcs.Logs), len(pc["logs"].([]any)))
				}
			}
			libK, err := n.Core().TokenContracts(holder)
			if err != nil {
				t.Fatal(err)
			}
			gk, err := gc.GetTokenContracts(ctx, &grpcapi.TokenContractsRequest{Address: holder.Bytes()})
			if err != nil {
				t.Fatal(err)
			}
			pk := httpGet(t, plain, "getTokenContracts", url.Values{"address": {holder.Hex()}})
			var jk []any
			json.Unmarshal(jsonRPC(t, jrpc, "edb_getTokenContracts", map[string]any{"address": holder}), &jk)
			if len(gk.Contracts) != len(libK) || len(pk["contracts"].([]any)) != len(libK) || len(jk) != len(libK) {
				t.Fatalf("token contracts of %s: library %d, gRPC %d, plain %d, JSON-RPC %d", holder, len(libK), len(gk.Contracts), len(pk["contracts"].([]any)), len(jk))
			}
		}
	}

	t.Logf("gRPC and JSON-RPC agree on the whole surface at head %d: block %d, tx %s, contract %s, %d streamed logs",
		head.Number, height, txHash, to, len(streamed))
}

// TestGRPCRefusesWhatTheNodeRefuses: the permanent refusal set is refused on
// gRPC too, and it is refused BY NAME. A method that does not exist answers
// Unimplemented (never an empty message a client would read as "none on this
// chain"), and GetNodeInfo carries the reason so a client can find out WHY
// without reading DESIGN.
func TestGRPCRefusesWhatTheNodeRefuses(t *testing.T) {
	n := synthNode(t)
	writeBlock(t, n, 0)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	grpcapi.RegisterEpochDBServer(g, grpcapi.New(n))
	go g.Serve(ln)
	defer g.Stop()
	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()

	// Nothing in the refusal set has a method, and asking for one is a status
	// code that names it rather than a plausible answer.
	for _, method := range []string{
		"GetProof", "DumpBlock", "AccountRange", "StorageRangeAt", "IntermediateRoots",
		"GetPreimage", "GetBadBlocks", "TraceBadBlock", "TraceChain", "GetModifiedAccounts",
		"SendRawTransaction", "SendTransaction", "SignTransaction", "GetTxPool",
		"StreamPendingTransactions",
	} {
		err := conn.Invoke(ctx, "/epochdb.v0.EpochDB/"+method, &grpcapi.HeadRequest{}, &grpcapi.HeadResponse{})
		if status.Code(err) != codes.Unimplemented {
			t.Fatalf("%s: %v (want Unimplemented; a refusal must never look like an answer)", method, err)
		}
	}

	// And the reasons are published, so a client learns them from the API.
	info, err := grpcapi.NewEpochDBClient(conn).GetNodeInfo(ctx, &grpcapi.NodeInfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"eth_getProof", "debug_preimage", "eth_sendRawTransaction", "txpool_"} {
		found := false
		for _, r := range info.Refusals {
			if strings.Contains(r.Capability, want) {
				if r.Reason == "" {
					t.Fatalf("refusal %q carries no reason", r.Capability)
				}
				found = true
			}
		}
		if !found {
			t.Fatalf("GetNodeInfo does not name %s among its refusals", want)
		}
	}
}

// jsonString calls a JSON-RPC method whose result is a string.
func jsonString(t *testing.T, srv *httptest.Server, method string, params ...any) string {
	t.Helper()
	var out string
	json.Unmarshal(jsonRPC(t, srv, method, params...), &out)
	return out
}

// hexBig prints a big-endian gRPC integer the way JSON-RPC prints it.
func hexBig(b []byte) string { return hexutil.EncodeBig(new(big.Int).SetBytes(b)) }

// sameJSON compares two JSON documents structurally: key order and whitespace
// are not part of the answer, everything else is.
func sameJSON(t *testing.T, a, b []byte) bool {
	t.Helper()
	var x, y any
	if err := json.Unmarshal(a, &x); err != nil {
		t.Fatalf("decode %s: %v", a, err)
	}
	if err := json.Unmarshal(b, &y); err != nil {
		t.Fatalf("decode %s: %v", b, err)
	}
	return reflect.DeepEqual(x, y)
}

// blockWithTxs walks down from the head for a block that has transactions, and
// returns it with its first transaction's hash. A corpus with none in the last
// 5,000 blocks fails the test rather than passing vacuously.
func blockWithTxs(t *testing.T, n *epochdb.Node, head uint64) (uint64, common.Hash) {
	t.Helper()
	for h := head; h > 0 && h+5000 > head; h-- {
		blk, err := n.Core().BlockAt(h)
		if err != nil {
			t.Fatal(err)
		}
		if txs := blk.Transactions(); len(txs) > 0 {
			return h, txs[0].Hash()
		}
	}
	t.Fatalf("no block with transactions in the 5000 below head %d", head)
	return 0, common.Hash{}
}

// --- the synthetic node: two blocks, written straight into the store ----------

// synthNode is a read-only node over a store this test writes into. The store
// is the same object the query layer reads, so a WriteBlock here is a new block
// as far as every adapter is concerned.
func synthNode(t *testing.T) *epochdb.Node {
	t.Helper()
	n, err := epochdb.Open(context.Background(), epochdb.Config{
		DataDir: t.TempDir(), Chain: testChain(t), ReadOnly: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.Close)
	return n
}

// writeBlock appends one empty block at height h.
func writeBlock(t *testing.T, n *epochdb.Node, h uint64) {
	t.Helper()
	hdr := &types.Header{
		Number:     new(big.Int).SetUint64(h),
		Time:       1767225600 + h,
		GasLimit:   15_000_000,
		BaseFee:    big.NewInt(25_000_000_000),
		Difficulty: big.NewInt(1),
		Extra:      []byte{},
	}
	raw, err := rlp.EncodeToBytes(hdr)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Store().WriteBlock(&store.BlockWrite{Height: h, HeaderRLP: raw}); err != nil {
		t.Fatal(err)
	}
}

// TestGRPCStreamFiresOnNewBlocks: the head stream must deliver a block that
// arrives AFTER the caller subscribed, which is the whole reason a stream
// exists rather than a poll loop in every consumer.
func TestGRPCStreamFiresOnNewBlocks(t *testing.T) {
	n := synthNode(t)
	writeBlock(t, n, 0)
	gc := grpcClient(t, n)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := gc.StreamHeads(ctx, &grpcapi.HeadRequest{})
	if err != nil {
		t.Fatal(err)
	}
	first, err := stream.Recv()
	if err != nil {
		t.Fatalf("first head: %v", err)
	}
	if first.Number != 0 {
		t.Fatalf("first head %d, want the stored head 0", first.Number)
	}

	// The block arrives while the stream is open.
	writeBlock(t, n, 1)
	next, err := stream.Recv()
	if err != nil {
		t.Fatalf("head after a new block: %v", err)
	}
	if next.Number != 1 {
		t.Fatalf("stream reported head %d after block 1 landed", next.Number)
	}
	blk, err := n.Core().BlockAt(1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(next.Hash, blk.Hash().Bytes()) {
		t.Fatalf("streamed hash %x, want %s", next.Hash, blk.Hash())
	}
}

// TestPlainHTTPAcceptsEveryParameterEncoding: query string, POST form body and
// POST JSON body are the same request (DESIGN entry point 4: "parameters
// accepted in ANY form"), and GET and POST are both allowed.
func TestPlainHTTPAcceptsEveryParameterEncoding(t *testing.T) {
	n := synthNode(t)
	writeBlock(t, n, 0)
	writeBlock(t, n, 1)
	srv := httptest.NewServer(plainhttp.Handler(n))
	defer srv.Close()

	want, err := n.Core().BlockAt(1)
	if err != nil {
		t.Fatal(err)
	}

	decode := func(resp *http.Response, err error) map[string]any {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		var out map[string]any
		if err := json.Unmarshal(body, &out); err != nil {
			t.Fatalf("decode %s: %v", body, err)
		}
		if e, ok := out["error"]; ok {
			t.Fatalf("plain HTTP: %v", e)
		}
		return out
	}

	answers := []map[string]any{
		// GET, query string.
		decode(http.Get(srv.URL + "/getBlock?number=1")),
		// POST, form body.
		decode(http.Post(srv.URL+"/getBlock", "application/x-www-form-urlencoded",
			strings.NewReader("number=1"))),
		// POST, JSON body.
		decode(http.Post(srv.URL+"/getBlock", "application/json",
			strings.NewReader(`{"number": 1}`))),
		// POST, JSON body, hex number: a number is read in either base.
		decode(http.Post(srv.URL+"/getBlock", "application/json",
			strings.NewReader(`{"number": "0x1"}`))),
		// GET with the number in the query and nothing else: the browser case.
		decode(http.Get(srv.URL + "/getBlock?number=0x1")),
	}
	for i, got := range answers {
		if got["hash"] != want.Hash().Hex() || uint64(got["number"].(float64)) != 1 {
			t.Fatalf("encoding %d answered block %v/%v, want 1/%s", i, got["number"], got["hash"], want.Hash())
		}
	}

	// The index is the discovery surface, and it names every method.
	idx := decode(http.Get(srv.URL + "/"))
	for _, m := range []string{"head", "getBlock", "getTransaction", "call", "getState",
		"searchTransactionsByAddress", "getLogs", "getLogsByEmitter", "getLogsByTopicValue", "getTopicGroups",
		"getTokenTransfersByHolder", "getTokenTransfersByContract", "getTokenContracts"} {
		if _, ok := idx[m]; !ok {
			t.Fatalf("GET / does not list %s: %v", m, idx)
		}
	}

	// An unknown method is a 404 that says so, not a 200 with an empty answer.
	resp, err := http.Get(srv.URL + "/getNothing")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown method: status %d", resp.StatusCode)
	}
}

// --- the baseline number -------------------------------------------------------

// BenchmarkInProcessCall is THE NUMBER THE FAST PATH EXISTS FOR (DESIGN's
// goal: 200-400k in-process calls/s on a 96-core box). It runs eth_call-shaped
// messages at the tip through the library, with nothing serialized anywhere:
//
//	EPOCHDB_CORPUS_DIR=$PWD/data-numine3 go test -run XXX -bench InProcessCall -benchtime 5s .
//
// Single-core and all-core are both reported; the parallel one is what the
// target compares against. NOT OPTIMIZED YET: this is the baseline to improve
// against, and the descent is what it measures (DESIGN's in-memory current
// state for the fast path does not exist yet).
func BenchmarkInProcessCall(b *testing.B) {
	n := corpusNode(b)
	head, err := n.Head()
	if err != nil {
		b.Fatal(err)
	}
	to, data := benchTarget(b, n, head.Number)
	msg := func() *rpc.CallMsg {
		return &rpc.CallMsg{
			To: &to, Data: data, Value: new(big.Int), GasLimit: 1_000_000,
			GasPrice: new(big.Int), GasFeeCap: new(big.Int), GasTipCap: new(big.Int),
		}
	}
	if out, err := n.CallAt(msg(), head.Number); err != nil {
		b.Logf("the probe call to %s reverts (%v): the EVM still runs, so the number stands", to, err)
	} else {
		b.Logf("probe call to %s at %d returned %d bytes", to, head.Number, len(out))
	}

	var failed atomic.Uint64
	b.Run("single", func(b *testing.B) {
		m := msg()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := n.CallAt(m, head.Number); err != nil {
				failed.Add(1)
			}
		}
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "calls/s")
	})
	b.Run("parallel", func(b *testing.B) {
		b.SetParallelism(1)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			m := msg()
			for pb.Next() {
				if _, err := n.CallAt(m, head.Number); err != nil {
					failed.Add(1)
				}
			}
		})
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "calls/s")
		b.Logf("GOMAXPROCS=%d", runtime.GOMAXPROCS(0))
	})
}

// benchTarget picks a real contract out of the corpus and an ERC20-shaped
// calldata for it: a call that reverts still runs the EVM, so the measurement
// stands either way, but a call that returns is the honest DeFi shape.
// EPOCHDB_BENCH_TO names a contract instead, which is how the CODE-HEAVY shape
// is measured: a proxy's call loads its own code and its implementation's, so
// it is two code reads per call rather than one.
func benchTarget(b testing.TB, n *epochdb.Node, head uint64) (common.Address, []byte) {
	b.Helper()
	if to := os.Getenv("EPOCHDB_BENCH_TO"); to != "" {
		return common.HexToAddress(to), common.FromHex("0x18160ddd") // totalSupply()
	}
	from := uint64(0)
	if head > 200 {
		from = head - 200
	}
	logs, err := n.Core().GetLogs(from, head, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	// The most recent log emitter is a contract with code, at the tip, and in
	// the read-through cache's hot region: the working set a DeFi caller has.
	if len(logs) > 0 {
		return logs[len(logs)-1].Address, common.FromHex("0x18160ddd") // totalSupply()
	}
	blk, err := n.Core().BlockAt(head)
	if err != nil {
		b.Fatal(err)
	}
	if txs := blk.Transactions(); len(txs) > 0 && txs[0].To() != nil {
		return *txs[0].To(), nil
	}
	b.Skip("no contract found near the head to call")
	return common.Address{}, nil
}

// BenchmarkGRPCCall is the same call over the gRPC adapter, in-process over a
// real socket: the difference against BenchmarkInProcessCall IS the per-call
// remote overhead DESIGN quotes at 20-50us of CPU.
func BenchmarkGRPCCall(b *testing.B) {
	n := corpusNode(b)
	head, err := n.Head()
	if err != nil {
		b.Fatal(err)
	}
	to, data := benchTarget(b, n, head.Number)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	g := grpc.NewServer()
	grpcapi.RegisterEpochDBServer(g, grpcapi.New(n))
	go g.Serve(ln)
	defer g.Stop()
	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()
	gc := grpcapi.NewEpochDBClient(conn)
	req := &grpcapi.CallRequest{To: to.Bytes(), Data: data, Gas: 1_000_000, Height: head.Number}
	ctx := context.Background()

	b.Run("single", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			gc.Call(ctx, req)
		}
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "calls/s")
	})
	b.Run("parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				gc.Call(ctx, req)
			}
		})
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "calls/s")
	})
}
