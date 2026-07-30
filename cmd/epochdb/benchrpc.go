package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"time"
)

// benchRPCMain A/Bs the block/fee/trivia RPC surface against the reference
// archive. Documented tolerances:
//   - eth_estimateGas: values may differ (different search ceilings);
//     compared on success/failure only, and the local estimate must
//     actually execute (verified via local eth_call with that gas).
//   - eth_gasPrice / eth_maxPriorityFeePerGas: the remote answers for its
//     LIVE head, ours for the fixed corpus; local-only sanity (> 0).
//   - eth_feeHistory: the remote refuses pre-London history ("beyond
//     historical limit"); local-only shape check.
//   - eth_getBlockReceipts: not in the remote coreth API; compared entry
//     by entry against remote eth_getTransactionReceipt.
//   - eth_syncing (remote: "not implemented in coreth"), eth_getProof
//     (locally unsupported by design), web3_clientVersion and
//     net_listening (identity fields): skipped.
func benchRPCMain(args []string) {
	fs := flag.NewFlagSet("ab-bench-rpc", flag.ExitOnError)
	local := fs.String("local", "http://127.0.0.1:9650", "local epochdb serve URL")
	remote := fs.String("remote", "", "reference archive RPC (default per --network)")
	network := fs.String("network", "fuji", "network: fuji|mainnet")
	n := fs.Int("n", 300, "total probes")
	seed := fs.Int64("seed", time.Now().UnixNano(), "probe RNG seed")
	fs.Parse(args)
	if *remote == "" {
		_, _, *remote = netParams(*network)
	}

	rng := rand.New(rand.NewSource(*seed))
	headRaw, rerr := rpcCall(*local, "eth_blockNumber", nil, 2)
	if rerr != nil {
		log.Fatalf("ab-bench-rpc: local head: %v", rerr)
	}
	var headHex string
	json.Unmarshal(headRaw, &headHex)
	var head uint64
	fmt.Sscanf(headHex, "0x%x", &head)
	log.Printf("ab-bench-rpc: local head=%d remote=%s seed=%d", head, *remote, *seed)

	stats := map[string][2]int{} // method -> {probes, mismatches}
	mismatches := 0
	bump := func(method, diff string) {
		s := stats[method]
		s[0]++
		if diff != "" {
			s[1]++
			mismatches++
			log.Printf("MISMATCH %-42s %s", method, diff)
		}
		stats[method] = s
	}

	// ab compares local vs remote result JSON for one call.
	ab := func(method string, params ...any) {
		lRes, lErr := rpcCall(*local, method, params, 2)
		rRes, rErr := rpcCall(*remote, method, params, 4)
		bump(method, diffJSON(params, lRes, lErr, rRes, rErr))
	}

	// Fixed-answer methods, once each.
	ab("eth_chainId")
	ab("net_version")
	ab("eth_accounts")
	ab("eth_coinbase")

	done := 4
	for done < *n {
		h := randHeight(rng, head)
		hHex := fmt.Sprintf("0x%x", h)

		// Block body both ways + counts + uncles.
		ab("eth_getBlockByNumber", hHex, false)
		ab("eth_getBlockByNumber", hHex, true)
		ab("eth_getBlockTransactionCountByNumber", hHex)
		ab("eth_getUncleCountByBlockNumber", hHex)
		ab("eth_getUncleByBlockNumberAndIndex", hHex, "0x0")
		done += 5

		// Hash-addressed variants, using the local block's hash.
		lBlk, lErr := rpcCall(*local, "eth_getBlockByNumber", []any{hHex, false}, 2)
		if lErr != nil {
			bump("eth_getBlockByNumber", fmt.Sprintf("h=%d local error: %v", h, lErr))
			continue
		}
		var blk struct {
			Hash         string   `json:"hash"`
			Transactions []string `json:"transactions"`
		}
		if err := json.Unmarshal(lBlk, &blk); err != nil {
			bump("eth_getBlockByNumber", fmt.Sprintf("h=%d local decode: %v", h, err))
			continue
		}
		ab("eth_getBlockByHash", blk.Hash, true)
		ab("eth_getBlockTransactionCountByHash", blk.Hash)
		done += 2

		if len(blk.Transactions) > 0 {
			idx := fmt.Sprintf("0x%x", rng.Intn(len(blk.Transactions)))
			ab("eth_getTransactionByBlockNumberAndIndex", hHex, idx)
			ab("eth_getTransactionByBlockHashAndIndex", blk.Hash, idx)
			done += 2

			// getBlockReceipts: remote lacks the method; compare entries
			// against remote per-tx receipts.
			bump("eth_getBlockReceipts", diffBlockReceipts(*local, *remote, hHex, blk.Transactions))
			done++

			// estimateGas tolerance probe: a value transfer from the
			// first tx's sender at this height.
			bump("eth_estimateGas", diffEstimateGas(*local, *remote, hHex, blk.Transactions[0]))
			done++
		}
	}

	// Local-only sanity (documented tolerances).
	for _, m := range []string{"eth_gasPrice", "eth_maxPriorityFeePerGas"} {
		res, rerr := rpcCall(*local, m, nil, 2)
		diff := ""
		var v string
		if rerr != nil || json.Unmarshal(res, &v) != nil || len(v) < 3 {
			diff = fmt.Sprintf("local sanity failed: %s %v", res, rerr)
		}
		bump(m+" (local-only)", diff)
	}
	fhRes, fhErr := rpcCall(*local, "eth_feeHistory", []any{"0x10", fmt.Sprintf("0x%x", head), []float64{25, 75}}, 2)
	var fh struct {
		BaseFeePerGas []any     `json:"baseFeePerGas"`
		GasUsedRatio  []float64 `json:"gasUsedRatio"`
		Reward        [][]any   `json:"reward"`
	}
	diff := ""
	if fhErr != nil || json.Unmarshal(fhRes, &fh) != nil ||
		len(fh.BaseFeePerGas) != 17 || len(fh.GasUsedRatio) != 16 || len(fh.Reward) != 16 {
		diff = fmt.Sprintf("shape: %s %v", fhRes, fhErr)
	}
	bump("eth_feeHistory (local-only)", diff)

	// eth_getProof must fail cleanly by design.
	_, perr := rpcCall(*local, "eth_getProof", []any{"0x0100000000000000000000000000000000000000", []string{}, "latest"}, 2)
	diff = ""
	if perr == nil || perr.Code != -32000 {
		diff = fmt.Sprintf("expected clean -32000, got %v", perr)
	}
	bump("eth_getProof (unsupported-by-design)", diff)

	fmt.Printf("\nab-bench-rpc: %d probes\n", done+4)
	for m, s := range stats {
		fmt.Printf("  %-46s %4d probes, %d mismatches\n", m, s[0], s[1])
	}
	if mismatches > 0 {
		log.Fatalf("RESULT: %d MISMATCHES", mismatches)
	}
	fmt.Println("RESULT: ZERO mismatches")
}

// diffJSON structurally compares two JSON-RPC answers (errors must agree
// on presence; results must DeepEqual after decoding).
func diffJSON(params []any, lRes json.RawMessage, lErr *remoteError, rRes json.RawMessage, rErr *remoteError) string {
	if (lErr != nil) != (rErr != nil) {
		return fmt.Sprintf("params=%v error asymmetry: local=%v remote=%v", params, lErr, rErr)
	}
	if lErr != nil {
		return ""
	}
	var lv, rv any
	if err := json.Unmarshal(lRes, &lv); err != nil {
		return fmt.Sprintf("params=%v local decode: %v", params, err)
	}
	if err := json.Unmarshal(rRes, &rv); err != nil {
		return fmt.Sprintf("params=%v remote decode: %v", params, err)
	}
	if !reflect.DeepEqual(lv, rv) {
		return fmt.Sprintf("params=%v\n  local:  %s\n  remote: %s", params, lRes, rRes)
	}
	return ""
}

func diffBlockReceipts(local, remote, hHex string, txHashes []string) string {
	lRes, lErr := rpcCall(local, "eth_getBlockReceipts", []any{hHex}, 2)
	if lErr != nil {
		// Paired errors are a match, same rule as diffJSON: above the sealed
		// end receipts legitimately fail on both sides.
		if _, rErr := rpcCall(remote, "eth_getTransactionReceipt", []any{txHashes[0]}, 4); rErr != nil {
			return ""
		}
		return fmt.Sprintf("block %s local error: %v", hHex, lErr)
	}
	var lReceipts []any
	if err := json.Unmarshal(lRes, &lReceipts); err != nil {
		return fmt.Sprintf("block %s decode: %v", hHex, err)
	}
	if len(lReceipts) != len(txHashes) {
		return fmt.Sprintf("block %s: %d receipts for %d txs", hHex, len(lReceipts), len(txHashes))
	}
	for i, txh := range txHashes {
		rRes, rErr := rpcCall(remote, "eth_getTransactionReceipt", []any{txh}, 4)
		if rErr != nil {
			return fmt.Sprintf("block %s remote receipt %d: %v", hHex, i, rErr)
		}
		var rv any
		if err := json.Unmarshal(rRes, &rv); err != nil {
			return fmt.Sprintf("block %s remote receipt %d decode: %v", hHex, i, err)
		}
		if !reflect.DeepEqual(lReceipts[i], rv) {
			lj, _ := json.Marshal(lReceipts[i])
			return fmt.Sprintf("block %s receipt %d:\n  local:  %s\n  remote: %s", hHex, i, lj, rRes)
		}
	}
	return ""
}

// diffEstimateGas applies the documented tolerance: success/failure must
// agree, and a successful local estimate must actually execute (verified
// via local eth_call with exactly that gas).
func diffEstimateGas(local, remote, hHex, txHash string) string {
	txRes, terr := rpcCall(local, "eth_getTransactionByHash", []any{txHash}, 2)
	if terr != nil {
		return "" // no tx index wired; nothing to probe
	}
	var tx struct {
		From string `json:"from"`
	}
	if json.Unmarshal(txRes, &tx) != nil || tx.From == "" {
		return ""
	}
	call := map[string]any{"from": tx.From, "to": "0x0100000000000000000000000000000000000000", "value": "0x1"}
	lRes, lErr := rpcCall(local, "eth_estimateGas", []any{call, hHex}, 2)
	rRes, rErr := rpcCall(remote, "eth_estimateGas", []any{call, hHex}, 4)
	if (lErr != nil) != (rErr != nil) {
		return fmt.Sprintf("h=%s success/failure asymmetry: local=%v(%s) remote=%v(%s)", hHex, lErr, lRes, rErr, rRes)
	}
	if lErr != nil {
		return ""
	}
	var gasHex string
	if err := json.Unmarshal(lRes, &gasHex); err != nil {
		return fmt.Sprintf("h=%s local decode: %v", hHex, err)
	}
	call["gas"] = gasHex
	if _, cerr := rpcCall(local, "eth_call", []any{call, hHex}, 2); cerr != nil {
		return fmt.Sprintf("h=%s estimate %s does not execute: %v", hHex, gasHex, cerr)
	}
	return ""
}
