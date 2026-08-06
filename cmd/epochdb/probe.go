package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ava-labs/avalanchego/ids"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/fetch"
)

// probeMain sizes an L1 WITHOUT a chain-specific RPC.
//
// The thing we kept getting wrong: an L1 candidate was rejected because it had
// no reachable public RPC, as if the tip height were an RPC fact. It is not.
// The accepted frontier is a consensus message, GetAncestors serves the
// containers, and both come off the chain's own validators over p2p, which is
// what the fetcher already does for a living. The P-chain (public, always
// there) supplies the descriptor. So the only thing a missing public RPC
// actually costs is upgrade.json, and since the chain root became
// sha256(genesisData) with the upgrade bytes DROPPED, an unknown upgrade file
// is no longer an identity problem: it is an execution risk that hard-stops at
// its activation height on a state-root mismatch, loudly, at a known place.
//
// Read-only by construction: no data dir, no store, no staging, nothing
// persisted. The output is one JSON object on stdout (the narration goes to
// stderr) so a survey over a hundred chains is a shell loop.
func probeMain(args []string) {
	fs := flag.NewFlagSet("probe", flag.ExitOnError)
	spec := fs.String("chain", "", "L1 blockchainID to probe (required)")
	network := fs.String("network", "mainnet", "network: fuji|mainnet")
	settle := fs.Duration("settle", 15*time.Second, "how long to keep watching for late validator connections after the dial returns")
	batches := fs.Int("batches", 3, "GetAncestors rounds sampled below the tip")
	timeout := fs.Duration("timeout", 3*time.Minute, "hard ceiling on the whole probe")
	// Surveying a hundred chains means a few hundred P-chain calls, and
	// api.avax.network is behind a rate limiter that answers a burst with an
	// hour of 429s (measured 2026-08-05). Any node serving /ext/bc/P will do
	// for the descriptor and the validator set, so the survey can point those
	// somewhere else and leave the public node only the p2p bootstrap.
	pchain := fs.String("pchain", "", "P-chain RPC base for the descriptor and validator set (default: --network's public endpoint)")
	fs.Parse(args)
	if *spec == "" {
		log.Fatal("epochdb dev probe: --chain <blockchainID> is required")
	}
	blockchainID, err := ids.FromString(*spec)
	if err != nil {
		log.Fatalf("epochdb dev probe: --chain %q: %v", *spec, err)
	}
	networkID, api, _ := netParams(*network)
	pAPI := api
	if *pchain != "" {
		pAPI = strings.TrimSuffix(*pchain, "/ext/bc/P")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	out := probeOut{Chain: *spec, Network: *network}

	// The descriptor, off the P-chain, exactly as chain.Resolve does it minus
	// the cache write (this command never touches a data dir).
	var bcs struct {
		Blockchains []struct {
			ID, Name, VMID string
		} `json:"blockchains"`
	}
	if err := retry(func() error { return pchainCall(ctx, pAPI, "platform.getBlockchains", nil, &bcs) }); err != nil {
		log.Printf("probe: platform.getBlockchains: %v", err)
	}
	for _, b := range bcs.Blockchains {
		if b.ID == *spec {
			out.Name, out.VMID = b.Name, b.VMID
			break
		}
	}
	var (
		genesisData []byte
		subnetStr   string
	)
	if err := retry(func() (err error) { genesisData, subnetStr, err = dist.CreateChainTx(ctx, pAPI, *spec); return }); err != nil {
		out.setupFail(fmt.Sprintf("descriptor: %v", err))
	}
	subnetID, err := ids.FromString(subnetStr)
	if err != nil {
		out.setupFail(fmt.Sprintf("subnetID %q: %v", subnetStr, err))
	}
	out.SubnetID = subnetStr
	out.Genesis = sniffGenesis(genesisData)
	root := dist.ChainRootFrom(genesisData)
	out.ChainRoot = fmt.Sprintf("%x", root[:8])

	// Weight shape first: a single-validator L1 has no fetch redundancy at all,
	// and that is worth knowing before a single byte is downloaded.
	var vdrs struct {
		Validators []struct {
			// The API emits stake weights as JSON STRINGS (they are uint64 and
			// JavaScript would eat the high ones).
			Weight string `json:"weight"`
		} `json:"validators"`
	}
	if err := retry(func() error {
		return pchainCall(ctx, pAPI, "platform.getCurrentValidators", map[string]string{"subnetID": subnetStr}, &vdrs)
	}); err != nil {
		out.setupFail(fmt.Sprintf("validators: %v", err))
	}
	weights := make([]uint64, 0, len(vdrs.Validators))
	for _, v := range vdrs.Validators {
		w, _ := strconv.ParseUint(v.Weight, 10, 64)
		weights = append(weights, w)
	}
	out.Validators = len(weights)
	out.Weights = weightShape(weights)

	cfg := fetch.Config{
		NodeURI: api,
		Chain: &chain.Chain{
			GenesisJSON:  genesisData,
			NetworkID:    networkID,
			SubnetID:     subnetID,
			BlockchainID: blockchainID,
			VMKind:       chain.SubnetEVM,
		},
	}
	// Retry ONLY a bootstrap-RPC failure. A p2p answer, including "nobody
	// connected", is the finding and is kept; re-dialling a dead chain would
	// cost the full 90s budget five times over.
	var (
		r    *fetch.ProbeReport
		perr error
	)
	retry(func() error {
		r, perr = fetch.ProbeChain(ctx, cfg, *settle, *batches)
		if perr != nil && bootstrapRPCFailure(perr) {
			return perr
		}
		return nil
	})
	if r != nil {
		out.Connected = r.Connected
		out.ConnectSecs = round1(r.ConnectSecs)
		out.Archival = r.Archival
		out.TipHeight = r.TipHeight
		out.TipDecoded = r.TipDecoded
		if r.TipID != ids.Empty {
			out.TipContainer = r.TipID.String()
		}
		out.Sampled = r.Sampled
		out.SampleLowHeight = r.LowHeight
		if r.Sampled > 0 {
			out.AvgBytes = round1(float64(r.SampleBytes) / float64(r.Sampled))
			out.AvgTxs = round2(float64(r.SampleTxs) / float64(r.Sampled))
			blocks := float64(r.TipHeight) + 1
			out.TipLocalRawBytes = uint64(out.AvgBytes * blocks)
			out.TipLocalTxs = uint64(out.AvgTxs * blocks)
			out.Coverage, out.SampleWarning = coverage(r.Sampled, r.TipHeight)
		}
	}
	if perr != nil {
		out.Error = perr.Error()
		if bootstrapRPCFailure(perr) {
			out.setupFail(perr.Error()) // our own throttle, not the chain's fault
		}
	}
	out.Verdict, out.Why = verdict(out)
	out.print()
}

// bootstrapRPCFailure reports that the probe died on the PUBLIC NODE rather
// than on the chain: the bootstrap RPC (info.getNetworkID, info.peers, the
// validator load) is the one part of a dial that is not p2p, and
// api.avax.network throttles it. The distinction is the whole survey: a
// throttled call says nothing about the chain, and recording it as UNREACHABLE
// would retire a candidate for our own rate limit.
//
// Matched on the message because dial builds these with fmt.Errorf and there
// is no sentinel to compare against; the strings are dial's own literals.
func bootstrapRPCFailure(err error) bool {
	s := err.Error()
	for _, frag := range []string{"info.getNetworkID", "info.getBlockchainID", "discover peers", "no peers returned from info.peers", "get validators"} {
		if strings.Contains(s, frag) {
			return true
		}
	}
	return false
}

// pchainCall is a bare platform.* JSON-RPC call against api+"/ext/bc/P", the
// same shape dist.CreateChainTx uses. Deliberately NOT avalanchego's
// platformvm.Client: that one posts to "/ext/P", an alias only a full node
// serves, and the third-party P-chain endpoints a survey falls back on when
// api.avax.network throttles us serve "/ext/bc/P" alone.
func pchainCall(ctx context.Context, api, method string, params any, out any) error {
	if params == nil {
		params = struct{}{}
	}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(api, "/")+"/ext/bc/P", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: http %d", method, resp.StatusCode)
	}
	var r struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return fmt.Errorf("%s: %w", method, err)
	}
	if r.Error != nil {
		return fmt.Errorf("%s: %s", method, r.Error.Message)
	}
	return json.Unmarshal(r.Result, out)
}

// retry rides out the public P-chain endpoint's rate limiter. A survey is a
// few hundred calls in a row against a Cloudflare-fronted host, so a 429 is
// the NORMAL answer there, and without this the survey's most common finding
// would be its own throttling.
// Slow on purpose: the limiter answers a burst with minutes of 429s, so a
// tight retry loop is indistinguishable from the burst that caused it.
func retry(f func() error) error {
	var err error
	for i := 0; i < 5; i++ {
		if err = f(); err == nil {
			return nil
		}
		time.Sleep(time.Duration(i+1) * 10 * time.Second)
	}
	return err
}

// probeOut is the whole report, and the JSON contract the survey reads.
type probeOut struct {
	Chain     string `json:"blockchainID"`
	Name      string `json:"name,omitempty"`
	Network   string `json:"network"`
	SubnetID  string `json:"subnetID,omitempty"`
	VMID      string `json:"vmID,omitempty"`
	ChainRoot string `json:"chainRoot,omitempty"`

	Validators int    `json:"validators"`
	Weights    string `json:"weightShape,omitempty"`
	Connected  int    `json:"connected"`
	// ConnectSecs is how long the LAST validator took to connect, which is the
	// number that separates "slow gossip" from "these nodes are gone".
	ConnectSecs float64 `json:"connectSecs"`
	Archival    int     `json:"archivalPeers"`

	TipHeight    uint64 `json:"tipHeight"`
	TipContainer string `json:"tipContainer,omitempty"`
	TipDecoded   bool   `json:"tipDecoded"`

	Sampled         int     `json:"sampledContainers"`
	SampleLowHeight uint64  `json:"sampledLowHeight"`
	AvgBytes        float64 `json:"avgBytesPerContainer"`
	AvgTxs          float64 `json:"avgTxsPerBlock"`
	// Coverage is what fraction of the chain the sample actually saw. It is
	// the number that makes the two below readable, and it is routinely on the
	// order of 0.01%: the sample is a CONTIGUOUS RUN IMMEDIATELY BELOW THE TIP
	// (p2p has no get-by-height, so reaching an arbitrary height means walking
	// there one GetAncestors round at a time), i.e. the last few hours of a
	// chain that may be years old.
	Coverage float64 `json:"sampleCoverageFraction"`
	// TipLocal* multiply the tip-local density by every block in the chain.
	// They are NOT counts, they are "what this chain would hold if it had
	// always run at today's rate", and no chain is required to have done that.
	// Measured errors in BOTH directions, 2026-08-06: Bnry read ~66M txs
	// against a real >1B (15x LOW, a chain that was busy and went quiet) and
	// blockticity read 20.6 tx/blk at the tip against 1.72 over a uniform
	// height sweep (12x HIGH). Do not admit a chain on these.
	TipLocalRawBytes uint64 `json:"tipLocalRawBytesExtrapolated"`
	TipLocalTxs      uint64 `json:"tipLocalTxsExtrapolated"`
	// SampleWarning spells the caveat out in the output itself, because the
	// coverage ratio was always derivable from sampledLowHeight and nobody
	// derived it.
	SampleWarning string `json:"sampleWarning,omitempty"`

	Genesis genesisSniff `json:"genesis"`

	Verdict string `json:"verdict"`
	Why     string `json:"why"`
	Error   string `json:"error,omitempty"`
}

func (o *probeOut) print() {
	blob, err := json.MarshalIndent(o, "", "  ")
	if err != nil {
		log.Fatalf("epochdb dev probe: %v", err)
	}
	fmt.Println(string(blob))
}

// setupFail ends a probe that never got as far as dialing. It is ERROR and
// NOT unreachable, which is the distinction the survey lives on: a throttled
// P-chain endpoint said nothing whatsoever about the chain's validators, and
// recording that as UNREACHABLE would retire a candidate for our own rate
// limit. Exit 0: it is a row in the survey, not a crash of the tool.
func (o *probeOut) setupFail(msg string) {
	o.Error = msg
	o.Verdict, o.Why = "ERROR", "the probe never reached p2p: "+msg
	o.print()
	os.Exit(0)
}

// genesisSniff is what the genesis bytes alone say about the VM. The vmID is
// NOT a signal: a stock subnet-evm ships under whatever plugin ID its team
// registered (FIFA does exactly that). The genesis config is: subnet-evm's
// feeConfig block has no coreth equivalent.
type genesisSniff struct {
	SubnetEVM   bool     `json:"looksSubnetEVM"`
	ChainID     uint64   `json:"chainId,omitempty"`
	Precompiles []string `json:"genesisPrecompiles,omitempty"`
	Note        string   `json:"note,omitempty"`
}

// genesisPrecompiles are subnet-evm's stateful-precompile config keys. Their
// presence does NOT make a chain non-stock (they are built into subnet-evm),
// but each one is behaviour a syncing node must reproduce, so they are named.
var genesisPrecompiles = []string{
	"contractDeployerAllowListConfig",
	"contractNativeMinterConfig",
	"txAllowListConfig",
	"feeManagerConfig",
	"rewardManagerConfig",
	"warpConfig",
}

func sniffGenesis(raw []byte) genesisSniff {
	var g struct {
		Config map[string]json.RawMessage `json:"config"`
	}
	if err := json.Unmarshal(raw, &g); err != nil {
		return genesisSniff{Note: "genesisData is not JSON: not an EVM chain"}
	}
	if g.Config == nil {
		return genesisSniff{Note: "genesis has no config block"}
	}
	s := genesisSniff{}
	if v, ok := g.Config["chainId"]; ok {
		json.Unmarshal(v, &s.ChainID)
	}
	_, hasFee := g.Config["feeConfig"]
	_, hasSubnetTS := g.Config["subnetEVMTimestamp"]
	s.SubnetEVM = hasFee || hasSubnetTS
	if !s.SubnetEVM {
		s.Note = "no feeConfig/subnetEVMTimestamp in genesis: unusual for subnet-evm"
	}
	for _, k := range genesisPrecompiles {
		if _, ok := g.Config[k]; ok {
			s.Precompiles = append(s.Precompiles, k)
		}
	}
	return s
}

// weightShape describes the stake distribution in one phrase. What an operator
// needs is not a histogram: it is whether losing one node loses the chain.
func weightShape(w []uint64) string {
	if len(w) == 0 {
		return "no validators"
	}
	if len(w) == 1 {
		return "SINGLE VALIDATOR (no fetch redundancy)"
	}
	sorted := append([]uint64(nil), w...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] > sorted[j] })
	var total uint64
	for _, v := range sorted {
		total += v
	}
	if total == 0 {
		return fmt.Sprintf("%d validators, zero weight", len(sorted))
	}
	top := 100 * float64(sorted[0]) / float64(total)
	if sorted[0] == sorted[len(sorted)-1] {
		return fmt.Sprintf("uniform, %d x %d", len(sorted), sorted[0])
	}
	return fmt.Sprintf("skewed, top=%.0f%% max/min=%.1fx", top, float64(sorted[0])/float64(max(sorted[len(sorted)-1], 1)))
}

// coverage reports what fraction of the chain the sample saw, and the warning
// that fraction earns. Split out so the threshold is one place and testable.
//
// 1% is generous: three GetAncestors rounds is ~1-2k containers (avalanchego
// caps a response at 2000 containers / 2 MiB), so anything past a ~200k-block
// chain is already below it, and a multi-million-block chain is at ~0.01%.
const trustworthyCoverage = 0.01

func coverage(sampled int, tipHeight uint64) (float64, string) {
	blocks := float64(tipHeight) + 1
	c := float64(sampled) / blocks
	if c >= trustworthyCoverage {
		return c, ""
	}
	return c, fmt.Sprintf("the sample is %.4f%% of the chain and sits entirely at the tip: "+
		"tipLocal* assume this chain always ran at today's rate. Do NOT admit a chain on them; "+
		"use tipHeight (exact) or a get-by-height sweep over a public RPC.", c*100)
}

// Size rule (the user's, amended 2026-08-06): the real bound is ~100M
// transactions, and the ~10M block cutoff is the PROXY for it at roughly 10
// tx/block. A known transaction count therefore wins over the block count.
//
// The catch, learned on Bnry the same day: the probe does not KNOW a
// transaction count. It knows the density of the last ~2k blocks. Bnry
// extrapolated to ~66M from the tip and really holds over a billion, so the
// block rule was the one that caught it and the tx extrapolation was the one
// that would have let it in. Hence maxTxs only ever REJECTS here; it never
// rescues a chain that is over maxBlocks. Overriding the block rule is a human
// call backed by a real count, which is exactly how the user made it.
const (
	maxBlocks = 10_000_000
	maxTxs    = 100_000_000
)

// verdict is the one line the survey ranks on. UNREACHABLE is a first-class
// answer: a chain whose validators never connect is the failure we want to
// find in 90 seconds rather than three days into a sync (lamina1 and henesys
// both died exactly there).
func verdict(o probeOut) (string, string) {
	switch {
	case o.Connected == 0:
		return "UNREACHABLE", "no validator connected over p2p"
	case o.TipHeight == 0 && o.Sampled == 0:
		// Its own verdict, because it is its own problem: the validators are
		// up and handshook, and the CHAIN is what will not answer (not
		// bootstrapped on those nodes, VM not running, or the subnet gated by
		// an allowedNodes list). Measured on 11 mainnet L1s, 2026-08-05.
		return "SILENT", "every validator connected, none answered GetAcceptedFrontier"
	case !o.TipDecoded:
		return "UNKNOWN", "frontier container did not decode as a subnet-evm block"
	case o.TipHeight > maxBlocks:
		return "TOO BIG", fmt.Sprintf("%d blocks > %d (a real tx count under %d can overrule this, a tip-local extrapolation cannot)", o.TipHeight, maxBlocks, maxTxs)
	case o.TipLocalTxs > maxTxs:
		return "TOO BIG", fmt.Sprintf("~%d txs extrapolated from tip-local density > %d", o.TipLocalTxs, maxTxs)
	case o.Archival == 0:
		return "SYNC", "within the size rule, but no peer answered GetAncestors: history may be pruned"
	}
	return "SYNC", fmt.Sprintf("%d blocks (exact), %d/%d validators serving; tip-local density extrapolates to ~%d txs but see sampleWarning", o.TipHeight, o.Archival, o.Validators, o.TipLocalTxs)
}

func round1(f float64) float64 { return float64(int64(f*10+0.5)) / 10 }
func round2(f float64) float64 { return float64(int64(f*100+0.5)) / 100 }
