package fetch

import (
	"context"
	"fmt"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	proposerblock "github.com/ava-labs/avalanchego/vms/proposervm/block"
	ethtypes "github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
)

// Probing a chain over p2p. Sizing an L1 does NOT need a public RPC: the
// accepted frontier is a consensus message every validator answers, and
// GetAncestors hands back the containers themselves. Everything below is the
// existing fetcher used READ-ONLY: dial (PeerList gossip to the L1's
// validators, 90s budget), acceptedFrontier, fetchAncestors. No store is
// opened, nothing is written, nothing is persisted.

// ProbeReport is what one p2p probe learned. Byte and tx counts are a SAMPLE
// taken from a contiguous run below the tip, not the whole chain: p2p has no
// get-block-by-height, so reaching an arbitrary height means walking there.
// Sampled history is therefore the busiest history, and an extrapolation from
// it is an upper bound on a chain whose traffic grew over time.
type ProbeReport struct {
	// Connected is the peers from the tracked set that connected (on an L1 the
	// set IS the subnet's validators), and ConnectWall is when the last one
	// arrived, measured from the first dial.
	Connected   int           `json:"connected"`
	ConnectWall time.Duration `json:"-"`
	ConnectSecs float64       `json:"connectSecs"`
	// Archival is how many of those answered a GetAncestors non-empty, i.e.
	// how many peers can actually serve this chain's history to a fetch.
	Archival int `json:"archivalPeers"`

	TipID     ids.ID `json:"tipContainerID"`
	TipHeight uint64 `json:"tipHeight"`
	// TipDecoded reports that the frontier container decoded as a
	// (ProposerVM-wrapped) EVM block under the registered extras. False means
	// the chain is not the VM kind we asked for, whatever its vmID says.
	TipDecoded bool `json:"tipDecoded"`

	Sampled     int    `json:"sampledContainers"`
	SampleBytes uint64 `json:"sampledBytes"`
	SampleTxs   uint64 `json:"sampledTxs"`
	SampleGas   uint64 `json:"sampledGas"`
	LowHeight   uint64 `json:"sampledLowHeight"`
}

// ProbeChain dials cfg's chain, reads its accepted frontier, and samples
// containers backward from it. settle is how long to keep watching for late
// validator connections after the dial returns (an L1's validators arrive
// through PeerList gossip 10-15s behind the primary peers). batches is how
// many GetAncestors rounds to sample; each round is one contiguous run.
//
// ctx MUST carry a deadline: a chain whose peers never answer is exactly what
// this is for, and the sampling loop retries until the context ends.
func ProbeChain(ctx context.Context, cfg Config, settle time.Duration, batches int) (*ProbeReport, error) {
	start := time.Now()
	f, err := dial(cfg)
	if err != nil {
		return nil, err
	}
	defer f.Close() // store is nil here: Close only tears the network down
	f.cfg = cfg

	r := &ProbeReport{}
	// Watch the pool rather than guessing: the interesting number is not
	// "someone connected" (dial already waited for that) but how much of the
	// validator set showed up and how long the tail took.
	for deadline := time.Now().Add(settle); time.Now().Before(deadline); {
		if n := f.connectedCount(); n > r.Connected {
			r.Connected, r.ConnectWall = n, time.Since(start)
		}
		select {
		case <-ctx.Done():
			return r, ctx.Err()
		case err := <-f.dispatchErrCh:
			return r, fmt.Errorf("network stopped: %w", err)
		case <-time.After(500 * time.Millisecond):
		}
	}
	r.ConnectSecs = r.ConnectWall.Seconds()
	if r.Connected == 0 {
		return r, fmt.Errorf("no peer of the tracked set connected")
	}

	tip, err := f.acceptedFrontier(ctx)
	if err != nil {
		return r, fmt.Errorf("accepted frontier: %w", err)
	}
	r.TipID = tip

	id := tip
	for i := 0; i < max(batches, 1); i++ {
		resp, _, ok := f.fetchAncestors(ctx, id)
		if !ok {
			break
		}
		next, n := ids.Empty, 0
		for _, raw := range resp.blocks {
			b, err := probeDecode(raw)
			if err != nil {
				break // a peer that mixes in junk stops the run, not the probe
			}
			if n == 0 && b.containerID != id {
				break
			}
			n++
			r.Sampled++
			r.SampleBytes += uint64(len(raw))
			r.SampleTxs += uint64(b.txs)
			r.SampleGas += b.gas
			if r.LowHeight == 0 || b.blockNumber < r.LowHeight {
				r.LowHeight = b.blockNumber
			}
			if r.TipHeight == 0 {
				r.TipHeight, r.TipDecoded = b.blockNumber, true
			}
			next = b.parentID
		}
		if n == 0 || next == ids.Empty || r.LowHeight <= 1 {
			break
		}
		id = next
	}
	r.Archival = f.pool.archivalCount()
	if r.Sampled == 0 {
		return r, fmt.Errorf("no peer served a container for frontier %s", tip)
	}
	return r, nil
}

// connectedCount is how many peers of the tracked set are connected right now.
func (f *Fetcher) connectedCount() int {
	f.pool.mu.Lock()
	defer f.pool.mu.Unlock()
	return len(f.pool.peers)
}

// probedBlock is parsedContainer plus the two numbers a sizing probe wants and
// the walk does not.
type probedBlock struct {
	parsedContainer
	txs int
	gas uint64
}

// probeDecode is parseContainer with the tx count and gas kept. Same two
// shapes, same order (ProposerVM wrapper first, bare eth block second), so a
// container this rejects is one the fetcher would reject too.
func probeDecode(raw []byte) (probedBlock, error) {
	if len(raw) == 0 {
		return probedBlock{}, fmt.Errorf("empty container")
	}
	inner := new(ethtypes.Block)
	proposerBlk, perr := proposerblock.ParseWithoutVerification(raw)
	if perr == nil {
		if err := rlp.DecodeBytes(proposerBlk.Block(), inner); err != nil {
			return probedBlock{}, fmt.Errorf("decode inner eth block: %w", err)
		}
		return probedBlock{
			parsedContainer: parsedContainer{
				containerID: proposerBlk.ID(),
				blockNumber: inner.NumberU64(),
				blockHash:   ids.ID(inner.Hash()),
				parentID:    proposerBlk.ParentID(),
				parentHash:  ids.ID(inner.ParentHash()),
				blockTime:   inner.Time(),
			},
			txs: inner.Transactions().Len(),
			gas: inner.GasUsed(),
		}, nil
	}
	_, _, rest, err := rlp.Split(raw)
	if err != nil {
		return probedBlock{}, fmt.Errorf("rlp split: %w (not a ProposerVM block either: %v)", err, perr)
	}
	if err := rlp.DecodeBytes(raw[:len(raw)-len(rest)], inner); err != nil {
		return probedBlock{}, fmt.Errorf("decode pre-fork eth block: %w (not a ProposerVM block either: %v)", err, perr)
	}
	return probedBlock{
		parsedContainer: parsedContainer{
			containerID: ids.ID(inner.Hash()),
			blockNumber: inner.NumberU64(),
			blockHash:   ids.ID(inner.Hash()),
			parentID:    ids.ID(inner.ParentHash()),
			parentHash:  ids.ID(inner.ParentHash()),
			blockTime:   inner.Time(),
		},
		txs: inner.Transactions().Len(),
		gas: inner.GasUsed(),
	}, nil
}
