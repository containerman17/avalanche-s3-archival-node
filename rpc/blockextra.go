package rpc

// The VM-kind-dependent half of eth_getBlockBy* (M7 of the subnet-evm
// scoping). A header's extras are a different Go type per VM kind, and the
// accessor for the OTHER kind reads a payload that was never registered, so
// this cannot be one function with a nil check inside it.
//
// The kind comes from the process-global libevm registration rather than from
// a NewServer argument: exactly one kind can be registered per process (the
// header encodings are mutually exclusive), so there is no second answer for
// this server to hold.

import (
	ccustomtypes "github.com/ava-labs/avalanchego/graft/coreth/plugin/evm/customtypes"
	sevmcustomtypes "github.com/ava-labs/avalanchego/graft/subnet-evm/plugin/evm/customtypes"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"

	"github.com/containerman17/avalanche-s3-archival-node/chain"
	"github.com/containerman17/avalanche-s3-archival-node/fetch"
)

// addHeaderExtraFields adds the block fields that live in the header's
// Avalanche extras, which the two VMs do not share:
//
//   - extDataHash, blockExtraData, extDataGasUsed exist ONLY on coreth, where
//     atomic txs ride in the block's ExtData. subnet-evm registers
//     NOOPBlockBodyHooks and has no ExtData at all, so those keys are OMITTED
//     rather than emitted empty (an empty extDataHash would read as a real
//     zero hash to a client).
//   - blockGasCost, timestampMilliseconds and minDelayExcess exist on both.
//
// Each VM's own PostRPCMarshal is what produces those keys inside the real
// node, so we call it rather than hand-copying a subset: a hand-copied subset
// is how timestampMilliseconds and minDelayExcess went missing from every
// block above the Granite activation. blockExtraData stays here because it
// comes from the block BODY, not the header extras.
func addHeaderExtraFields(fields map[string]any, blk *types.Block) {
	head := blk.Header()
	if fetch.RegisteredKind() == chain.SubnetEVM {
		// subnet-evm reports totalDifficulty as the height (difficulty is
		// always 1); coreth reports 0. Measured against the live reference
		// RPCs 2026-08-01: FIFA returns the height, Fuji C returns 0x0, and
		// emitting the height for both failed all 54 eth_getBlockByNumber
		// probes of the C-chain gate.
		fields["totalDifficulty"] = (*hexutil.Big)(head.Number)
		sevmcustomtypes.GetHeaderExtra(head).PostRPCMarshal(head, fields)
		return
	}
	fields["totalDifficulty"] = (*hexutil.Big)(common.Big0)
	ccustomtypes.GetHeaderExtra(head).PostRPCMarshal(head, fields)
	fields["blockExtraData"] = hexutil.Bytes(ccustomtypes.BlockExtData(blk))
}
