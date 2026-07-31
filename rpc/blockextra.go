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
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/fetch"
)

// addHeaderExtraFields adds the block fields that live in the header's
// Avalanche extras, which the two VMs do not share:
//
//   - extDataHash, blockExtraData, extDataGasUsed exist ONLY on coreth, where
//     atomic txs ride in the block's ExtData. subnet-evm registers
//     NOOPBlockBodyHooks and has no ExtData at all, so those keys are OMITTED
//     rather than emitted empty (an empty extDataHash would read as a real
//     zero hash to a client).
//   - blockGasCost exists on both.
func addHeaderExtraFields(fields map[string]any, blk *types.Block) {
	head := blk.Header()
	if fetch.RegisteredKind() == chain.SubnetEVM {
		if bgc := sevmcustomtypes.GetHeaderExtra(head).BlockGasCost; bgc != nil {
			fields["blockGasCost"] = (*hexutil.Big)(bgc)
		}
		return
	}
	headExtra := ccustomtypes.GetHeaderExtra(head)
	fields["extDataHash"] = headExtra.ExtDataHash
	fields["blockExtraData"] = hexutil.Bytes(ccustomtypes.BlockExtData(blk))
	if headExtra.ExtDataGasUsed != nil {
		fields["extDataGasUsed"] = (*hexutil.Big)(headExtra.ExtDataGasUsed)
	}
	if headExtra.BlockGasCost != nil {
		fields["blockGasCost"] = (*hexutil.Big)(headExtra.BlockGasCost)
	}
}
