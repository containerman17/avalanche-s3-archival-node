package store

import (
	"bytes"
	"encoding/binary"
	"fmt"

	proposerblock "github.com/ava-labs/avalanchego/vms/proposervm/block"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
)

// CONTAINER REASSEMBLY.
//
// epochdb never stores a container whole. It stores hdr/<height> (the header
// RLP), pvm/<height> (everything else about the container) and tx/<txnum> (the
// tx RLPs), and p2p block serving still has to hand a peer back the VERBATIM
// bytes the container was fetched as (DESIGN entry point 5).
//
// THE pvm ROW IS A TEMPLATE, NOT THE WRAPPER BYTES. A container laid out flat
// is:
//
//	A | headerRLP | B | tx1..txN | C
//
// where A is the proposervm wrapper prefix plus the inner block's RLP list
// header, B is the transaction list's RLP header, and C is everything after
// the last transaction: the inner block's remaining fields (uncles, and for
// coreth also Version and ExtData) plus the wrapper's suffix (its signature).
// A, B and C are stored verbatim as
//
//	pvm = [4B big-endian len(A)] A [4B big-endian len(B)] B C
//
// so Reassemble is pure concatenation: no RLP is re-encoded, no list length is
// recomputed (the transactions are stored verbatim, so every length in A and B
// is still correct), and the shape of the block is irrelevant. That last point
// is why this is a template and not "the wrapper prefix and suffix around the
// inner block": coreth's block body carries Version and ExtData AFTER the
// uncles, and a scheme that rebuilds the inner block from header + txs alone
// silently drops them.
//
// An EMPTY pvm row is the shorthand for the common pre-proposervm container:
// no wrapper at all, and an inner block that is exactly the three-field geth
// shape [header, txs, uncles] with no uncles. SplitContainer emits the
// shorthand only when it has checked that it reproduces the container byte for
// byte, so it is never a guess.

// SplitContainer takes a raw container as fetched and returns the pvm row and
// the inner eth block. The libevm extras must already be registered
// (fetch.RegisterExtras).
//
// It CHECKS its own work: the pieces epochdb will store (the re-encoded header
// and transactions, plus the returned pvm row) are reassembled and compared to
// raw before returning. A container that does not round trip is rejected here,
// where it can still be diagnosed, rather than years later when a peer asks
// for it.
func SplitContainer(raw []byte) (pvm []byte, inner *types.Block, err error) {
	if len(raw) == 0 {
		return nil, nil, fmt.Errorf("store: empty container")
	}

	// Same decoder order as fetch/parse.go: proposervm first, then the
	// pre-fork bare RLP block.
	innerBytes := raw
	proposerBlk, perr := proposerblock.ParseWithoutVerification(raw)
	if perr == nil {
		innerBytes = proposerBlk.Block()
	} else {
		_, _, rest, serr := rlp.Split(raw)
		if serr != nil {
			return nil, nil, fmt.Errorf("store: rlp split: %w (not a ProposerVM block either: %v)", serr, perr)
		}
		innerBytes = raw[:len(raw)-len(rest)]
	}

	inner = new(types.Block)
	if err := rlp.DecodeBytes(innerBytes, inner); err != nil {
		return nil, nil, fmt.Errorf("store: decode inner eth block: %w", err)
	}

	headerRLP, txRLPs, err := blockPieces(inner)
	if err != nil {
		return nil, nil, err
	}

	// The shorthand first: if the bare three-field rebuild already is the
	// container, the pvm row is empty.
	if out, e := Reassemble(nil, headerRLP, txRLPs); e == nil && bytes.Equal(out, raw) {
		return nil, inner, nil
	}

	off := bytes.Index(raw, innerBytes)
	if off < 0 {
		return nil, nil, fmt.Errorf("store: inner block bytes are not a substring of the container")
	}
	hs, he, ts, te, err := blockSpans(innerBytes)
	if err != nil {
		return nil, nil, err
	}
	pvm = pvmTemplate(raw, off+hs, off+he, off+ts, off+te)

	out, err := Reassemble(pvm, headerRLP, txRLPs)
	if err != nil {
		return nil, nil, err
	}
	if !bytes.Equal(out, raw) {
		return nil, nil, fmt.Errorf("store: container does not round trip (%d bytes in, %d out)", len(raw), len(out))
	}
	return pvm, inner, nil
}

// pvmTemplate cuts the container into the three template pieces around the
// header span [hs, he) and the transaction-payload span [ts, te), and packs
// them as [4B len(A)] A [4B len(B)] B C.
func pvmTemplate(raw []byte, hs, he, ts, te int) []byte {
	a, b, c := raw[:hs], raw[he:ts], raw[te:]
	pvm := binary.BigEndian.AppendUint32(make([]byte, 0, 8+len(a)+len(b)+len(c)), uint32(len(a)))
	pvm = append(pvm, a...)
	pvm = binary.BigEndian.AppendUint32(pvm, uint32(len(b)))
	pvm = append(pvm, b...)
	return append(pvm, c...)
}

// Reassemble rebuilds the verbatim container from the stored pieces.
func Reassemble(pvm, headerRLP []byte, txRLPs [][]byte) ([]byte, error) {
	if len(pvm) == 0 {
		txs := make([]rlp.RawValue, len(txRLPs))
		for i, t := range txRLPs {
			txs[i] = t
		}
		return rlp.EncodeToBytes(&bareBlock{Header: headerRLP, Txs: txs})
	}
	if len(pvm) < 8 {
		return nil, fmt.Errorf("store: pvm row is %d bytes, too short for a template", len(pvm))
	}
	la := int(binary.BigEndian.Uint32(pvm[:4]))
	if 4+la+4 > len(pvm) {
		return nil, fmt.Errorf("store: pvm row: bad prefix length %d", la)
	}
	a := pvm[4 : 4+la]
	lb := int(binary.BigEndian.Uint32(pvm[4+la : 8+la]))
	if 8+la+lb > len(pvm) {
		return nil, fmt.Errorf("store: pvm row: bad tx-list header length %d", lb)
	}
	b, c := pvm[8+la:8+la+lb], pvm[8+la+lb:]

	n := len(a) + len(headerRLP) + len(b) + len(c)
	for _, t := range txRLPs {
		n += len(t)
	}
	out := make([]byte, 0, n)
	out = append(out, a...)
	out = append(out, headerRLP...)
	out = append(out, b...)
	for _, t := range txRLPs {
		out = append(out, t...)
	}
	return append(out, c...), nil
}

// bareBlock is the pre-proposervm shorthand: the three-field geth block RLP,
// written from bytes epochdb already holds. rlp.RawValue is copied through
// verbatim, so the list headers are the only thing computed here.
type bareBlock struct {
	Header rlp.RawValue
	Txs    []rlp.RawValue
	Uncles []rlp.RawValue
}

// blockPieces re-encodes the rows epochdb stores for a block: hdr/<height> and
// one tx/<txnum> per transaction.
func blockPieces(b *types.Block) (headerRLP []byte, txRLPs [][]byte, err error) {
	headerRLP, err = rlp.EncodeToBytes(b.Header())
	if err != nil {
		return nil, nil, fmt.Errorf("store: encode header: %w", err)
	}
	for i, tx := range b.Transactions() {
		enc, err := rlp.EncodeToBytes(tx)
		if err != nil {
			return nil, nil, fmt.Errorf("store: encode tx %d: %w", i, err)
		}
		txRLPs = append(txRLPs, enc)
	}
	return headerRLP, txRLPs, nil
}

// blockSpans locates the header element and the transaction-list PAYLOAD
// inside an inner eth block RLP, as offsets into it. Everything before hs,
// between he and ts, and after te is template.
func blockSpans(inner []byte) (hs, he, ts, te int, err error) {
	content, rest, err := rlp.SplitList(inner)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("store: block is not an RLP list: %w", err)
	}
	hs = len(inner) - len(rest) - len(content)
	_, _, afterHeader, err := rlp.Split(content)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("store: block header element: %w", err)
	}
	he = len(inner) - len(afterHeader)
	txs, afterTxs, err := rlp.SplitList(afterHeader)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("store: block transaction list: %w", err)
	}
	ts = len(inner) - len(afterTxs) - len(txs)
	return hs, he, ts, ts + len(txs), nil
}
