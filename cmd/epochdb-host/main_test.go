package main

import (
	"testing"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/upgrade"
	proposerblock "github.com/ava-labs/avalanchego/vms/proposervm/block"
	"github.com/ava-labs/libevm/rlp"
)

// unwrap must hand the inner VM the P-chain height proposervm would: the
// epoch's after Granite, the header's own after Etna, the parent's before.
func TestUnwrapPChainHeight(t *testing.T) {
	inner, _ := rlp.EncodeToBytes([]byte("inner"))
	up := upgrade.Mainnet
	build := func(ts time.Time, pch uint64, epoch proposerblock.Epoch) []byte {
		b, err := proposerblock.BuildUnsigned(ids.GenerateTestID(), ts, pch, epoch, inner)
		if err != nil {
			t.Fatal(err)
		}
		return b.Bytes()
	}
	var parent uint64
	got, pch := unwrap(build(up.EtnaTime.Add(-time.Hour), 10, proposerblock.Epoch{}), &up, &parent)
	if string(got) != string(inner) || pch != 0 || parent != 10 {
		t.Fatalf("pre-Etna: pch=%d parent=%d", pch, parent)
	}
	_, pch = unwrap(build(up.EtnaTime.Add(time.Hour), 11, proposerblock.Epoch{}), &up, &parent)
	if pch != 11 || parent != 11 {
		t.Fatalf("post-Etna: pch=%d parent=%d", pch, parent)
	}
	_, pch = unwrap(build(up.GraniteTime.Add(time.Hour), 12, proposerblock.Epoch{PChainHeight: 7, Number: 1}), &up, &parent)
	if pch != 7 || parent != 12 {
		t.Fatalf("post-Granite: pch=%d parent=%d", pch, parent)
	}
	// Pre-proposervm: the container is the eth block, trailing bytes dropped.
	got, pch = unwrap(append(append([]byte{}, inner...), 0xff), &up, &parent)
	if string(got) != string(inner) || pch != 0 {
		t.Fatalf("pre-fork: got %x pch=%d", got, pch)
	}
}
