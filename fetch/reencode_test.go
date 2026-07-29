package fetch

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"testing"

	ccustomtypes "github.com/ava-labs/avalanchego/graft/coreth/plugin/evm/customtypes"
	proposerblock "github.com/ava-labs/avalanchego/vms/proposervm/block"
	ethtypes "github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
)

// TestBitIdentity: decode real stored containers, re-encode the inner eth
// block, and require byte-identical output. Also emits a rolling digest of
// every parsed field so two builds can be compared.
func TestBitIdentity(t *testing.T) {
	dir := os.Getenv("BITID_DIR")
	if dir == "" {
		t.Skip("BITID_DIR unset")
	}
	RegisterExtras()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var heights []uint64
	for _, e := range entries {
		var bucket uint64
		if _, err := fmt.Sscanf(e.Name(), "index_%d.log", &bucket); err != nil {
			continue
		}
		raw, err := os.ReadFile(dir + "/" + e.Name())
		if err != nil {
			t.Fatal(err)
		}
		for off := 0; off+indexRecSize <= len(raw); off += indexRecSize {
			heights = append(heights, binary.BigEndian.Uint64(raw[off:off+8]))
		}
	}
	sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })
	if len(heights) == 0 {
		t.Fatal("no heights")
	}
	t.Logf("corpus heights %d..%d (%d)", heights[0], heights[len(heights)-1], len(heights))

	r, err := OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	want := 20000
	if os.Getenv("BITID_ALL") != "" {
		want = len(heights)
	}
	step := len(heights) / want
	if step < 1 {
		step = 1
	}
	h := sha256.New()
	n, reenc, sae := 0, 0, 0
	for i := 0; i < len(heights); i += step {
		raw, ok, err := r.GetByHeight(heights[i])
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			continue
		}
		pc, err := parseContainer(raw)
		if err != nil {
			t.Fatalf("height %d: %v", heights[i], err)
		}
		if pc.blockNumber != heights[i] {
			t.Fatalf("index height %d holds block %d", heights[i], pc.blockNumber)
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], pc.blockNumber)
		h.Write(buf[:])
		binary.BigEndian.PutUint64(buf[:], pc.blockTime)
		h.Write(buf[:])
		h.Write(pc.containerID[:])
		h.Write(pc.blockHash[:])
		h.Write(pc.parentID[:])
		h.Write(pc.parentHash[:])

		// byte-identity: re-encode the inner block, compare to source bytes.
		inner := raw
		if pb, err := proposerblock.ParseWithoutVerification(raw); err == nil {
			inner = pb.Block()
		}
		blk := new(ethtypes.Block)
		if err := rlp.DecodeBytes(inner, blk); err != nil {
			t.Fatalf("height %d decode: %v", heights[i], err)
		}
		out, err := rlp.EncodeToBytes(blk)
		if err != nil {
			t.Fatalf("height %d encode: %v", heights[i], err)
		}
		if string(out) != string(inner) {
			t.Fatalf("height %d: re-encode differs (%d vs %d bytes)", heights[i], len(out), len(inner))
		}
		reenc++
		if ccustomtypes.GetHeaderExtra(blk.Header()).SettledHeight != nil {
			sae++
		}
		n++
	}
	t.Logf("BITID sampled=%d reencoded=%d sae_markers=%d digest=%s", n, reenc, sae, hex.EncodeToString(h.Sum(nil)))
}
