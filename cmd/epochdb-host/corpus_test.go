package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/upgrade"
	ethtypes "github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/avalanche-s3-archival-node/chain"
	"github.com/containerman17/avalanche-s3-archival-node/fetch"
)

func testCorpus(t *testing.T) ([]byte, []ids.ID) {
	t.Helper()
	fetch.RegisterExtras(chain.SubnetEVM)
	data := bytes.NewBufferString(corpusMagic)
	hashes := []ids.ID{ids.GenerateTestID()}
	for h := uint64(1); h <= 3; h++ {
		block := ethtypes.NewBlockWithHeader(&ethtypes.Header{
			Number: new(big.Int).SetUint64(h), Difficulty: new(big.Int),
			ParentHash: [32]byte(hashes[len(hashes)-1]), GasLimit: 1_000_000,
		})
		raw, err := rlp.EncodeToBytes(block)
		if err != nil {
			t.Fatal(err)
		}
		if err := binary.Write(data, binary.BigEndian, h); err != nil {
			t.Fatal(err)
		}
		if err := binary.Write(data, binary.BigEndian, uint32(len(raw))); err != nil {
			t.Fatal(err)
		}
		data.Write(raw)
		hashes = append(hashes, ids.ID(block.Hash()))
	}
	return data.Bytes(), hashes
}

func writeTestCorpus(t *testing.T, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "blocks.corpus")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestReadCorpusStopsAndResumes(t *testing.T) {
	data, hashes := testCorpus(t)
	for _, accepted := range []uint64{0, 1, 2} {
		t.Run(fmt.Sprint(accepted), func(t *testing.T) {
			var heights []uint64
			err := readCorpus(context.Background(), writeTestCorpus(t, data), accepted, 2, hashes[accepted], &upgrade.Mainnet, func(it item) error {
				heights = append(heights, it.h)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(heights) != int(2-accepted) {
				t.Fatalf("emitted heights %v from accepted %d", heights, accepted)
			}
			for i, height := range heights {
				if height != accepted+1+uint64(i) {
					t.Fatalf("emitted heights %v", heights)
				}
			}
		})
	}
}

func TestReadCorpusRejectsInvalidInput(t *testing.T) {
	data, hashes := testCorpus(t)
	badHeight := bytes.Clone(data)
	binary.BigEndian.PutUint64(badHeight[len(corpusMagic):], 2)
	badSize := bytes.Clone(data)
	binary.BigEndian.PutUint32(badSize[len(corpusMagic)+8:], maxCorpusContainer+1)
	for name, input := range map[string][]byte{
		"magic": []byte("incorrect"), "height": badHeight, "size": badSize,
		"short header": data[:len(corpusMagic)+3], "short payload": data[:len(data)-1],
	} {
		t.Run(name, func(t *testing.T) {
			err := readCorpus(context.Background(), writeTestCorpus(t, input), 0, 3, hashes[0], &upgrade.Mainnet, func(item) error { return nil })
			if err == nil {
				t.Fatal("accepted invalid corpus")
			}
		})
	}
	for _, accepted := range []uint64{0, 1} {
		if err := readCorpus(context.Background(), writeTestCorpus(t, data), accepted, 2, ids.GenerateTestID(), &upgrade.Mainnet, func(item) error { return nil }); err == nil {
			t.Fatalf("accepted wrong anchor at %d", accepted)
		}
	}
}

func TestWaitCorpusRPCRequiresExactHeight(t *testing.T) {
	var calls int
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":"0x9"}`)
			return
		}
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":"0xa"}`)
	})
	if err := waitCorpusRPC(context.Background(), handler, 10); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("made %d readiness calls", calls)
	}
	if err := waitCorpusRPC(context.Background(), handler, 9); err == nil {
		t.Fatal("accepted RPC height beyond the stop")
	}
}
