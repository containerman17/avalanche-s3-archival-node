// feecheck prints stock's EstimateNextBaseFee (customheader, the same call
// eth_gasPrice makes at the wall clock) for the head of a running node at
// several offsets after the head's timestamp, as tip + fee (= eth_gasPrice).
//
//	go run ./exp/feecheck URL BLOCK off1 off2 ...   (BLOCK: latest or a hex / decimal number)
//	go run ./exp/feecheck synth                      (vectors for rs/rpc fee.rs's unit test)
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"strconv"

	"github.com/ava-labs/avalanchego/graft/subnet-evm/commontype"
	"github.com/ava-labs/avalanchego/graft/subnet-evm/params/extras"
	"github.com/ava-labs/avalanchego/graft/subnet-evm/plugin/evm/customheader"
	"github.com/ava-labs/avalanchego/graft/subnet-evm/plugin/evm/customtypes"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"
)

func rpc(url, method string, params ...any) json.RawMessage {
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var r struct {
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(raw, &r); err != nil || r.Error != nil {
		panic(fmt.Sprintf("%s: %s %v", method, raw, err))
	}
	return r.Result
}

func main() {
	customtypes.Register()
	if os.Args[1] == "synth" {
		synth()
		return
	}
	url := os.Args[1]
	block := any(os.Args[2])
	if n, err := strconv.ParseUint(os.Args[2], 0, 64); err == nil {
		block = hexutil.EncodeUint64(n)
	}
	var head struct {
		Number    hexutil.Uint64 `json:"number"`
		Time      hexutil.Uint64 `json:"timestamp"`
		GasUsed   hexutil.Uint64 `json:"gasUsed"`
		BaseFee   *hexutil.Big   `json:"baseFeePerGas"`
		Extra     hexutil.Bytes  `json:"extraData"`
		TimeMilli *hexutil.Uint64 `json:"timestampMilliseconds"`
	}
	must(json.Unmarshal(rpc(url, "eth_getBlockByNumber", block, false), &head))
	var fcr struct {
		FeeConfig commontype.FeeConfig `json:"feeConfig"`
	}
	must(json.Unmarshal(rpc(url, "eth_feeConfig", block), &fcr))
	var tip hexutil.Big
	must(json.Unmarshal(rpc(url, "eth_maxPriorityFeePerGas"), &tip))
	h := customtypes.WithHeaderExtra(&types.Header{Number: new(big.Int).SetUint64(uint64(head.Number)), Time: uint64(head.Time), GasUsed: uint64(head.GasUsed), BaseFee: head.BaseFee.ToInt(), Extra: head.Extra}, &customtypes.HeaderExtra{TimeMilliseconds: (*uint64)(head.TimeMilli)})
	zero := uint64(0)
	cfg := &extras.ChainConfig{NetworkUpgrades: extras.NetworkUpgrades{SubnetEVMTimestamp: &zero}}
	fmt.Printf("head %d time %d gasUsed %d baseFee %s tip %s\n", h.Number, h.Time, h.GasUsed, h.BaseFee, tip.ToInt())
	for _, a := range os.Args[3:] {
		off, _ := strconv.ParseUint(a, 10, 64)
		// the oracle's clock in ms; any ms inside the second floors to the same second
		fee, err := customheader.EstimateNextBaseFee(cfg, fcr.FeeConfig, h, (h.Time+off)*1000+999)
		must(err)
		fmt.Printf("off %d now %d gasPrice %s\n", off, h.Time+off, hexutil.EncodeBig(new(big.Int).Add(fee, tip.ToInt())))
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// synth prints BaseFee for synthetic parents exercising every branch of
// baseFeeFromWindow: over / under target, the elapsed shift, windowsElapsed > 1,
// the floor, the exact-target early return, the delta floor of 1.
func synth() {
	zero := uint64(0)
	cfg := &extras.ChainConfig{NetworkUpgrades: extras.NetworkUpgrades{SubnetEVMTimestamp: &zero}}
	fc := commontype.FeeConfig{GasLimit: big.NewInt(20_000_000), TargetBlockRate: 2, MinBaseFee: big.NewInt(25_000_000_000), TargetGas: big.NewInt(15_000_000), BaseFeeChangeDenominator: big.NewInt(36), MinBlockGasCost: big.NewInt(0), MaxBlockGasCost: big.NewInt(1_000_000), BlockGasCostStep: big.NewInt(200_000)}
	window := func(v ...uint64) []byte {
		out := make([]byte, 80)
		for i, x := range v {
			for j := 0; j < 8; j++ {
				out[i*8+j] = byte(x >> (56 - 8*j))
			}
		}
		return out
	}
	type c struct {
		name    string
		baseFee int64
		gasUsed uint64
		extra   []byte
	}
	cases := []c{
		{"busy", 300_000_000_000, 8_000_000, window(1_000_000, 2_000_000, 3_000_000, 4_000_000, 5_000_000, 6_000_000, 7_000_000, 8_000_000, 9_000_000, 10_000_000)},
		{"quiet", 300_000_000_000, 21_000, window(0, 0, 0, 0, 0, 0, 0, 0, 0, 100_000)},
		{"exact", 300_000_000_000, 5_000_000, window(0, 0, 0, 0, 0, 0, 0, 0, 0, 10_000_000)},
		{"tiny", 25_000_000_001, 21_000, window(0, 0, 0, 0, 0, 0, 0, 0, 0, 14_978_999)},
		{"floor", 25_000_000_000, 0, window()},
	}
	for _, k := range cases {
		h := customtypes.WithHeaderExtra(&types.Header{Number: big.NewInt(1000), Time: 1_700_000_000, GasUsed: k.gasUsed, BaseFee: big.NewInt(k.baseFee), Extra: k.extra}, &customtypes.HeaderExtra{})
		for _, off := range []uint64{0, 1, 3, 9, 10, 11, 19, 20, 25, 100, 1000} {
			fee, err := customheader.EstimateNextBaseFee(cfg, fc, h, (h.Time+off)*1000+999)
			must(err)
			fmt.Printf("(\"%s\", %d, %d),\n", k.name, off, fee)
		}
	}
}
