//go:build epochdb_stub

package validator

import (
	"encoding/json"

	sevmcore "github.com/ava-labs/avalanchego/graft/subnet-evm/core"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
)

const realEngine = false

// testGenesisHeader: subnet-evm/core is fine here, the stub links no engine.
func testGenesisHeader(genesis []byte) *types.Header {
	registerExtras()
	g := new(sevmcore.Genesis)
	if err := json.Unmarshal(genesis, g); err != nil {
		panic(err)
	}
	return g.ToBlock().Header()
}

// prepareEngine preloads the canned head header the stub hands back.
func prepareEngine(genesis []byte) {
	raw, err := rlp.EncodeToBytes(testGenesisHeader(genesis))
	if err != nil {
		panic(err)
	}
	stubSet(0, raw)
}
