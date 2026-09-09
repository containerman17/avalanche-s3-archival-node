//go:build epochdb_stub

package validator

import "github.com/ava-labs/libevm/rlp"

const realEngine = false

// prepareEngine preloads the canned head header the stub hands back.
func prepareEngine(genesis []byte) {
	raw, err := rlp.EncodeToBytes(testGenesisHeader(genesis))
	if err != nil {
		panic(err)
	}
	stubSet(0, raw)
}
