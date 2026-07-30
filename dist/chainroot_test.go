package dist

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"

	"github.com/ava-labs/avalanchego/utils/constants"
)

// TestChainRootCChain pins the C-chain verdict of ruling 5: the embedded
// cfg.CChainGenesis bytes ARE the on-chain CreateChainTx genesisData, so the
// root computed with no network call equals sha256 of what platform.getTx
// serves. The two hashes below were taken from the live API on 2026-07-31
// (mainnet /ext/bc/P getTx 2q9e4r6Mu3U68nU1fYjgbR6JvwrRx36CohpAX5UQxse55x1Q5,
// Fuji getTx yH8D7ThNJkxmtkuv2jgBa4P1Rn3Qpr4pPr7QYNfcdoS6k6HWp; 1352 bytes
// each). If avalanchego's embedded string ever drifts from the chain, this
// fails, which is the point.
func TestChainRootCChain(t *testing.T) {
	for _, tc := range []struct {
		net  uint32
		want string // sha256 of the on-chain genesisData
	}{
		{constants.MainnetID, "b666bb656e443e80968ab52843ae53244005ee5759e15fbc6c0bb4b73fba3c1a"},
		{constants.FujiID, "ee55f14ef1ecb457457dd06760d06e8db709314578876b5acac6b8c3f56f4b3a"},
	} {
		root, err := ChainRoot(tc.net)
		if err != nil {
			t.Fatalf("network %d: %v", tc.net, err)
		}
		if got := hex.EncodeToString(root[:]); got != tc.want {
			t.Fatalf("network %d chain root %s, want the on-chain genesisData hash %s", tc.net, got, tc.want)
		}
	}
}

// TestGenesisDataFromCreateChainTx runs the L1 path over a recorded
// platform.getTx response (the FIFA chain,
// SUDoK9P89PCcguskyof41fZexw7U3zubDP2DZpGf3HbFWwJ4E, fetched 2026-07-31) with
// no network call, and pins the byte-exact root rule on top of it.
func TestGenesisDataFromCreateChainTx(t *testing.T) {
	raw, err := os.ReadFile("testdata/fifa_createchaintx.json")
	if err != nil {
		t.Fatal(err)
	}
	g, err := parseGenesisData(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(g) != 55231 {
		t.Fatalf("genesisData is %d bytes, want the recorded 55231", len(g))
	}
	const want = "5d82add21f32a0a0d63496dddfbdb9752e68273ba7d6c4114c5eb6a2f2f71ff1"
	sum := sha256.Sum256(g)
	if hex.EncodeToString(sum[:]) != want {
		t.Fatalf("genesisData hashes to %x, want %s", sum, want)
	}
	// Ruling 6: no upgrade.json means an empty contribution, so the root is
	// exactly sha256(genesisData); an upgrade file must change it.
	if root := ChainRootFrom(g, nil); root != sum {
		t.Fatalf("root with no upgrade bytes %x, want %x", root, sum)
	}
	if ChainRootFrom(g, []byte("{}")) == sum {
		t.Fatal("upgrade bytes did not change the chain root")
	}

	if _, err := parseGenesisData([]byte(`{"jsonrpc":"2.0","error":{"code":-32000,"message":"not found"},"id":1}`)); err == nil {
		t.Fatal("an error response must not parse as genesis bytes")
	}
	if _, err := parseGenesisData([]byte(`{"jsonrpc":"2.0","result":{"tx":{"unsignedTx":{}}},"id":1}`)); err == nil {
		t.Fatal("a tx with no genesisData must be refused")
	}
}
