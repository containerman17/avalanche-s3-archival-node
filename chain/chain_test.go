package chain

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	avaconstants "github.com/ava-labs/avalanchego/utils/constants"

	"github.com/containerman17/epochdb/dist"
)

// TestCChainDescriptor pins the C-chain path: the descriptor built from the
// embedded config must produce the SAME chain root the pre-descriptor code
// produced, or every sealed Fuji/mainnet epoch's footer chain breaks.
func TestCChainDescriptor(t *testing.T) {
	for _, tc := range []struct {
		networkID    uint32
		blockchainID string
	}{
		{avaconstants.FujiID, "yH8D7ThNJkxmtkuv2jgBa4P1Rn3Qpr4pPr7QYNfcdoS6k6HWp"},
		{avaconstants.MainnetID, "2q9e4r6Mu3U68nU1fYjgbR6JvwrRx36CohpAX5UQxse55x1Q5"},
	} {
		c, err := CChain(tc.networkID)
		if err != nil {
			t.Fatalf("network %d: %v", tc.networkID, err)
		}
		if got := c.BlockchainID.String(); got != tc.blockchainID {
			t.Errorf("network %d BlockchainID = %s, want %s", tc.networkID, got, tc.blockchainID)
		}
		if c.SubnetID != avaconstants.PrimaryNetworkID {
			t.Errorf("network %d SubnetID = %s, want the primary network", tc.networkID, c.SubnetID)
		}
		if c.VMKind != Coreth {
			t.Errorf("network %d VMKind = %s, want coreth", tc.networkID, c.VMKind)
		}
		if len(c.UpgradeJSON) != 0 {
			t.Errorf("network %d carries upgrade bytes, the C-chain has none", tc.networkID)
		}
		want, err := dist.ChainRoot(tc.networkID)
		if err != nil {
			t.Fatal(err)
		}
		if c.Root() != want {
			t.Errorf("network %d root = %x, want %x", tc.networkID, c.Root(), want)
		}
	}
	// 0 is Fuji, as it was everywhere before the descriptor.
	def, err := CChain(0)
	if err != nil {
		t.Fatal(err)
	}
	if def.NetworkID != avaconstants.FujiID {
		t.Errorf("CChain(0).NetworkID = %d, want Fuji", def.NetworkID)
	}
}

// TestResolveCached covers everything Resolve does without a network call: the
// cached descriptor is the authority on the dir, upgrade.json enters the root
// (ruling 6), "C" is the embedded C-chain, and nothing defaults to zero.
func TestResolveCached(t *testing.T) {
	ctx := context.Background()
	const (
		fifaChain  = "SUDoK9P89PCcguskyof41fZexw7U3zubDP2DZpGf3HbFWwJ4E"
		fifaSubnet = "h7egyVb6fKHMDpVaEsTEcy7YaEnXrayxZS4A1AEU4pyBzmwGp"
	)
	genesisData := []byte(`{"config":{"chainId":13322},"gasLimit":"0x0"}`)
	base := map[string]any{
		"networkID":    avaconstants.MainnetID,
		"blockchainID": fifaChain,
		"subnetID":     fifaSubnet,
		"vmKind":       "subnetevm",
		"genesisData":  base64.StdEncoding.EncodeToString(genesisData),
	}
	// dirWith writes a chain.json (and optionally an upgrade.json) into a fresh
	// data dir, exactly as a first start would have left it.
	dirWith := func(t *testing.T, f map[string]any, upgrade []byte) string {
		t.Helper()
		dir := t.TempDir()
		raw, err := json.Marshal(f)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "chain.json"), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		if upgrade != nil {
			if err := os.WriteFile(filepath.Join(dir, "upgrade.json"), upgrade, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		return dir
	}

	c, err := Resolve(ctx, fifaChain, avaconstants.MainnetID, dirWith(t, base, nil))
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if c.VMKind != SubnetEVM {
		t.Errorf("VMKind = %s, want subnetevm", c.VMKind)
	}
	if string(c.GenesisJSON) != string(genesisData) {
		t.Errorf("genesis bytes were not carried verbatim")
	}
	if c.BlockchainID.String() != fifaChain || c.SubnetID.String() != fifaSubnet {
		t.Errorf("ids not carried: %s / %s", c.BlockchainID, c.SubnetID)
	}
	if c.Root() != dist.ChainRootFrom(genesisData, nil) {
		t.Errorf("root is not sha256(genesisData)")
	}

	// upgrade.json in the data dir, avalanchego's convention: verbatim into the
	// root, no re-serialization.
	upgrade := []byte("{\"precompileUpgrades\":[]}\n")
	cu, err := Resolve(ctx, fifaChain, avaconstants.MainnetID, dirWith(t, base, upgrade))
	if err != nil {
		t.Fatalf("resolve with upgrade: %v", err)
	}
	if cu.Root() != dist.ChainRootFrom(genesisData, upgrade) {
		t.Error("root is not sha256(genesisData || upgradeBytes)")
	}
	if cu.Root() == c.Root() {
		t.Error("upgrade bytes did not change the chain root (ruling 6)")
	}

	// "C" is the embedded C-chain and needs no cache file at all.
	cc, err := Resolve(ctx, "C", avaconstants.MainnetID, t.TempDir())
	if err != nil {
		t.Fatalf("resolve C: %v", err)
	}
	if cc.VMKind != Coreth || cc.SubnetID != avaconstants.PrimaryNetworkID {
		t.Errorf("C resolved to %s on subnet %s", cc.VMKind, cc.SubnetID)
	}

	// Refusals. No snow.Context field may default to zero, an unknown vmKind
	// must not fall through to subnet-evm, and a cache for another chain must
	// not be read as this one.
	for name, mutate := range map[string]func(map[string]any){
		"no-network": func(f map[string]any) { delete(f, "networkID") },
		"no-subnet":  func(f map[string]any) { delete(f, "subnetID") },
		"no-genesis": func(f map[string]any) { delete(f, "genesisData") },
		"bad-kind":   func(f map[string]any) { f["vmKind"] = "erigon" },
		"other-chain": func(f map[string]any) {
			f["blockchainID"] = "2tmrrBo1Lgt1mzzvPSFt73kkQKFas5d1AP88tv9cicwoFp8BSn"
		},
	} {
		f := map[string]any{}
		for k, v := range base {
			f[k] = v
		}
		mutate(f)
		if _, err := Resolve(ctx, fifaChain, avaconstants.MainnetID, dirWith(t, f, nil)); err == nil {
			t.Errorf("%s: Resolve accepted an invalid cache", name)
		}
	}
	// A spec that is neither "C" nor a blockchainID is a typo, not a fetch.
	if _, err := Resolve(ctx, "not-an-id", avaconstants.MainnetID, t.TempDir()); err == nil {
		t.Error("Resolve accepted a spec that is not an ID")
	}
}
