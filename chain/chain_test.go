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

// TestLoad covers the --chain file: verbatim inline genesis, an upgrade file
// entering the root (ruling 6), and the refusals. No network call.
func TestLoad(t *testing.T) {
	dir := t.TempDir()
	genesisData := []byte(`{"config":{"chainId":13322},"gasLimit":"0x0"}`)
	upgrade := []byte(`{"precompileUpgrades":[]}`)
	upgradePath := filepath.Join(dir, "upgrade.json")
	if err := os.WriteFile(upgradePath, upgrade, 0o644); err != nil {
		t.Fatal(err)
	}

	const (
		fifaChain  = "SUDoK9P89PCcguskyof41fZexw7U3zubDP2DZpGf3HbFWwJ4E"
		fifaSubnet = "h7egyVb6fKHMDpVaEsTEcy7YaEnXrayxZS4A1AEU4pyBzmwGp"
		fifaVM     = "XxNJpcFRu85cPDeTZofVHdeD5NCLj1XTgZpJNJABexXJ5e2ho"
	)
	write := func(t *testing.T, name string, f map[string]any) string {
		t.Helper()
		raw, err := json.Marshal(f)
		if err != nil {
			t.Fatal(err)
		}
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, raw, 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	base := map[string]any{
		"networkID":    avaconstants.MainnetID,
		"blockchainID": fifaChain,
		"subnetID":     fifaSubnet,
		"vmID":         fifaVM,
		"genesisData":  base64.StdEncoding.EncodeToString(genesisData),
	}

	c, err := Load(context.Background(), write(t, "fifa.json", base))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c.VMKind != SubnetEVM {
		t.Errorf("VMKind = %s, want subnetevm (a custom vmID is not coreth)", c.VMKind)
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

	withUpgrade := map[string]any{}
	for k, v := range base {
		withUpgrade[k] = v
	}
	withUpgrade["upgradeFile"] = upgradePath
	cu, err := Load(context.Background(), write(t, "fifa-upgrade.json", withUpgrade))
	if err != nil {
		t.Fatalf("load with upgrade: %v", err)
	}
	if cu.Root() == c.Root() {
		t.Error("upgrade bytes did not change the chain root (ruling 6)")
	}
	if cu.Root() != dist.ChainRootFrom(genesisData, upgrade) {
		t.Error("root is not sha256(genesisData || upgradeBytes)")
	}

	// The C-chain's own vmID means coreth even in a descriptor file.
	cc := map[string]any{}
	for k, v := range base {
		cc[k] = v
	}
	cc["vmID"] = avaconstants.EVMID.String()
	ck, err := Load(context.Background(), write(t, "coreth.json", cc))
	if err != nil {
		t.Fatal(err)
	}
	if ck.VMKind != Coreth {
		t.Errorf("VMKind = %s, want coreth for the EVM vmID", ck.VMKind)
	}

	// Refusals: no snow.Context field may default to zero, and an unknown
	// vmKind must not silently fall through to subnet-evm.
	for name, mutate := range map[string]func(map[string]any){
		"no-network": func(f map[string]any) { delete(f, "networkID") },
		"no-chain":   func(f map[string]any) { delete(f, "blockchainID") },
		"no-subnet":  func(f map[string]any) { delete(f, "subnetID") },
		"bad-kind":   func(f map[string]any) { f["vmKind"] = "erigon" },
	} {
		f := map[string]any{}
		for k, v := range base {
			f[k] = v
		}
		mutate(f)
		if _, err := Load(context.Background(), write(t, name+".json", f)); err == nil {
			t.Errorf("%s: Load accepted an invalid descriptor", name)
		}
	}
}
