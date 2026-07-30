package dist

// The chain root, the anchor of the epoch hash chain (DESIGN.md rulings 5 and 6
// of 2026-07-31). Epoch 1's footer carries it as its previous-file hash, so one
// head hash authenticates all of a chain's history and there is no global
// registry anywhere.
//
// THE BYTE-EXACT RULE:
//
//	root = sha256(genesisData || upgradeBytes)
//
// genesisData is the P-chain CreateChainTx `genesisData` field verbatim, for
// the tx whose txID IS the blockchainID: canonical, on-chain, independently
// verifiable by anyone with a P-chain endpoint, and immune to the whitespace
// and key-order sensitivity of hashing a JSON string someone re-serialized.
// upgradeBytes is the chain's upgrade.json verbatim, or nothing at all when the
// chain has none. No separator, no length prefix, no normalization of either
// side: the on-chain genesisData length fixes the split.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/ava-labs/avalanchego/genesis"
)

// ChainRoot is the anchor for a primary-network C-chain.
//
// VERIFIED 2026-07-31 against the live public API, which is what ruling 5 asked
// for: `platform.getTx` DOES serve the C-chain's CreateChainTx (mainnet
// 2q9e4r6Mu3U68nU1fYjgbR6JvwrRx36CohpAX5UQxse55x1Q5, Fuji
// yH8D7ThNJkxmtkuv2jgBa4P1Rn3Qpr4pPr7QYNfcdoS6k6HWp), and its genesisData is
// BYTE-IDENTICAL to the embedded cfg.CChainGenesis this reads: 1352 bytes
// hashing to b666bb65... on mainnet and ee55f14e... on Fuji, matching the
// on-chain bytes exactly (see chainroot_test.go, which pins both). So the
// C-chain follows the same rule as an L1 and needs no network call to do it:
// the embedded string IS the canonical genesisData. The C-chain has no
// upgrade.json, so its upgrade contribution is empty.
func ChainRoot(networkID uint32) ([32]byte, error) {
	cfg := genesis.GetConfig(networkID)
	if cfg == nil {
		return [32]byte{}, fmt.Errorf("dist: no embedded genesis config for network %d", networkID)
	}
	return ChainRootFrom([]byte(cfg.CChainGenesis), nil), nil
}

// ChainRootFrom is the rule itself: sha256(genesisData || upgradeBytes), with
// upgradeBytes nil for a chain that has no upgrade.json.
func ChainRootFrom(genesisData, upgradeBytes []byte) [32]byte {
	h := sha256.New()
	h.Write(genesisData)
	h.Write(upgradeBytes)
	var out [32]byte
	h.Sum(out[:0])
	return out
}

// GenesisData fetches a chain's canonical genesis bytes from a P-chain
// endpoint: `platform.getTx` on the blockchainID, which IS the CreateChainTx's
// txID. apiURL is a node base URL, e.g. https://api.avax.network.
//
// This is the L1 door for ruling 5 and nothing more: a subnet-evm chain has no
// embedded genesis anywhere, so this is where its root (and later its executor)
// gets the bytes. The C-chain does not need it, see ChainRoot.
func GenesisData(ctx context.Context, apiURL, blockchainID string) ([]byte, error) {
	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"platform.getTx","params":{"txID":%q,"encoding":"json"}}`, blockchainID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL+"/ext/bc/P", bytes.NewReader([]byte(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("dist: platform.getTx %s: %w", blockchainID, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("dist: platform.getTx %s: %w", blockchainID, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("dist: platform.getTx %s: http %d", blockchainID, resp.StatusCode)
	}
	return parseGenesisData(raw)
}

// parseGenesisData pulls genesisData out of a platform.getTx JSON response. Its
// own function so the FIFA fixture can exercise it with no network call.
func parseGenesisData(raw []byte) ([]byte, error) {
	var r struct {
		Result struct {
			Tx struct {
				UnsignedTx struct {
					// []byte through encoding/json is base64, which is what
					// the API emits for genesisData under encoding "json".
					GenesisData []byte `json:"genesisData"`
				} `json:"unsignedTx"`
			} `json:"tx"`
		} `json:"result"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, fmt.Errorf("dist: platform.getTx response: %w", err)
	}
	if r.Error != nil {
		return nil, fmt.Errorf("dist: platform.getTx: %s", r.Error.Message)
	}
	g := r.Result.Tx.UnsignedTx.GenesisData
	if len(g) == 0 {
		return nil, fmt.Errorf("dist: platform.getTx returned no genesisData: not a CreateChainTx")
	}
	return g, nil
}
