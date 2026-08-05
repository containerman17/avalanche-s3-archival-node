package dist

// The chain root, the anchor of the epoch hash chain (DESIGN.md ruling 5 of
// 2026-07-31 as amended 2026-08-05). Epoch 1's footer carries it as its
// previous-file hash, so one head hash authenticates all of a chain's history
// and there is no global registry anywhere.
//
// THE BYTE-EXACT RULE:
//
//	root = sha256(genesisData)
//
// genesisData is the P-chain CreateChainTx `genesisData` field verbatim, for
// the tx whose txID IS the blockchainID: canonical, on-chain, independently
// verifiable by ANYONE WITH A P-CHAIN ENDPOINT AND NOTHING ELSE, and immune to
// the whitespace and key-order sensitivity of hashing a JSON string someone
// re-serialized. No separator, no length prefix, no normalization.
//
// UPGRADE BYTES ARE NOT IN THE ANCHOR (amended 2026-08-05, user ruling), and
// the decisive fact is upstream: subnet-evm's Genesis.toBlock
// (core/genesis.go:314) calls ONLY ApplyPrecompileActivations, while STATE
// upgrades run through ApplyUpgrades from core/state_processor.go:84,
// miner/worker.go:232 and the tracer, every one of which takes a PARENT
// timestamp. Upgrades therefore apply inside a block and never to genesis
// ("In block processing and building, [ApplyUpgrades] is called instead which
// also applies state upgrades", core/state_processor_ext.go:25), so genesis
// state and the genesis block hash are a pure function of genesisData.
//
// The user's reasoning, verbatim (2026-08-05): correctness is enforced by the
// STATE ROOT, not by the anchor. A semantically wrong upgrade.json diverges at
// its activation height and the executor hard-stops, so a corpus built with the
// wrong file cannot exist. A file that differs only in FORMATTING produces
// byte-identical execution, so hashing it verbatim manufactures a divergence
// where there is none, and makes the anchor depend on an off-chain file we
// often cannot obtain. The residual case, two operators with different
// FUTURE-dated upgrades producing identical corpora today, resolves itself
// loudly when the timestamp arrives.
//
// upgrade.json is UNCHANGED in every other respect: still read verbatim at
// every start, still drives execution, still lives at <data>/upgrade.json.

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
// the embedded string IS the canonical genesisData.
func ChainRoot(networkID uint32) ([32]byte, error) {
	cfg := genesis.GetConfig(networkID)
	if cfg == nil {
		return [32]byte{}, fmt.Errorf("dist: no embedded genesis config for network %d", networkID)
	}
	return ChainRootFrom([]byte(cfg.CChainGenesis)), nil
}

// ChainRootFrom is the rule itself, in full: sha256(genesisData).
func ChainRootFrom(genesisData []byte) [32]byte {
	return sha256.Sum256(genesisData)
}

// CreateChainTx fetches a chain's canonical genesis bytes and its subnetID
// from a P-chain endpoint: `platform.getTx` on the blockchainID, which IS the
// CreateChainTx's txID. apiURL is a node base URL, e.g. https://api.avax.network.
//
// This is the L1 door for ruling 5 and nothing more: a subnet-evm chain has no
// embedded genesis anywhere, so this is where its root (and later its executor)
// gets the bytes, and it is also why a blockchainID is the only thing an
// operator has to type. The C-chain does not need it, see ChainRoot.
func CreateChainTx(ctx context.Context, apiURL, blockchainID string) (genesisData []byte, subnetID string, err error) {
	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"platform.getTx","params":{"txID":%q,"encoding":"json"}}`, blockchainID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL+"/ext/bc/P", bytes.NewReader([]byte(body)))
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("dist: platform.getTx %s: %w", blockchainID, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("dist: platform.getTx %s: %w", blockchainID, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("dist: platform.getTx %s: http %d", blockchainID, resp.StatusCode)
	}
	return parseCreateChainTx(raw)
}

// parseCreateChainTx pulls genesisData and subnetID out of a platform.getTx
// JSON response. Its own function so the FIFA fixture can exercise it with no
// network call.
func parseCreateChainTx(raw []byte) ([]byte, string, error) {
	var r struct {
		Result struct {
			Tx struct {
				UnsignedTx struct {
					// []byte through encoding/json is base64, which is what
					// the API emits for genesisData under encoding "json".
					GenesisData []byte `json:"genesisData"`
					SubnetID    string `json:"subnetID"`
				} `json:"unsignedTx"`
			} `json:"tx"`
		} `json:"result"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, "", fmt.Errorf("dist: platform.getTx response: %w", err)
	}
	if r.Error != nil {
		return nil, "", fmt.Errorf("dist: platform.getTx: %s", r.Error.Message)
	}
	u := r.Result.Tx.UnsignedTx
	if len(u.GenesisData) == 0 {
		return nil, "", fmt.Errorf("dist: platform.getTx returned no genesisData: not a CreateChainTx")
	}
	if u.SubnetID == "" {
		return nil, "", fmt.Errorf("dist: platform.getTx returned no subnetID: not a CreateChainTx")
	}
	return u.GenesisData, u.SubnetID, nil
}
