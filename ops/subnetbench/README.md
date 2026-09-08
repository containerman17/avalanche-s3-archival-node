The `subnet-evm-pruned` command runs the pinned subnet-evm VM through
`rpcchainvm.Serve`. Its chain config selects Firewood or the local `pruned`
backend. The VM, execution, consensus hooks, block handling, and RPC remain
subnet-evm's implementations.

Prepare the pinned source patch from the repository root:

```sh
python3 ops/subnetbench/prepare.py --output /tmp/subnet-evm-pruned-source
. /tmp/subnet-evm-pruned-source/env.sh
go vet -tags subnetbench ./cmd/subnet-evm-pruned
go test -tags subnetbench ./cmd/subnet-evm-pruned
go run -tags subnetbench ./cmd/subnet-evm-pruned --version
```

The output directory must be new. Python 3, Go, and `patch` must be installed.
The generator checks the module version and each original file's SHA-256 before
applying the patches. It creates a private module cache, copies only the pinned
subnet-evm and libevm modules, and links other cache branches to the original
cache. It patches only the private copies. Both backends use those same sources
and command.
The original module cache and all `go.mod` files remain unchanged.

Go rejects source overlays below `GOMODCACHE`. A Go workspace also changes lazy
dependency loading and exposes conflicting genproto packages in this dependency
graph. The private cache preserves normal module resolution without adding
module replacements. Treat it as generated build input; regenerate it when the
patch changes.

Pass this config through the plugin host's normal chain config bytes:

```json
{
  "benchmark-state-backend": "epochdb",
  "state-scheme": "firewood",
  "pruning-enabled": true,
  "state-sync-enabled": false,
  "snapshot-cache": 0,
  "use-standalone-database": true,
  "database-type": "pebbledb",
  "database-config": "eyJzeW5jIjp0cnVlfQ==",
  "state-history": 32,
  "commit-interval": 31
}
```

Change only `benchmark-state-backend` to `firewood` for the baseline. That value
also defaults to `firewood` when the field is absent. The wrapper removes the
field before calling the stock VM's `Initialize`. It requires pruning, the
existing Firewood proposal lifecycle, and disabled state sync for both modes.
`state-history` controls bounded runtime retention. The Epoch state directory
is `epochdb` below subnet-evm's chain data directory.

The patch adds `core.RegisterTrieDBConstructor` and a shared recovery
interface. A nil constructor preserves the original backend selection.
Firewood retains its original constructor and proof handler. The replacement
backend skips Firewood's proof handler only when state sync is disabled.
Historical Firewood reconstruction and state sync are outside this integration.

The timed RPC set is `eth_getBalance`, `eth_getStorageAt`, and `eth_call`, all
at `latest`. Proof and debug APIs are excluded. In particular, the replacement
and Firewood currently return different unsupported errors for `eth_getProof`.

The `database-config` value is base64 for `{"sync":true}`. Both variants use
synced Pebble writes for block data and acceptance metadata. This adds write
latency to both measurements compared with subnet-evm's default standalone
Pebble configuration. Crash recovery checks kill the plugin after the fixed
height is accepted and its RPC reports that height. They do not test a kill
during an unfinished `Accept` call.

Run a fixed corpus through the same host and plugin for each backend:

```sh
go run ./cmd/epochdb-host --chain "$CHAIN_ID" --vm /path/to/subnet-evm-pruned \
  --data /path/to/variant-data --corpus /path/to/blocks.corpus \
  --config /path/to/variant-config.json --stop 100000 --queue-ahead 4096 \
  --node http://127.0.0.1:9650 --http 127.0.0.1:19900
```

Place the same cached `chain.json` and optional `upgrade.json` in both data
directories. Corpus mode opens no fetcher or follower. It streams local records
through the existing parse ring, reports queue wait time, and keeps the original
validator RPC context. The corpus begins with eight bytes `EPCORP01`; each record
contains a big-endian uint64 height, a big-endian uint32 size, and that many raw
container bytes. Heights start at one and must be contiguous. A container is
limited to 64 MiB. The exporter validates the complete file and records its hash
before timing. Queue wait remains part of the report; a starved run is invalid.

The host feeds no block after `--stop`. On restart it skips earlier records,
checks the plugin's accepted block against the corpus, and resumes at the next
height. A restart already at the stop height executes no additional block. The
`corpus ready` log appears after acceptance, preference, normal operation, and an
`eth_blockNumber` response at the stop height. RPC stays open until the host is
stopped. The publication build uses `subnetbench,netgo,osusergo` tags.
