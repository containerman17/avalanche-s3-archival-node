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
`eth_blockNumber` response at the stop height. It includes the host PID, and the
measurement script requires it to match systemd `MainPID`, so stale ready lines
from an earlier run cannot satisfy restart readiness. RPC stays open until the
host is stopped. The publication build uses `subnetbench,netgo,osusergo` tags.

## Current-state RPC

Generate the request file on baseline A at the fixed accepted height, before
the process restart used for the cold pass:

```sh
python3 ops/subnetbench/rpcbench.py generate --url "$RPC_A" --height 100000 \
  --seed 17 --output /path/to/requests.jsonl
```

The default selection samples 128 blocks from the last 10,000 blocks. It uses
transaction senders, targets, and observed transaction inputs from those blocks.
Those block reads select inputs; every state read and call uses `latest`.
Selection uses a 100-second phase budget and a five-second HTTP timeout. The
seed fixes sampling and request order for the probes completed within that
budget. Machine speed can change which probes finish, so generate once and
reuse the saved file for all passes and both backends.

For sampled contracts with current code, storage selection probes direct slots
0 through 31 and candidate balance mapping positions for four sampled addresses
and slots 0 through 7. It computes mapping keys with `web3_sha3`. Selection must
find populated storage; it then draws approximately 70% of storage requests from
populated slots and 30% from zero slots when both are available. Call selection
keeps successful current calls using observed transaction inputs and candidate
`balanceOf(address)`, `totalSupply()`, and `decimals()` inputs, with a gas limit of
2,000,000. These are sampled candidates, not traced storage accesses or verified
token interfaces. Metadata records successful empty call results separately.

The output contains exactly 1,000 requests per method, shuffled together. Small
pools require repeated requests. The `.meta.json` sidecar records the chain ID,
head hash, request file SHA-256, actual sampled heights, contracts, probe counts,
successful storage and call probes with their results, nonzero and zero storage
counts, and unique request counts. Copy the request file and its sidecar together.
Every command refuses to overwrite an existing output file.

After restarting A and waiting for readiness at the same height, run the cold
pass first and then the warm pass:

```sh
python3 ops/subnetbench/rpcbench.py replay --url "$RPC_A" \
  --requests /path/to/requests.jsonl --output /path/to/a-cold.jsonl --label a-cold
python3 ops/subnetbench/rpcbench.py replay --url "$RPC_A" \
  --requests /path/to/requests.jsonl --output /path/to/a-warm.jsonl --label a-warm
```

Repeat those two commands for B, changing only the URL, output paths, and labels.
Cold means the first workload pass after a process restart. Requests within that
pass can warm caches, and the operating system cache is not cleared. Generation
is outside the measurement and occurs before the restart.

Replay uses one sequential HTTP connection with keepalive. Each JSONL record
contains the method, parameters, complete response body, parsed response, HTTP
status, and latency in nanoseconds. Timing starts before the HTTP request and
ends after reading the complete body; JSON encoding and decoding are excluded.
Replay verifies the chain ID, head hash, and height before and after each pass.

```sh
python3 ops/subnetbench/rpcbench.py compare /path/to/a-cold.jsonl /path/to/b-cold.jsonl
python3 ops/subnetbench/rpcbench.py compare /path/to/a-warm.jsonl /path/to/b-warm.jsonl
python3 ops/subnetbench/rpcbench.py summarize /path/to/a-cold.jsonl
python3 -m unittest discover -s ops/subnetbench -p test_rpcbench.py
```

Also compare cold and warm responses within each backend. Comparison ignores
only the top-level JSON-RPC ID and dictionary key order. Errors, missing results,
different responses, changed heads, or failed file hashes fail validation.
Reports include per-method success and error counts, p50 and p95 latency for
successful requests, and nonzero and zero storage response counts. The optional
`--output` argument on `compare` and `summarize` saves the report as JSON.
