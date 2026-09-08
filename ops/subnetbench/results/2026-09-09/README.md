# Pruned Firewood replacement inside subnet-evm

Run in progress. Results will be filled after both backends pass the full replay,
restart, and current-state RPC comparisons. This run supersedes the discarded
archival-node comparison; none of those old measurements are used here.

## Scope and fixed inputs

Both variants run the same `subnet-evm-pruned` binary under the same `epochdb-host`.
Only `benchmark-state-backend` changes between `firewood` and `epochdb`. EVM
execution, validation, block storage, and RPC are subnet-evm implementations.
Both prune state, retain 32 recent runtime revisions, disable state sync and
snapshots, and use the same synced standalone Pebble configuration. Block history
is shared VM behavior; the state backend keeps no archival state history.

The Firewood full run started at 2026-09-09 05:28:33 JST. The replacement
full run started at 05:50:16 JST on the same day.

The dedicated runner is i7i.2xlarge, 8 vCPU, 64 GiB, local NVMe mounted as XFS at
`/data`, instance `i-09b1ba1bd3d047f8f`. One contender runs at a time. The existing
fleet is untouched. See `runner-environment.txt` for the hardware and binary
fingerprints. All human-readable times in this report use JST; raw host logs use
UTC, nine hours behind JST.

Beam blocks 1..1,000,000 come from one validated local corpus. The host opens no
network block fetcher. Its ring holds 4,096 containers and parses batches of 256.
The corpus is read once before each timed run to warm the input file cache. Fresh
contender directories receive identical `chain.json` and `upgrade.json` files.
`corpus.json` records the full input hash, gas, transactions, and terminal block.

- Corpus SHA-256: `f0b7add5aab3c819ce54f72d3e13767d276d3c9bc234e78551530fa59eb45d5c`.
- Corpus: 2,584,588,557 bytes, 1,000,000 blocks, 1,358,991 transactions,
  69,587,362,119 gas.
- Expected block hash: `0xd53a08bdc947113309bfd066a9f76faccfa5764109518bf247be8cc7c95bf7a5`.
- Expected state root: `0xc0df2d7718e6047532228276d4f2515fd5310b47a43b6af79b3d88273b80c74e`.

## Build provenance

The backend and plugin source is commit `2b7c4a8`; the measured host adds only
the process-specific readiness log in `4d00ac9`. Recovery documentation and the
additional alias test are in `ebf8d9e`. The fixed RPC workload tool is `076bff5`; the replay report tool is `a50fa43`.

Pinned avalanchego, graft/subnet-evm, and graft/evm:
`v1.14.3-0.20260804141953-6dc4c3b395b6`. Effective libevm is the repository's
existing replacement `github.com/containerman17/libevm`
`v1.13.15-0.20260817022927-4c8a6553b55f`. Firewood FFI is `v0.8.0`.
`prepare.py` checks exact original source hashes and applies the checked-in
subnet-evm and libevm patches inside a private module cache. Both variants use
those same patched dependencies. There is no new go.mod replacement.

Measured binary SHA-256:

| Binary | SHA-256 |
| --- | --- |
| subnet-evm-pruned | b4244115d55ff757feafc078c29d60c2225ad4449e39e78ebfc19026e6fe9aea |
| epochdb-host | 30a2e438a3b185df70ba1003c481db6ce4883a1527cb4c3e6937e663ceb0b2c9 |
| subnetbench-corpus | 743402123f753c3b0cd3b065d9d41f9080dbf86c7eb785e99661e42c1c00f416 |

Publication build after source preparation:

```sh
python3 ops/subnetbench/prepare.py --output /tmp/subnetbench-source
. /tmp/subnetbench-source/env.sh
CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build \
  -tags subnetbench,netgo,osusergo \
  -ldflags '-linkmode external -extldflags "-static"' \
  -o /path/to/subnet-evm-pruned ./cmd/subnet-evm-pruned
# Use the same command for ./cmd/epochdb-host and ./cmd/subnetbench-corpus.
```

## Corpus reproduction

Export the immutable local source before either timed run:

```sh
/data/bench/bin/subnetbench-corpus --source /data/bench/archive/data \
  --manifest /data/bench/beam-archive-manifest.json --version 4 --stop 1000000 \
  --output /data/bench/input/beam-1m.corpus
```

The export validates consecutive heights, parent links, and container round trips.
The exact corpus is also retained locally as
`/home/ilia/.herdr/worktrees/epochdb/subnet-evm-pruned/.audit/input/beam-1m.corpus.zst`.
Decompressing that copy reproduces the SHA-256 in `corpus.json`. The large corpus
and built binaries are local artifacts and are not stored in git.

## Run and measurement procedure

Each command below is an individual step. Stop and inspect its results before
the next step. Select `variant=firewood` first, then `variant=epochdb` after all
Firewood reads finish. Use the committed config of the same name.

```sh
variant=firewood
chain_id=2tmrrBo1Lgt1mzzvPSFt73kkQKFas5d1AP88tv9cicwoFp8BSn
rpc=http://127.0.0.1:19900/ext/bc/$chain_id/rpc
mkdir /data/bench/$variant
cp /data/bench/seed/chain.json /data/bench/seed/upgrade.json /data/bench/$variant/
cat /data/bench/input/beam-1m.corpus >/dev/null
sudo systemd-run --unit=subnetbench-$variant --uid=ec2-user --gid=ec2-user \
  -p MemoryMax=48G -p RuntimeMaxSec=10800 \
  -p StandardOutput=append:/data/bench/logs/$variant.log \
  -p StandardError=append:/data/bench/logs/$variant.log \
  /data/bench/bin/epochdb-host --chain "$chain_id" --network mainnet \
  --vm /data/bench/bin/subnet-evm-pruned --data /data/bench/$variant \
  --corpus /data/bench/input/beam-1m.corpus --config /data/bench/$variant.json \
  --stop 1000000 --queue-ahead 4096 --batch 256 \
  --node https://api.avax.network --http 127.0.0.1:19900
python3 /data/bench/bin/measure.py --unit subnetbench-$variant.service \
  --log /data/bench/logs/$variant.log --data /data/bench/$variant \
  --rpc "$rpc" --height 1000000 --output /data/bench/results/$variant.jsonl \
  --timeout 10800 --settle 10
```

Wall time begins at systemd's monotonic process start and ends after the current
host logs readiness and an external RPC returns exactly N. Readiness follows VM
acceptance completion, preference, NormalOp, and RPC height verification. No
block above N is submitted. RAM samples sum host and plugin resident memory once
per second. The sum of per-process high-water marks is a separate upper bound,
because those peaks need not be simultaneous. Settled RSS and disk are collected
10 seconds after ready. Disk includes the recovery journal, and separates backend
files from the common block database. Apparent file size and allocated bytes are
both retained; Pebble WAL preallocation can make those differ substantially.

The host emits gas rates every 10 seconds. The median uses complete windows and
excludes the startup window and final partial exit window. Cumulative wall gas
rate uses corpus gas divided by measured process-to-ready time. Input waiting is
reported at the host log's 0.1-second precision; a printed zero does not establish
an exact zero wait.

Generate the RPC request file on Firewood at N before its restart, using the
committed `rpcbench.py generate` defaults and seed 17. Then kill the whole host
and plugin cgroup with SIGKILL. Restart the same command and directory with a new
unit name and fresh log path, and measure readiness to N again. A correct restart
at N executes zero additional blocks. Run the frozen 3,000-request file once for
the first pass after restart and immediately again for the warm pass. Repeat on
the replacement, reusing the same request file and metadata. Compare answers
across both backends and across both passes, ignoring only the JSON-RPC response
ID and object key order. The RPC tool refuses errors or any changed head.

## Selected current-state workload

The frozen request SHA-256 is
`87c5254739ece169c25071956a32d99f0d9cf0176312cec39bee8eedd7e43a56`.
Generation scanned 128 blocks in 990,001..1,000,000 and found eight code-bearing
contracts. The 1,000 balance requests cover 123 unique addresses. Storage probes
found 14 populated and 498 empty slots; the frozen storage requests cover 245
unique address/slot pairs and return 699 nonzero and 301 zero values on Firewood.
The 1,000 calls draw from 13 unique successful inputs: seven observed transaction
inputs, four balanceOf candidates, one decimals candidate, and one totalSupply
candidate. Five of those 13 inputs return empty bytes. Selection rejected 79
reverting calls before freezing the workload. Actual replay has zero RPC errors.

This is a small working set with repeated requests. First-pass latency includes
cache warming within the pass, and the call sample is not a broad contract
workload. `requests.jsonl.meta.json` records all selected probes and their results.

## Checks and limits

Repository vet and tests with the prepared source cache and `subnetbench` tag
passed, as did pruned race tests and the commitment/StateDB oracle tests. Checks
cover pending and rejected branches, duplicate-root block aliases, bounded
retention, deletion and recreation through Finalise and snapshot reverts,
checkpoint rotation, journal corruption handling, and process-kill recovery.
Both actual subnet-evm variants passed the 3,000-block smoke and SIGKILL restart
with identical terminal hashes and roots. Smoke files are retained as gates;
smoke timings are not used in the full-run comparison.

This is a fixed local replay and current-state RPC comparison. It does not test
live consensus, state sync, archival state, debug APIs, or proof APIs. The
replacement does not implement state proofs and currently returns a different
unsupported error from Firewood for eth_getProof. VM execution is serialized by
the common host, as in the VM lifecycle exercised here. Arbitrary overlapping
StateDB commits are outside the test.

The restart test kills processes only after the fixed accepted ready point. It
does not claim recovery from an arbitrary unfinished Accept call or power loss.
Both variants explicitly sync Pebble writes, which changes their absolute
throughput from the default standalone Pebble configuration. The first RPC pass
is cold only with respect to process restart; the operating system cache stays
warm, and repeated requests warm process caches within that pass.

The host's public validator API returns an unsupported-height warning during
background validator-set refresh. Both backends share this path. Those warnings
do not interrupt block replay or current-state RPC, and are retained in raw logs.
