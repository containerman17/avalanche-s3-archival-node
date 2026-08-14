# epochdb

An ultra-compact Avalanche archival node. One process serves one chain: it follows consensus,
executes every block, indexes it, cuts and publishes immutable epoch files, uploads them, and
answers JSON-RPC. It recovers by itself, and it is the sole writer of its data directory.

Sealed history is content-addressed and offloaded to any S3-compatible bucket, so the local disk is a
**cache of that bucket** rather than the only copy. A node with credentials and an empty directory
reconstructs a published chain on its own: there is no separate bootstrap command and no snapshot to
fetch out of band.

Status: measurement build. Fuji and a set of L1s run continuously; mainnet on the final format is in
progress. Numbers below are what we measured, and projections are labelled as projections.

[DESIGN.md](DESIGN.md) is the design of record: format, read path, verification, rulings. This file
is only how to run it.

## What it is not

A read-only archive. These are absences by construction, not a roadmap:

- **No mempool and no transaction submission.** `eth_sendRawTransaction`, `eth_sendTransaction`,
  `eth_fillTransaction` and `eth_resend` return a named error. There is no forwarding URL anywhere in
  the configuration, so relaying is not half-built, it is out of scope.
- **No keystore.** `eth_sign` and `eth_signTransaction` refuse. Sign client-side.
- **No proofs.** `eth_getProof`, `debug_dumpBlock`, `debug_accountRange`, `debug_storageRangeAt` and
  `debug_intermediateRoots` refuse because epochdb stores no per-block merkle tries. The merkle store
  it does keep is verify-only with zero readers, deliberately: the trie is what an archival node
  spends most of its disk on, and not keeping a queryable one is the entire point of the project.
- **No preimage table.** The state key space is already hashed, so `debug_preimage` has nothing to
  look a hash up in.
- **No node-ops surface.** The `personal_`, `miner_`, `admin_`, `les_`, `clique_` and `ethash_`
  namespaces refuse by namespace.

Every refusal is a named JSON-RPC error whose message says which class it falls into, so a caller can
tell a missing feature from a typo.

## Requirements

Two of these will surprise you. Both are hard.

- **The `zstd` CLI at exactly v1.5.7, on `PATH`.** Every sealed epoch carries a compression
  dictionary trained by the zstd CLI at seal time, and the artifact's bytes depend on the trainer, so
  a different version writes different bytes for the same blocks and breaks byte-identity between
  independent builders. `serve` checks this at startup and **refuses to start** if the binary is
  missing or the version is wrong. There is no opt-out flag and no environment override. The
  published image ships the right version, built statically; if you build from source, install v1.5.7
  yourself.
- **cgo, against glibc.** The merkle store (Firewood) ships a prebuilt static archive linked against
  glibc, so `CGO_ENABLED=1` is mandatory and the runtime image is `distroless/cc-debian13` rather
  than `distroless/static`, which cannot link it. A musl or fully static target will not build.
- Go 1.26 to build. Linux, x86-64, an NVMe, and enough RAM for the chain (see Sizing).
- Nothing else. No config file, no external database, no separate indexer process.

## Quick start: Docker

Every push to `main` publishes `ghcr.io/containerman17/epochdb` as `latest` and `sha-<shortsha>`.
There are no tagged releases; the per-commit image is the release channel. The image carries the
pinned zstd.

```sh
docker run -v epochdb-data:/data -p 9650:9650 ghcr.io/containerman17/epochdb \
  serve --data /data --network fuji
```

The data directory is the only state. One container per chain, which is how containers work anyway:
one chain's crash restarts one container, and testnet and mainnet mix freely on a box because the
network is a per-process flag.

**Always pass `--network mainnet` for a mainnet chain.** The default is `fuji`, and the mismatch
fails late, after work.

`serve` is the entire operator surface:

```
serve --data <dir>              data directory; ONE chain owns it (default ./data)
      --network fuji|mainnet    the network the chain lives on (default fuji)
      --chain C|<blockchainID>  C is --network's primary C-chain, anything else is an L1 (default C)
      --port 9650               HTTP listen port: JSON-RPC, plain HTTP at /v0/, /status
      --grpc-port 9660          gRPC listen port (0 disables)
      --vdr-sources <urls>      comma-separated P-chain RPCs for the cross-checked validator set
      --node <uri>              bootstrap RPC node URI (default: the public endpoint for the network)
      --state-cache <GiB>       executor read cache (default 1, clamped to 1/8 of the container's ceiling)
      --sync-every <dur>        bucket upload cadence (default 5m, no-op without S3)
      --per-peer <n>            max outstanding requests per archival peer (default 1)
      --tip-override <containerID>  fetch forward only up to this container and stop
                                there, instead of following the live tip
      --pprof <addr>            serve net/http/pprof on a separate address
```

Environment variables, all optional:

```
EPOCHDB_S3_ENDPOINT       scheme://host[:port]. Empty means fully local, no S3 at all.
EPOCHDB_S3_BUCKET         required when the endpoint is set
EPOCHDB_S3_ACCESS_KEY     static keys. Leave BOTH unset to use the AWS default credential
EPOCHDB_S3_SECRET_KEY     chain instead (env, shared config, SSO, instance role).
EPOCHDB_S3_PREFIX         key prefix, used verbatim
EPOCHDB_S3_REGION         defaults to "auto" (R2 wants that; MinIO ignores it)
EPOCHDB_CACHE_DIR         chunk cache root, default <data>/cache. The one thing several
                          chain processes on a box share.
EPOCHDB_CACHE_MIN_FREE    bytes of free space the cache stops filling at (default 5% of the fs)
EPOCHDB_CACHE_MAX_AGE     a cached chunk's ceiling age, Go duration (default 720h)
EPOCHDB_FETCH_CONCURRENCY parallel sub-range GETs per cold chunk; a property of your NIC
EPOCHDB_HEAVY_SLOTS       concurrent frontier builds allowed per box (default 1)
EPOCHDB_FIREWOOD_CACHE    bytes of Firewood node cache (default an eighth of the container's
                          own memory.max, or 3% of the Firewood dir when there is no
                          container). Raise it when `exec: split` shows fw dominating the
                          wall clock and the container has headroom. A value below the
                          64MB floor, or one with a size suffix, refuses to start.
EPOCHDB_FLAT_CACHE        bytes of the flat latest-state cache: the in-RAM account/slot map
                          that serves reads at the head without touching the SSTs (default
                          an eighth of the container's own memory.max, or 1GB when there is
                          no container). 0 turns it off; it is a pure read cache, so the
                          only thing it can cost is speed.
EPOCHDB_CODE_CACHE        bytes of the contract-code cache: an LRU of code blobs by hash in
                          front of the code descent (default a tenth of EPOCHDB_FLAT_CACHE's
                          budget). Bytecode is immutable by hash, so this cache has nothing
                          to invalidate. 0 turns it off.
EPOCHDB_NEW_CHAIN         set to 1 only to start a NEW chain in a prefix that already holds
                          other chains' objects. See "Joining a published chain".
GOMEMLIMIT                honoured if you set it; otherwise derived as 7/10 of the container ceiling
```

A minimal two-chain compose file. The shared cache is one host directory, so the containers **must
run as the same uid**, or one leaves files its siblings cannot evict:

```yaml
services:
  mainnet-c:
    image: ghcr.io/containerman17/epochdb
    command: serve --data /data --network mainnet --port 9650
    environment: [EPOCHDB_CACHE_DIR=/cache]
    volumes: ["c-data:/data", "cache:/cache"]
    ports: ["9650:9650"]
    cgroup_parent: epochdb.slice
    mem_limit: 24g
    restart: unless-stopped
  some-l1:
    image: ghcr.io/containerman17/epochdb
    command: serve --data /data --network mainnet --chain <blockchainID> --port 9650
    environment: [EPOCHDB_CACHE_DIR=/cache]
    volumes: ["l1-data:/data", "cache:/cache"]
    ports: ["9651:9650"]
    cgroup_parent: epochdb.slice
    mem_limit: 6g
    restart: unless-stopped
volumes: {c-data: {}, l1-data: {}, cache: {}}
```

`cgroup_parent` is explained under "Operating many chains on one box". Drop it and `mem_limit` if you
are running one chain on a dedicated box.

## Quick start: from source

```sh
git clone https://github.com/containerman17/epochdb && cd epochdb
zstd --version          # must print v1.5.7
CGO_ENABLED=1 go run ./cmd/epochdb serve --data ./data --network fuji
```

That is the whole build. `go run ./cmd/epochdb` with no arguments prints the usage.

## S3 is optional

With no `EPOCHDB_S3_ENDPOINT` there is no object store at all: the spool directory *is* the node's
storage, uploads are a no-op, and reads mmap the local file. **The read path is unchanged**, so a
fully local node serves exactly what a tiered one serves.

The one thing you lose is joining a published corpus, because the join needs a pointer to walk. A
credential-less node either syncs the chain itself from the network or is seeded by copying a data
directory.

Credentials can also arrive later, which is how a chain synced on one box is handed to another: a
node that sealed offline still recorded its `latest` pointer, so restarting that same directory with
credentials publishes the artifacts **and** the pointer on the next sync, with nothing re-sealed.

The client hand-rolls SigV4 with path-style addressing and accepts `auto` as a region, so it is not
AWS-specific: Cloudflare R2, MinIO, DigitalOcean Spaces, Backblaze B2 and Hetzner object storage are
the same code path, and for real external traffic their egress pricing is the reason to prefer them.
Still unexercised at scale on the S3 path: R2 with the default credential chain, provider throttling,
and cold ranged-GET latency under load.

## Joining a published chain

Point an empty data directory at a bucket that already holds a chain, and start `serve`. It resolves
the `latest` pointer, walks the epoch hash chain backward confirming every linked artifact is
available and contiguous and that the first epoch links to this chain's root, merges every epoch's
state section into a local frontier at the last sealed block, and starts executing forward from
there. Anything missing or inconsistent is a hard error that names the artifact and refuses to start.
It never silently falls back to re-executing from genesis, because a typo'd `EPOCHDB_S3_PREFIX`
quietly doing that is the most expensive failure in the system.

A *missing* pointer is refused on the same grounds. Content is named by its hash and says nothing
about which chain it belongs to, but a prefix that holds objects is a prefix somebody has published
to, so no pointer over one is a broken publish, not a fresh chain, and the node says so and stops.
An **empty** prefix is the first publisher and starts with no friction at all: that check is one
list request, and there is nothing to set. The one case the node cannot tell apart is a genuinely
new chain going into a prefix that other chains already publish into; start that one with
`EPOCHDB_NEW_CHAIN=1`, and unset it once the chain has sealed its first epoch.

Two measured joins, each on a machine that had never seen the chain:

- **Fuji C-chain**, 20 epochs (a 112.4 GB published corpus): pointer walk about a second, then
  269,929,372 rows merged into 6,776,146 accounts and 245,517,764 slots, root verified against the
  sealed header at block 55,462,597, in **35m08s** at ~15 GB peak. It then executed 2,076,363 blocks
  forward at ~1,640 blk/s to the live tip, and answered `eth_blockNumber` byte-identically to
  `api.avax-test.network`.
- **A small L1**, 7 epochs: 25,073,821 rows into 2,787,236 accounts and 22,286,282 slots, frontier
  built and root-verified at block 3,742,654, in **4m16s** (~97.8k rows/s), zero chunk rejections.

## Entry points

One process, four surfaces over one core query layer, none stacked on another:

- **The Go library.** `epochdb.Open(ctx, epochdb.Config{...})` starts the same full node `serve`
  runs and hands back `Head`, `StateAt`, `CallAt`, `TraceTx`, `Core()` and `Store()`, with nothing
  serialized on the way. `serve` is a thin main over it.
- **gRPC** on `--grpc-port`, the primary remote API, and it covers the whole capability set:
  `GetHead`, `StreamHeads`, `GetNodeInfo`, `GetBlock`, `GetTransaction`, `GetState`, `Call`,
  `EstimateGas`, `Trace`, `GetLogs`, `StreamLogs`, `StreamTransactions`,
  `SearchTransactionsByAddress`, `GetContractCreator`, `GetFeeHistory`, `GetGasPrice`. Streams
  replace the install-and-poll filter model, and `GetNodeInfo` publishes every capability this node
  refuses by design, with the reason. Proto and generated code in `grpcapi/`.
- **Plain HTTP** at `/v0/<method>`: the browser-debuggable subset (`head`, `getBlock`,
  `getTransaction`, `call`, `getState`, `searchTransactionsByAddress`, `getLogs`), parameters in any
  form, JSON out.

  ```
  curl 'localhost:9650/v0/getBlock?number=1036467'
  curl -d 'address=0xEAaB...&limit=5' localhost:9650/v0/searchTransactionsByAddress
  curl -H 'Content-Type: application/json' -d '{"to":"0xEAaB...","data":"0x18160ddd"}' localhost:9650/v0/call
  ```

- **JSON-RPC** at `/` and `/ext/bc/<blockchainID>/rpc`, below.

## RPC

Effectively full coreth/subnet-evm parity across the `eth_`, `debug_`, `net_`, `web3_` and `txpool_`
namespaces (78 methods), minus the refusal classes above; filters and WebSocket subscriptions are
real and fire. Historical `eth_call`, `eth_getBalance`, `eth_getCode` and `eth_getStorageAt` work at
**any** height, per block, with no snapshot interval and no pruning floor. The chain answers on one
port at `/`, at `/ext/bc/<blockchainID>/rpc` and at `/ext/bc/<blockchainID>/ws`.

**JSON-RPC batching works**, with the standard semantics: an array of N requests gets an array of N
responses each carrying its own id, an element with no id is a notification and gets no entry, a
batch of nothing but notifications gets an empty 200, an empty array is `-32600`, and one malformed
element errors on its own rather than failing the batch. Elements run sequentially in one goroutine,
so the head can move between them (as on any node) but no single element sees torn state. Two caps,
both refused by name rather than silently truncated: **1000 requests per batch** and a **10 MiB**
body, which are geth's own defaults, so tooling calibrated against a stock node needs no tuning.

`eth_getLogs` takes the `blockHash` filter form (logs of exactly that block), mutually exclusive with
`fromBlock`/`toBlock`; an unknown hash is an error naming it rather than an empty array, since `[]`
would say the block exists and has no matching logs. A stored filter (`eth_newFilter`) still refuses
`blockHash`, because a filter is a moving range.

The one shape that differs from a stock node: `newPendingTransactions` subscriptions and
`eth_newPendingTransactionFilter` install and then never fire, which is correct rather than broken:
with no mempool, nothing is ever pending.

`debug_getModifiedAccountsByNumber`/`ByHash` work at any height straight from the per-block write
capture, where a normal node needs both boundary states live in its trie database. Caveats: the range
is capped at 10k blocks, results are in capture order rather than hash order, a value rewritten to
its original still counts, and heights whose raw writelogs have been sealed away return a clean
error.

## Sizing

**A serving node.** Local disk is the merkle store (Firewood) plus the unsealed tail plus warm
indexes plus whatever chunk cache you allow; the sealed corpus lives in the bucket and the cache
windows over it. Measured on mainnet C-chain at block 10,000,000: Firewood 20.7 GB, against 2.82 GB
of raw keys and values, i.e. 86.4% merkle overhead. Full mainnet is a **projection, not a
measurement**: Firewood 130-157 GB, ~14.5 GB of unsealed tail, ~7.7 GB of warm indexes, and a
published bucket tier of 500-690 GB. For a real datapoint at real scale, Fuji's 20-epoch published
corpus (55.4M blocks) is 112.4 GB in the bucket.

Sealed epochs compress **roughly 3x to 6.9x** whole-epoch, and this is a range rather than a headline
number because it varies with chain density and era. The low end (~3.0-3.6x) is sparse chains and
early small epochs; the high end is transaction-dense L1s filling whole 8M-transaction epochs, where
the per-epoch trained dictionary has a lot of repetition to learn from (measured on one such chain:
10,359.6 MB down to 1,594.2 MB, 6.50x, and 9,903.4 MB down to 1,530.9 MB, 6.47x). Mainnet C-chain
sits in the middle at 4.2-4.6x. The dictionary is worth roughly a quarter on its own: on one mainnet
epoch, 4.62x with it against 3.72x without. Sealed cost lands at 409-592 bytes per transaction
depending on era.

Epoch size is a pure function of the epoch index (`min(8M, 250k << i)` transactions) so that every
honest builder cuts byte-identical boundaries with no configuration. There is no size knob.

**Memory, and this is the operational lesson worth paying attention to.** A chain's RAM ceiling
should scale with its **unsealed (staged) block count**, not with how much history it has published.
Every process that opens the store builds a RAM index entry per block per raw family over the whole
unsealed tail, measured at ~182 bytes per block. A sparse L1 with cheap blocks sitting near the
8M-transaction epoch ceiling therefore carries ~1.2 GB of index for a corpus that looks tiny on disk.
**Small chains are not free.** On top of that: Firewood's cgo node cache (3% of its directory,
clamped to 64 MB..4 GB), the executor state cache (capped at 1/8 of the container ceiling), the blooms on
the Go heap, and a Go runtime baseline. Note the shipped grants derive from the container's own
`memory.max`, not from block count, so the block-count rule is how you should *choose* that ceiling.

The largest memory event is not steady state at all: it is the frontier build during a join, measured
at **21.54 GiB peak** on a six-epoch Fuji corpus. Size for it, or gate it (below).

## Operating many chains on one box

Several chains are several processes, one container each, sharing one `EPOCHDB_CACHE_DIR`. Nothing
coordinates them: the chunk cache's LRU is written in the directory tree rather than in any process's
memory, so they share it without agreeing about anything, and the worst a race costs is one refetch.

The rest of this was learned expensively, from two outages of one box in a single day:

- **Per-container memory limits bound one chain and say nothing about the sum.** A box running 22
  chains had every container under its own `mem_limit` and zero container OOM kills, and still
  starved to the point where sshd accepted TCP and could not send a banner. The per-container
  ceilings were deliberately oversubscribed (~130 GB of ceilings on a 61 GiB box) because a ceiling
  is not a reservation, and that is the right call for isolation. It is simply not a budget.
- **A whole-box budget is what keeps the machine responsive.** We use a systemd slice
  ([`ops/epochdb.slice`](ops/epochdb.slice); its header carries the install and verification steps):
  `MemoryHigh` at ~70% of RAM, `MemoryMax` at ~85%, `CPUWeight=50`, with every container joined to it
  via `cgroup_parent`. Past `MemoryHigh` the kernel reclaims the slice's own clean pages, so the
  fleet degrades by slowing down instead of dying, and anything outside the slice (sshd included) is
  never squeezed. The requirement is not "no chain dies", it is "sshd always runs".
- **Gate the frontier builds.** A stop/start that wipes an instance store makes every chain try to
  join at once, and each join is a 20+ GiB memory event. `EPOCHDB_HEAVY_SLOTS` (default 1) is a
  cross-process `flock` in the shared cache directory that admits one build at a time per box. A
  waiting chain has already bound its port, so its requests sit in the accept backlog.
- Containers sharing the cache directory **must run as the same uid**.

## What to watch

`GET /status` on the chain's own port returns one JSON object for that one process:

`chain`, `serving`, `accepted` (the follower's head), `executed`, `cooked`, and the chunk cache's own
account of itself: `cacheHorizon` (the age of the window it last evicted, i.e. how long a chunk
really survives on this node), `cacheEvictions`, `cacheRefusals`, `cacheFreeBytes`,
`cacheMinFreeBytes`, `cacheFillErrors`, `cacheEvictErrors`, `cacheReadErrors`, `cacheLastError`.
`fetched` is the highest block the ascending fetch has verified, and `queueBytes`/`peakQueueBytes`
are the RAM those fetched-not-executed containers hold: nothing pre-execution touches disk, so that
is the whole cost of running ahead of the executor. Under `--tip-override` there is no follower, so
`target` replaces `accepted`.

The listener binds **before** the startup work, which on a large corpus can take over an hour, so
`/status` answers with `serving:false` from the first second and you can watch a cold start rather
than guess at it.

There is no aggregate health endpoint and there will not be one: a rollup over the chains of a box is
a rollup over containers, which your load balancer or scrape config already does better.

**A failure exits nonzero.** A start that cannot proceed exits 1 with a reason; a runtime failure
flushes the writers first, so the restart is a clean resume rather than a crash walk-back, then exits
1. Restarting is your supervisor's job (`restart: unless-stopped` is enough). Nothing retries
forever, and nothing degrades into serving wrong answers.

For a fleet, watch the slice rather than the individual processes:
`/sys/fs/cgroup/epochdb.slice/memory.current` against `memory.high`; `memory.events`, where a rising
`high` means the budget is holding and any `oom_kill` means it is too tight for what is running;
`memory.pressure`'s `some avg60` for the throttle band; and `free -g`, whose `available` is the sshd
reserve and must never approach zero.

## License

MIT. See [LICENSE](LICENSE).
