# epochdb

Super-compact Avalanche C-chain historical node: stores blocks, logs, and receipt fields in sealed epoch files, no database. Re-execution serves only debug traces, eth_createAccessList, and verification.

Differentiator: debug_getModifiedAccountsByNumber/ByHash works at ANY height straight from the per-block write capture; a normal node needs both boundary states live in its trie database.
Status: v1 measurement build, Fuji testnet.

## Running it

There is one command, `serve`, and it hosts ONE chain. It follows the chain, executes it, indexes it, cuts and publishes epoch files, uploads them, and answers JSON-RPC, in one process that recovers by itself.

```
epochdb serve --data <dir> --network mainnet                        # that network's C-chain
epochdb serve --data <dir> --network mainnet --chain <blockchainID> # an L1
```

The chain answers at `/` and at `/ext/bc/<blockchainID>/rpc`, and `/status` reports it. Several chains on one box are several processes: a process that cannot start its chain exits nonzero, and restarting it is your supervisor's job. `--verify` re-verifies the sealed epochs before serving them, and a failure means the node does not start.

Set `EPOCHDB_S3_*` and a fresh data dir joins the published chain by itself: there is no bootstrap step.

`EPOCHDB_CACHE_DIR` is the one thing several chain processes share: point them all at one directory and they share one elastic chunk cache (default `<data>/cache`). No coordination is involved, whoever needs the disk takes it, and a chunk another process evicted costs a refetch.

## Docker

Every commit on main is published to `ghcr.io/containerman17/epochdb` as `latest` and `sha-<shortsha>`; there are no tagged releases. The data dir is the only state, so mount it and pass `--data`:

```
docker run -v epochdb-data:/data -p 9650:9650 ghcr.io/containerman17/epochdb serve --data /data --network fuji
```

ONE CONTAINER PER CHAIN, which is how containers work anyway: one chain's crash restarts one container, and testnet and mainnet mix freely because the network is a per-process flag. Two chains sharing a cache:

```yaml
services:
  c-chain:
    image: ghcr.io/containerman17/epochdb
    command: serve --data /data --network mainnet --port 9650
    environment: [EPOCHDB_CACHE_DIR=/cache]
    volumes: ["c-data:/data", "cache:/cache"]
    ports: ["9650:9650"]
    restart: unless-stopped
  fifa:
    image: ghcr.io/containerman17/epochdb
    command: serve --data /data --network mainnet --chain SUDoK9P89PCcguskyof41fZexw7U3zubDP2DZpGf3HbFWwJ4E --port 9650
    environment: [EPOCHDB_CACHE_DIR=/cache]
    volumes: ["fifa-data:/data", "cache:/cache"]
    ports: ["9651:9650"]
    restart: unless-stopped
volumes: {c-data: {}, fifa-data: {}, cache: {}}
```

The cache mount is the same directory in every container, so THEY MUST AGREE ON UID: run them all as the same user (add `user: "1000:1000"` to each service, or leave them all at the image default). A container that writes the shared cache as a different uid leaves files its siblings cannot evict.
