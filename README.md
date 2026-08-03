# epochdb

Super-compact Avalanche C-chain historical node: stores blocks, logs, and receipt fields in sealed epoch files, no database. Re-execution serves only debug traces, eth_createAccessList, and verification.

Differentiator: debug_getModifiedAccountsByNumber/ByHash works at ANY height straight from the per-block write capture; a normal node needs both boundary states live in its trie database.
Status: v1 measurement build, Fuji testnet.

## Running it

There is one command, `serve`. It follows the chain, executes it, indexes it, cuts and publishes epoch files, uploads them, and answers JSON-RPC, in one process that recovers by itself.

```
epochdb serve --data <dir> --network mainnet                       # one chain (default: that network's C-chain)
epochdb serve --data <dir> --network mainnet --chain <blockchainID> # an L1
epochdb serve --data <root> --chains <blockchainID>,<blockchainID>  # several L1s in one process
```

With `--chains`, each chain gets `<root>/<blockchainID>`, answers at `/ext/bc/<blockchainID>/rpc`, and `/status` reports all of them. A chain that cannot start does not take the process or its siblings down: it stops with a reason `/status` carries. `--verify` re-verifies the sealed epochs before serving them, and a chain that fails verification does not start.

Set `EPOCHDB_S3_*` and a fresh data dir joins the published chain by itself: there is no bootstrap step.

## Docker

Every commit on main is published to `ghcr.io/containerman17/epochdb` as `latest` and `sha-<shortsha>`; there are no tagged releases. The data dir is the only state, so mount it and pass `--data`:

```
docker run -v epochdb-data:/data -p 9650:9650 ghcr.io/containerman17/epochdb serve --data /data --network fuji
```
