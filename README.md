# epochdb

Super-compact Avalanche C-chain historical node: stores blocks, logs, and receipt fields in sealed epoch files, no database. Re-execution serves only debug traces, eth_createAccessList, and verification.

Differentiator: debug_getModifiedAccountsByNumber/ByHash works at ANY height straight from the per-block write capture; a normal node needs both boundary states live in its trie database.
Status: v1 measurement build, Fuji testnet.

## Docker

Every commit on main is published to `ghcr.io/containerman17/epochdb` as `latest` and `sha-<shortsha>`; there are no tagged releases. The data dir is the only state, so mount it and pass `--data`:

```
docker run -v epochdb-data:/data -p 9650:9650 ghcr.io/containerman17/epochdb serve --data /data --network fuji
```
