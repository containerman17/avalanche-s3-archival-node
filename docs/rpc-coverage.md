# RPC coverage matrix vs a normal Avalanche C-chain node

Authoritative source: coreth v1.14.2 registered namespaces (`internal/ethapi/api.go`,
`eth/filters/api.go`, `node/api.go`, `eth/api_admin.go`, `eth/api_debug.go`,
`eth/backend.go` namespace table): eth, web3, net, txpool, personal, admin, debug.
Status values: DONE (served, A/B-verified where the remote can answer),
TODO(phase1) = this phase, TODO(other) = other session's lane,
PHASE3 = documented error until live-tip/mempool work lands,
N/A = unsupported by design with the reason.

## eth

| method | status | notes |
|---|---|---|
| eth_chainId | DONE | |
| eth_blockNumber | DONE | = readable head |
| eth_syncing | DONE | returns false (geth-compatible); coreth itself errors "not implemented in coreth", tolerance documented |
| eth_getBalance | DONE | |
| eth_getTransactionCount | DONE | |
| eth_getCode | DONE | |
| eth_getStorageAt | DONE | |
| eth_call | DONE | |
| eth_estimateGas | DONE | |
| eth_createAccessList | DONE | geth fixed-point AccessListTracer loop |
| eth_etherbase | DONE | = eth_coinbase (coreth alias); public API refuses it (-32601, internal-eth namespace off), value transitively A/B-verified via eth_coinbase |
| eth_getBlockByNumber / ByHash | DONE | |
| eth_getHeaderByNumber / ByHash | DONE | header JSON; public API does not expose these (-32601), so shape-verified against our getBlockByNumber |
| eth_getBlockTransactionCountByNumber / ByHash | DONE | |
| eth_getBlockReceipts | DONE | stored epoch sections, no re-execution |
| eth_getTransactionByHash | DONE | |
| eth_getTransactionReceipt | DONE | |
| eth_getTransactionByBlockNumberAndIndex / BlockHashAndIndex | DONE | |
| eth_getRawTransactionByHash | DONE | tx.MarshalBinary |
| eth_getRawTransactionByBlockNumberAndIndex / BlockHashAndIndex | DONE | |
| eth_getLogs | DONE | 10k range cap (public API caps at 2048) |
| eth_gasPrice | DONE | |
| eth_baseFee | DONE | avalanche-specific; head base fee (pre-AP3 corpus: 0x0) |
| eth_maxPriorityFeePerGas | DONE | |
| eth_feeHistory | DONE | |
| eth_blobBaseFee | N/A | not served by coreth either; no blobs on the C-chain |
| eth_newFilter | DONE | in-memory registry |
| eth_newBlockFilter | DONE | |
| eth_newPendingTransactionFilter | DONE | no pool: always empty changes |
| eth_getFilterChanges | DONE | static corpus: empty after first poll is correct |
| eth_getFilterLogs | DONE | full query, same engine as eth_getLogs |
| eth_uninstallFilter | DONE | |
| eth_subscribe / eth_unsubscribe | DONE | WS; newHeads/logs/newPendingTransactions accepted, silent on a static corpus, live in phase 3 |
| eth_sendRawTransaction | PHASE3 | documented error: no mempool/relay yet |
| eth_sendTransaction | PHASE3 | also needs accounts; error |
| eth_fillTransaction / eth_resend | PHASE3 | mempool-adjacent, error |
| eth_pendingTransactions | DONE | empty list (no pool) |
| eth_accounts | DONE | [] (no keystore) |
| eth_coinbase | DONE | coreth's fixed blackhole 0x0100..00, A/B-verified |
| eth_sign / eth_signTransaction | N/A | no keystore by design |
| eth_getProof | N/A | historically impossible by construction (no per-block tries, DESIGN.md); at the tip it is a Firewood limitation, not ours: ffi v0.3.1 only produces Firewood-native RangeProof/ChangeProof (proprietary encoding for merkle sync), no EIP-1186 MPT path proofs, and graft/evm's firewood trie hard-errors Prove/NodeIterator (base_trie.go), so even real Firewood coreth nodes cannot serve it |
| eth_getUncle{By,CountBy}* | DONE | zero/null (no uncles on avalanche) |

## web3 / net

| method | status | notes |
|---|---|---|
| web3_clientVersion | DONE | |
| web3_sha3 | DONE | keccak256, deterministic A/B |
| net_version | DONE | chain id string |
| net_listening | DONE | true |
| net_peerCount | DONE | 0x0 (no p2p serving); tolerance: remote reports its own peers |

## txpool

| method | status | notes |
|---|---|---|
| txpool_status | DONE | {pending:0x0, queued:0x0}; tolerance: remote pool is live |
| txpool_content | DONE | {pending:{}, queued:{}} |
| txpool_contentFrom | DONE | {pending:{}, queued:{}} |
| txpool_inspect | DONE | {pending:{}, queued:{}} |

## personal / admin

All N/A by design: no keystore (personal_*), no node ops surface
(admin_importChain etc. are operator tools of a full node, not archive reads).

## debug

| method | status | notes |
|---|---|---|
| debug_traceTransaction | DONE | replay block to tx + tracer (struct logger and named tracers) |
| debug_traceBlockByNumber / ByHash | DONE | |
| debug_traceCall | DONE | traces on the post-state of the tag, same base as eth_call; gate: 40 real-tx samples, structLogger/callTracer/eth_call parity on returnValue, failed, gasUsed; public API refuses (-32601) |
| debug_getRawBlock / Header / Transaction / Receipts | DONE | receipts by re-execution (debug lane) |
| debug_getModifiedAccountsByNumber / ByHash | DONE | served from the per-block write capture at ANY height (geth needs both boundary states live; here it is one writelog frame per block). Caveats: union-of-writes counts a value rewritten to its original across a range (geth trie-diff would not); capture order, not hash order; needs the raw writelogs, epoch-only/bootstrap nodes get a clean error; two-param range capped at 10k blocks. Gate: 100 random tx-bearing blocks (every tx sender present, ByHash == ByNumber, range == union of singles) + 5 empty blocks; public API refuses (-32601) |
| debug_getBadBlocks | DONE | always []: the root-verified replay retains no bad blocks; public API refuses (-32601) |
| debug_dumpBlock / accountRange / storageRangeAt | N/A (this release) | tip-POSSIBLE: ffi Revision.Iter can range-read the Firewood frontier at head, but the replay process holds the exclusive Firewood handle (single-writer), so a serving process cannot open it alongside; revisit when the unified node owns one handle |
| debug_printBlock / traceChain / traceBadBlock / intermediateRoots / getAccessibleState | N/A | default-off debug surface in coreth; no epochdb use case |
| debug_preimage | N/A | no preimage store by design |
| debug_setHead / chaindbCompact / mutating ops | N/A by design | |

## avax-specific (plugin/evm service)

avax_getAtomicTx, avax_getAtomicTxStatus, avax_getUTXOs, avax_issueTx etc.:
N/A for this phase; atomic txs/UTXOs live in extdata and the shared memory
UTXO set, indexed nowhere (DESIGN.md caveat), issuance is phase-3 territory
at the earliest.
