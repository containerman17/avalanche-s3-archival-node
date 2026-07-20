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
| eth_createAccessList | TODO(other) | |
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
| eth_coinbase | DONE | zero address like coreth |
| eth_sign / eth_signTransaction | N/A | no keystore by design |
| eth_getProof | N/A | impossible by construction: no per-block tries (DESIGN.md) |
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

## debug (other session's lane)

debug_traceTransaction, debug_traceBlockByNumber/ByHash, debug_traceCall,
debug_getRawBlock/Header/Receipts/Transaction, debug_dumpBlock,
debug_accountRange, debug_storageRangeAt, debug_getModifiedAccountsBy*,
debug_getBadBlocks, debug_preimage: TODO(other). debug_setHead,
debug_chaindbCompact and other mutating/ops calls: N/A by design.

## avax-specific (plugin/evm service)

avax_getAtomicTx, avax_getAtomicTxStatus, avax_issueTx etc.: N/A for this
phase; atomic txs live in extdata and are indexed nowhere (DESIGN.md caveat),
issuance is phase-3 territory at the earliest.
