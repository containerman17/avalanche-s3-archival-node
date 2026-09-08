# Latest-state run layout experiment (2026-09-09, branch rs-layout)

Question: which on-disk layout gives the fastest resident point lookup on
real C-chain keys, with size only as a means to keep more of the run in
page cache. Rulings applied: no cache layer (every Get pays its full cost),
the format is free to change, a per-roll dictionary stored in the file is
fine, nothing on the Tokyo box.

Code: `rs/layout/src/main.rs` (workspace member `epochdb-layout`, binary
`layout`). `run.rs` is untouched; candidate A calls it as is. Every
candidate is checked before it is timed: 20k random present keys return
their value, 20k absent keys miss, a full scan returns every row in order.

## Corpora and keys

Both corpora are Tokyo statedumps converted to contract form the way the
node stores them (`sample::to_contract_rows`: account `keccak(addr)+0x00`
with RLP[nonce, balance, codeHash], slot `keccak(addr)+0x01+keccak(slot)`
with the trimmed word, empty values dropped).

| corpus | rows | accounts | slots | contracts with slots | top contract | raw key+value |
|---|---|---|---|---|---|---|
| `cstate.bin` (newest C terminal run) | 3,592,482 | 1,673,117 (47%) | 1,919,365 | 24,371 | 970k slots (50% of slots) | 70.3 B/key |
| `cstate_recent_full.bin` | 13,551,760 | 116,792 (0.9%) | 13,434,968 | 16,306 | 5.79M slots (43%) | 67.6 B/key |

The brief had these swapped: `cstate.bin` is the account-heavy workload
(the 2023-style "many accounts" sample, 1.5M of its accounts are byte
identical proxy clones), `cstate_recent_full` is the 99%-slots one with
the 41-43% contract. No synthesis was needed.

Keys: 1M present keys drawn uniformly from the rows, 1M miss keys (an
existing contract with the last 8 bytes of the slot hash replaced, or a
never-seen address), the L3 variant is a 200k-row uniform subset built
into the same layout (8-12 MB, fits the 16 MB L3).

## Why 60.8 vs 34.2 B/key

`REPORT.md`'s 60.83 B/key is `cstate.bin` in raw statedump form (4M rows
including 407k tombstones, 21-byte `addr+'a'` keys with the 71-byte
5-field coreth RLP, 53-byte `addr+'s'+slot` keys that share only the
20-byte address). Reproduced here exactly (row "A raw"). The 34.2 and the
Go statebench's 34.7 were synthetic keccak-shaped samples with short
values. In contract form the same `cstate.bin` rows front-code to 55.2
B/key (47% account rows: 33-byte key sharing ~2 bytes with its neighbour
plus a 40-75 byte RLP value) and the 99%-slot corpus to 36.7 B/key
(3 + 32 + ~1.5 value bytes per slot). So 60.8 was the corpus and the key
form, not the layout; the honest baseline for the node is 55.2 / 36.7.

## Method

i7-10700K 8C/16T, 25 GB, WSL2. Machine load 5 to 10 during every run
(other agents' cargo builds), recorded at the start and end of each sweep
below. Every candidate: build, write, mmap, touch every page, then
3 rounds of {Get 1 thread 2M ops (mean), 200k timed ops (p50, p99, the
clock has 100 ns resolution here), Get 16 threads 16M ops, miss 1M ops,
full scan, 200k random block decompressions for D}; the table shows the
median of the 3 rounds. Round-to-round spread on the loaded box is about
10% on single-thread Get and 30% on the 30-100 ms scan timings, so treat
differences under 10% as noise; the ordering below was the same on both
corpora.

Columns: B/key = file bytes / rows (index, dictionaries and sidecar
included); build = single-threaded build from in-memory rows, no fsync;
RAM = bytes that must live outside the run's blocks and block index
(sidecar table, contract table, zstd dictionary); L3 = the 200k-row
variant.

Candidates:

- A: `run.rs` as is (4 KB front-coded blocks, linear scan, 40-byte prefix index, binary search).
- B: front-coded blocks of 2/4/8 KB with restart points every R entries (R0 = none; a restart entry stores its full key, binary search over the restarts then a linear scan), 16-byte block index (u64 contract-hash prefix, u64 slot-hash prefix of the block's first key), binary search at both levels ("Bin").
- C: the same blocks with the index searched differently. "Interp" = C1: contract table (distinct contract prefixes -> first block, built at open, 12 B per entry) searched by interpolation (uniform keccak prefixes), then interpolation over the slot prefixes inside a contract's block range. "Mixed" = binary at the top, interpolation in the tie range. "Bin" = C2.
- D: fixed-width rows ([klen][key padded to the block's max key width][vlen][value padded to the block's max value width]) so the block is binary searched, blocks of 512 B / 1 KB / 4 KB compressed with zstd 1, zstd 3 or lz4 (`lz4_flex`), with and without a 32 KB dictionary trained on the blocks (`zstd::dict::from_samples`, also used as the lz4 dictionary), decompressed on every Get. "Plain" = the same rows uncompressed.
- E: hash sidecar over B's blocks: open-addressing table of u64 slots [24-bit fingerprint | 40-bit file offset of the entry's restart group], linear probing, load factor 0.80 (10 B/key), Get = one probe then a scan of at most R entries; a miss probes to the first empty slot. "heap" = the table and the index copied into anonymous memory with MADV_HUGEPAGE instead of the 4 KB-paged file mapping.
- F: value encodings on the same blocks. Packed = account RLP -> [nlen<<2 | coded<<1 | empty][blen][nonce][balance][codeHash unless empty]; Dict = Packed plus the 127 most saving whole values coded in the vlen byte (vlen >= 128, no value bytes; the values in the file); Subst = Dict plus a table of the 127 most common code hashes (1-byte codes).

## Results: cstate.bin (47% accounts)

Load 6.7 at start, 9.8 at end.

| candidate | B/key | file MB | build s | Get 1T ns | p50 | p99 | Get 16T ns | miss ns | scan Mkeys/s | decomp ns | RAM MB | L3 Get 1T | L3 Get 16T |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A raw keys (REPORT.md's measurement, 4M raw rows) | 60.83 | 243 | 0.97 | 748 | 800 | 1300 | 95.5 | 150 | 138 | | | 458 | 45.9 |
| A run.rs 4K/p40 | 55.17 | 198 | 1.84 | 778 | 800 | 1400 | 94.1 | 498 | 122 | | | 479 | 48.8 |
| B front 2K R0 Bin | 55.91 | 201 | 0.38 | 735 | 800 | 1500 | 77.8 | 521 | 126 | | | 418 | 39.4 |
| B front 2K R8 Bin | 58.51 | 210 | 0.36 | 794 | 900 | 1600 | 78.4 | 553 | 99 | | | 431 | 40.0 |
| B front 2K R16 Bin | 57.12 | 205 | 0.47 | 839 | 900 | 1600 | 82.5 | 597 | 104 | | | 454 | 40.6 |
| B front 2K R32 Bin | 56.32 | 202 | 0.50 | 834 | 800 | 1500 | 79.3 | 587 | 124 | | | 473 | 40.9 |
| B front 4K R0 Bin | 54.91 | 197 | 0.49 | 865 | 900 | 1700 | 107.8 | 559 | 88 | | | 528 | 53.5 |
| B front 4K R8 Bin | 57.57 | 207 | 0.59 | 820 | 800 | 1500 | 85.4 | 587 | 123 | | | 428 | 37.8 |
| B front 4K R16 Bin | 56.14 | 202 | 0.51 | 845 | 800 | 1400 | 77.6 | 592 | 94 | | | 425 | 37.8 |
| B front 4K R32 Bin | 55.50 | 199 | 0.46 | 781 | 800 | 1400 | 76.0 | 601 | 121 | | | 431 | 38.3 |
| B front 8K R0 Bin | 54.48 | 196 | 0.40 | 907 | 900 | 1600 | 137.4 | 709 | 128 | | | 675 | 62.4 |
| B front 8K R8 Bin | 57.10 | 205 | 0.44 | 862 | 900 | 1500 | 90.8 | 680 | 120 | | | 419 | 37.8 |
| B front 8K R16 Bin | 55.78 | 200 | 1.54 | 985 | 1100 | 1600 | 91.9 | 723 | 118 | | | 486 | 42.3 |
| B front 8K R32 Bin | 55.12 | 198 | 0.83 | 951 | 1000 | 1700 | 93.4 | 767 | 124 | | | 470 | 43.7 |
| C1 front 4K R16 Interp | 56.14 | 202 | 0.76 | 802 | 800 | 1300 | 81.7 | 560 | 121 | | 0.4 | 519 | 43.7 |
| C front 4K R16 Mixed | 56.14 | 202 | 0.38 | 811 | 800 | 1400 | 78.0 | 580 | 121 | | | 425 | 37.9 |
| C1 front 8K R16 Interp | 55.78 | 200 | 0.89 | 880 | 900 | 1400 | 83.1 | 680 | 118 | | 0.2 | 456 | 37.6 |
| C front 8K R16 Mixed | 55.78 | 200 | 1.20 | 934 | 900 | 1500 | 89.1 | 692 | 119 | | | 426 | 38.7 |
| B front 4K R16 Bin heap | 56.14 | 202 | 0.51 | 739 | 800 | 1300 | 73.8 | 544 | 128 | | | 389 | 36.0 |
| D fixed 512B zstd1 | 51.93 | 187 | 2.26 | 1100 | 1100 | 2200 | 91.1 | 859 | 29 | 424 | | 563 | 50.2 |
| D fixed 512B zstd1+dict | 42.75 | 154 | 2.16 | 1308 | 1200 | 2400 | 107.0 | 981 | 24 | 471 | 0.03 | 665 | 60.5 |
| D fixed 512B zstd3 | 51.86 | 186 | 2.51 | 1076 | 1100 | 2200 | 94.2 | 852 | 23 | 415 | | 571 | 51.6 |
| D fixed 512B zstd3+dict | 42.54 | 153 | 2.77 | 1226 | 1200 | 2600 | 104.3 | 900 | 24 | 461 | 0.03 | 623 | 57.2 |
| D fixed 512B lz4 | 53.75 | 193 | 1.13 | 1014 | 900 | 1900 | 83.4 | 700 | 55 | 296 | | 453 | 44.0 |
| D fixed 512B lz4+dict | 44.11 | 158 | 7.02 | 1178 | 1400 | 3000 | 102.1 | 822 | 44 | 352 | 0.03 | 483 | 45.8 |
| D fixed 1K zstd1 | 44.94 | 161 | 1.67 | 1234 | 1300 | 2400 | 113.4 | 974 | 35 | 600 | | 733 | 71.1 |
| D fixed 1K zstd1+dict | 38.02 | 137 | 2.43 | 1372 | 1300 | 2600 | 115.8 | 1054 | 30 | 698 | 0.03 | 771 | 81.2 |
| D fixed 1K zstd3 | 44.90 | 161 | 1.95 | 1110 | 1100 | 2000 | 102.3 | 898 | 35 | 568 | | 697 | 64.2 |
| D fixed 1K zstd3+dict | 38.15 | 137 | 2.89 | 1207 | 1200 | 2200 | 112.0 | 1036 | 30 | 600 | 0.03 | 711 | 70.6 |
| D fixed 1K lz4 | 48.01 | 172 | 0.84 | 990 | 1000 | 1900 | 87.4 | 739 | 54 | 397 | | 554 | 56.5 |
| D fixed 1K lz4+dict | 41.08 | 148 | 4.21 | 1166 | 1200 | 2400 | 101.9 | 850 | 46 | 473 | 0.03 | 637 | 70.8 |
| D fixed 4K zstd1 | 38.57 | 139 | 1.04 | 2077 | 1900 | 3600 | 206.2 | 1793 | 29 | 1521 | | 1534 | 171.5 |
| D fixed 4K zstd1+dict | 34.19 | 123 | 5.72 | 1842 | 1800 | 2800 | 180.7 | 1599 | 39 | 1296 | 0.03 | 1385 | 147.9 |
| D fixed 4K zstd3 | 38.43 | 138 | 1.31 | 2137 | 2000 | 3500 | 211.0 | 1841 | 32 | 1605 | | 1640 | 176.1 |
| D fixed 4K zstd3+dict | 34.31 | 123 | 7.11 | 1944 | 1900 | 3500 | 196.6 | 1694 | 39 | 1334 | 0.03 | 1503 | 162.4 |
| D fixed 4K lz4 | 43.25 | 155 | 0.71 | 1527 | 1400 | 2500 | 158.5 | 1292 | 49 | 1044 | | 1237 | 141.7 |
| D fixed 4K lz4+dict | 38.96 | 140 | 6.77 | 1592 | 1500 | 3000 | 169.9 | 1383 | 45 | 1145 | 0.03 | 1444 | 154.8 |
| D fixed 4K plain (no compression) | 85.98 | 309 | 1.40 | 828 | 900 | 1500 | 79.1 | 614 | 139 | | | 375 | 34.9 |
| E sidecar on 4K R8 Bin | 67.57 | 243 | 1.45 | 566 | 600 | 1000 | 52.1 | 267 | 120 | | 35.9 | 293 | 27.6 |
| E sidecar on 4K R16 Bin | 66.14 | 238 | 1.45 | 605 | 600 | 1200 | 54.7 | 281 | 116 | | 35.9 | 342 | 32.1 |
| E sidecar on 4K R16 Bin heap | 66.14 | 238 | 0.62 | 566 | 600 | 1000 | 49.3 | 266 | 120 | | 35.9 | 306 | 30.3 |
| F2 front 4K R16 Bin Packed | 54.54 | 196 | 0.64 | 805 | 800 | 1300 | 76.6 | 594 | 77 | | | 414 | 38.8 |
| F3 front 4K R16 Bin Dict | 38.13 | 137 | 0.98 | 771 | 800 | 1300 | 72.0 | 569 | 80 | | | 376 | 35.0 |
| F3 front 2K R0 Interp Subst | 37.40 | 134 | 0.86 | 685 | 700 | 1100 | 72.0 | 418 | 82 | | 0.4 | 416 | 42.0 |
| F3 front 4K R0 Interp Subst | 36.83 | 132 | 0.92 | 836 | 700 | 1300 | 99.4 | 513 | 67 | | 0.2 | 591 | 58.2 |
| E + Dict, 4K R16 Bin heap | 48.13 | 173 | 0.96 | 541 | 600 | 1000 | 47.9 | 261 | 81 | | 35.9 | 303 | 28.7 |
| E + Subst, 4K R16 Interp | 48.12 | 173 | 1.13 | 601 | 600 | 1100 | 54.1 | 280 | 74 | | 36.1 | 366 | 30.4 |

## Results: cstate_recent_full.bin (99% slots, one contract holds 43%)

Shortlist only (D was clearly losing on Get after the first corpus, so
only its two most compact variants ran). Load 4.9 at start, 6.3 at end.

| candidate | B/key | file MB | build s | Get 1T ns | p50 | p99 | Get 16T ns | miss ns | scan Mkeys/s | decomp ns | RAM MB | L3 Get 1T | L3 Get 16T |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A run.rs 4K/p40 | 36.72 | 498 | 6.89 | 937 | 1000 | 1600 | 106.1 | 633 | 153 | | | 513 | 49.5 |
| B front 2K R0 Bin | 37.07 | 502 | 2.89 | 864 | 900 | 1500 | 85.4 | 591 | 147 | | | 386 | 39.3 |
| B front 4K R0 Bin | 36.54 | 495 | 2.80 | 873 | 900 | 1400 | 101.2 | 629 | 155 | | | 479 | 48.9 |
| B front 4K R16 Bin | 38.68 | 524 | 3.02 | 908 | 900 | 1400 | 89.7 | 676 | 89 | | | 363 | 34.2 |
| B front 8K R0 Bin | 36.17 | 490 | 3.90 | 1089 | 1100 | 1800 | 147.5 | 869 | 156 | | | 694 | 72.0 |
| C1 front 4K R16 Interp | 38.68 | 524 | 2.83 | 844 | 800 | 1300 | 81.6 | 601 | 143 | | 0.1 | 368 | 36.8 |
| C front 4K R16 Mixed | 38.68 | 524 | 2.96 | 872 | 900 | 1400 | 86.8 | 629 | 146 | | | 366 | 34.9 |
| B front 4K R16 Bin heap | 38.68 | 524 | 2.95 | 919 | 900 | 1400 | 89.9 | 658 | 144 | | | 353 | 35.3 |
| D fixed 512B zstd1+dict | 41.63 | 564 | 7.05 | 1402 | 1400 | 2600 | 121.3 | 1096 | 31 | 475 | 0.03 | 560 | 58.6 |
| D fixed 4K zstd1+dict | 33.80 | 458 | 9.74 | 1847 | 1900 | 3500 | 188.5 | 1762 | 53 | 1156 | 0.03 | 1268 | 135.2 |
| E sidecar on 4K R8 Bin | 51.09 | 692 | 7.26 | 588 | 600 | 1000 | 53.3 | 394 | 151 | | 135.5 | 270 | 25.9 |
| E sidecar on 4K R16 Bin | 48.68 | 660 | 6.84 | 626 | 600 | 1100 | 57.3 | 426 | 146 | | 135.5 | 288 | 28.0 |
| E sidecar on 4K R16 Bin heap | 48.68 | 660 | 7.65 | 670 | 700 | 1100 | 59.6 | 462 | 146 | | 135.5 | 293 | 30.2 |
| F2 front 4K R16 Bin Packed | 38.53 | 522 | 4.39 | 919 | 900 | 1400 | 88.4 | 686 | 143 | | | 369 | 35.4 |
| F3 front 4K R16 Bin Dict | 37.01 | 502 | 6.78 | 942 | 900 | 1500 | 89.4 | 673 | 126 | | | 388 | 39.2 |
| F3 front 2K R0 Interp Subst | 35.34 | 479 | 2.44 | 777 | 800 | 1200 | 83.8 | 522 | 103 | | 0.1 | 551 | 73.9 |
| F3 front 4K R0 Interp Subst | 34.75 | 471 | 3.25 | 931 | 900 | 1500 | 118.1 | 685 | 127 | | 0.0 | 618 | 58.4 |
| E + Dict, 4K R16 Bin heap | 47.01 | 637 | 11.00 | 674 | 700 | 1100 | 61.2 | 469 | 127 | | 135.5 | 295 | 29.0 |
| E + Subst, 4K R16 Interp | 47.00 | 637 | 4.17 | 629 | 600 | 1100 | 57.4 | 416 | 127 | | 135.6 | 336 | 30.9 |

## What the numbers say

1. Baseline reproduced. Raw keys: 60.83 B/key exactly; Get 748 ns / 95.5 ns 16-way under load 6-8 (REPORT.md's 658 / 68 were taken on a quieter box). The A vs B-4K-R0 pairs (778 vs 865, 937 vs 873) show the run-to-run noise floor: about 10%.

2. The Get is a chain of dependent cache misses, not a scan. Even fully L3-resident (200k rows) A costs 480-510 ns: about 12 dependent L3 hits (index binary search, then the block). Going to the full-size file adds only 300-400 ns of DRAM. That is why restart points (B) buy nothing: the in-block linear scan is cheap and prefetched, while the binary search over restarts adds dependent misses. R8-R32 are within noise of R0 on Get and cost 1-2 B/key (a restart entry stores the whole 65-byte key). Drop them, except as the anchor the sidecar needs.

3. Block size: 2 KB is the fastest at every thread count (735 / 864 ns single, 78 / 85 ns 16-way, versus 865 / 873 and 108 / 101 at 4 KB, 907 / 1089 and 137 / 148 at 8 KB) for +0.5 to +1 B/key. Half the lines per lookup. 8 KB loses clearly.

4. Index: the 16-byte-per-block index (u64 contract prefix, u64 slot prefix) performs like the 40-byte one at 2.5x less space, and the interpolation search over the contract table (C1) is 5-7% faster than binary search on both corpora (802 vs 845, 844 vs 908), including on the 43% contract where the slot-prefix interpolation lands in about 3 probes. The contract table costs 0.1-0.4 MB here. Mixed is between. Copying the index and the sidecar into hugepage heap memory ("heap") made no measurable difference, so the 4 KB file mapping's TLB cost is not what we pay; the misses are real DRAM misses.

5. Block compression (D) loses everywhere. Decompression alone is 300-470 ns per Get at 512 B, 400-700 ns at 1 KB, 1.0-1.6 us at 4 KB (zstd 1 and 3 decode alike; lz4 is 30% cheaper to decode but compresses 15-25% worse). The most compact variant (4 KB zstd1 + trained dictionary, 34.2 / 33.8 B/key) is 2.0-2.4x slower on Get than the baseline, and the 512 B variants that keep Get within 1.4x are barely smaller than front coding (42.8 / 41.6). The Go experiment's "compression on top of front coding buys nothing" holds in Rust too, and fixed rows plus a dictionary does not change it. Build time also doubles or triples.

6. Value encodings (F) give the size that compression was supposed to give, at zero Get cost. Packed accounts alone are worth little (most accounts in these runs have code, so the empty-code flag rarely fires: -1.6 / -0.2 B/key). The whole-value dictionary is the real win: -17 B/key on the account-heavy corpus (1.5M proxy clones share one packed account value: same nonce, balance and code hash) and -1.7 B/key on the slot-heavy one (10.5M of 13.4M slot values are the single byte 0x01, coded to zero bytes). The code-hash table (Subst) adds nothing measurable beyond the whole-value dictionary on either corpus, so it can be left out. Decoding the packed RLP on account Gets is free at this scale (805 vs 845 ns).

7. The hash sidecar (E) is the only way to a materially faster Get: 566-629 ns single (-20 to -35% vs A), 48-57 ns 16-way (-40 to -50%), misses 260-430 ns, and it is insensitive to the giant contract. It costs 10 B/key for the table plus 1-2 B/key of restart entries: 47-48 B/key with the dictionary against 35-37 for the same blocks without it. Its L3-resident cost of 270-300 ns is the shortest dependent chain of any candidate (one table line, one block line, a scan of at most 8-16 entries).

8. Scan throughput (the roll's need) is 80-155 Mkeys/s for every uncompressed layout (differences are noise on 30-100 ms timings) and 25-55 Mkeys/s for the compressed ones; the roll itself runs at 1 Mkeys/s, so none of this matters for the roll.

## Recommendation

Adopt, as the run format:

- front-coded 2 KB blocks, no restart points (B, 2K R0);
- the 16-byte block index (u64 contract-hash prefix + u64 slot-hash prefix of the block's first key) with the contract table and interpolation search (C1); resolve the rare exact-prefix tie by comparing the block's stored first key, which makes the index exact rather than probabilistic;
- packed account values plus the per-run whole-value dictionary of the top 127 values coded in the vlen byte (F3 Dict; skip the code-hash table).

Measured as "F3 front 2K R0 Interp Subst" (Subst equals Dict within
0.1 B/key): 685 / 777 ns single-thread Get against A's 778 / 937 (-12% /
-17%), 72 / 84 ns 16-way against 94 / 106 (-23% / -21%), misses 418 / 522
against 498 / 633, at 37.4 / 35.3 B/key against 55.2 / 36.7 (-32% / -4%),
with an index 2.5x smaller per block. It is better than A on every axis
on both corpora and needs no RAM beyond the mapping.

Add the hash sidecar (E) only if the executor profile shows Get on the
critical path: it is another -12% to -19% single-thread and -25% to -32%
16-way on top of the format above, for +10 B/key of page cache (8.5 GB
on C) and +1-2 B/key of restart entries. The table can be written as an
optional section of the same file by the roll, so this is a later switch,
not a format decision now.

Do not adopt block compression in any form: 2-2.4x slower Gets for a size
the dictionary already delivers.

## Projections for C (847M keys)

Composition matters, the two corpora bracket it:

| | slot-heavy (cstate_recent_full shape) | account-heavy (cstate shape) |
|---|---|---|
| A today | 36.7 B/key, 31.1 GB | 55.2 B/key, 46.7 GB |
| recommended (2K, Interp, Dict) | 35.3 B/key, 29.9 GB | 37.4 B/key, 31.7 GB |
| with the sidecar (4K R16 + table) | 47.0 B/key, 39.8 GB (8.5 GB of it the table) | 48.1 B/key, 40.8 GB |

Block index: 2 KB blocks hold about 55-58 rows, so C has about 15M blocks
and a 240 MB index (A's 40-byte index over 4 KB blocks: 7.5M blocks,
300 MB). The index must be resident for the Get numbers above; it is
mmapped like the blocks (heap made no difference). Contract table:
proportional to the distinct contract prefixes that start a block, 0.1 MB
per 13.5M keys here, under 10 MB on C. Sidecar RAM if adopted: 8.5 GB at
load factor 0.80 (10 B/key), all of it hot for the one-probe Get; 40-bit
offsets cover 1 TB files; 24-bit fingerprints give a false group scan
once per 5M probes; a miss probes to the first empty slot, 13 slots
(2 cache lines) expected at 0.80.

Build at roll: the front coder is streaming (0.4-3 s per corpus here,
about 4-5M rows/s single-threaded, so 3-4 minutes for C inside a roll
that takes 14 minutes at 1M keys/s). The dictionary needs a value census
before the blocks are written: a HashMap over 13.5M values took 3-6 s
here; on C do it on a 1% sample of the merge stream or carry the previous
run's dictionary (the codes are per file, so a stale dictionary only
costs bytes, never correctness). The sidecar build is 4-7 s per 13.5M
keys of random writes (about 5-8 minutes on C) and needs the 8.5 GB table
in memory or mapped during the roll.

## Risks

- Index size: 16 B per 2 KB block is 240 MB on C, plus the contract table; the 40-byte index at 4 KB was 300 MB, so no regression, but 2 KB blocks double the block count compared with 4 KB at the same index width. If the index must shrink, 4 KB blocks with the same index (120 MB) cost about 15% on single-thread Get and 20-40% on 16-way.
- Prefix ties: the index compares 8-byte prefixes. Two different contracts sharing 8 leading keccak bytes, or two slots of the 43% contract sharing 8 slot-hash bytes, both happen on C at the block level (the largest contract on C may hold hundreds of millions of slots, a few 8-byte collisions among 5M block boundaries are likely). The experiment's search treats such a tie as "this block or the one before" and would return a spurious miss for a key that equals a colliding block's first-key prefix but sorts before it. The production version must compare the stored first key of the block on an exact prefix tie (one extra read in a case that is effectively never hit). The sidecar path has no such ambiguity (full key comparison in the scan).
- The 41-43% contract: the index plateau is handled by interpolation on the slot prefix (measured on the 43% corpus: 844 vs 908 ns for binary). Interpolation degrades if slot prefixes are not uniform inside one contract; they are keccak outputs, so this only breaks for a contract that stores under crafted slot keys, and the fallback is the bounded binary search the code already has (at most 8 interpolation steps, then binary on the window).
- Dictionary drift: a value that leaves the top 127 between rolls is only written longer, never wrong; a value longer than 127 bytes cannot be stored (account RLP is at most 82 bytes, slots 32; the vlen code space is 128..255).
- Noise: every number above was measured under load 5-10 from other agents' builds. The absolute Get numbers are 10-15% worse than a quiet box (REPORT.md's 658 ns baseline vs 778 here); the relative ordering was the same on both corpora and across rounds, and the size numbers are exact.
- Not measured: page-cache-cold Gets (all reads resident by ruling), the write side of the roll with fsync, and the executor's real key distribution (uniform over the corpus here; hot keys would favour every layout equally except that the sidecar keeps fewer lines hot).

## Reproduce

```sh
cd rs && cargo build --release -p epochdb-layout
S=<scratch>/rs   # cstate.bin, cstate_recent_full.bin (the contract-form cache is written beside them)
target/release/layout $S/cstate.bin --raw --rounds 3 --set A           # REPORT.md's 60.83 B/key row
target/release/layout $S/cstate.bin --rounds 3                          # all sets: A,B,C,D,E,F
target/release/layout $S/cstate.bin --rounds 3 --set G,H                # Subst rows, heap rows
target/release/layout $S/cstate_recent_full.bin --rounds 3 --set S,G    # the shortlist
```
