#!/usr/bin/env python3
"""Compare blockcheck --senders output (height idx txhash sender) against
eth_getBlockByNumber(full txs) from an RPC door: block hash, tx hash, from.
usage: senders_oracle.py <senders.txt> <rpc url> [max blocks] [step]"""
import json, sys, urllib.request
from collections import defaultdict

path, url = sys.argv[1], sys.argv[2]
maxblocks = int(sys.argv[3]) if len(sys.argv) > 3 else 300
step = int(sys.argv[4]) if len(sys.argv) > 4 else 97
by_height = defaultdict(dict)
for line in open(path):
    h, i, txh, snd = line.split()
    by_height[int(h)][int(i)] = (txh.lower(), snd.lower())
heights = sorted(by_height)[::step][:maxblocks]
ok = bad = ntx = 0
for at in range(0, len(heights), 50):
    batch = heights[at:at + 50]
    req = [{"jsonrpc": "2.0", "id": h, "method": "eth_getBlockByNumber", "params": [hex(h), True]} for h in batch]
    r = urllib.request.Request(url, json.dumps(req).encode(), {"content-type": "application/json", "user-agent": "curl/8.0"})
    for resp in json.load(urllib.request.urlopen(r, timeout=60)):
        h, blk = resp["id"], resp["result"]
        want = by_height[h]
        if len(blk["transactions"]) != len(want):
            bad += 1; print("tx count", h, len(blk["transactions"]), len(want)); continue
        for i, t in enumerate(blk["transactions"]):
            ntx += 1
            if (t["hash"].lower(), t["from"].lower()) != want[i]:
                bad += 1; print("mismatch", h, i, t["hash"], t["from"], want[i])
            else:
                ok += 1
print(f"blocks={len(heights)} txs={ntx} ok={ok} bad={bad}")
sys.exit(1 if bad else 0)
