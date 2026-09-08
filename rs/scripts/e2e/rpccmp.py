"""rpccmp.py LOCAL REMOTE [N=5]: compare the N most recent blocks (remote head - 40 .. ) between the
plugin's /rpc and build.onbeam.com: blocks, receipts, txs, callTracer, balances, code, logs."""
import json, sys, time, urllib.request

L, R = sys.argv[1], sys.argv[2]
N = int(sys.argv[3]) if len(sys.argv) > 3 else 5
LAG = 40  # blocks below the remote head, so both sides surely have them

def call(url, method, params, tries=6):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    for i in range(tries):
        try:
            req = urllib.request.Request(url, body, {"content-type": "application/json", "user-agent": "curl/8.5.0"})
            with urllib.request.urlopen(req, timeout=60) as r:
                out = json.load(r)
            if "error" in out and (out["error"].get("code") in (-32005, 429) or "rate" in str(out["error"]).lower() or "limit" in str(out["error"]).lower()):
                time.sleep(3 * (i + 1)); continue
            return out.get("result", {"__error__": out.get("error")})
        except urllib.error.HTTPError as e:
            if e.code == 429: time.sleep(2 * (i + 1)); continue
            return {"__error__": f"http {e.code}"}
        except Exception as e:
            time.sleep(1); last = str(e)
    return {"__error__": "gave up"}

def remote(method, params):
    time.sleep(0.8)  # the public endpoint takes 1-3 calls/s
    return call(R, method, params)

def local(method, params): return call(L, method, params)

results = []
def probe(name, method, params):
    a, b = local(method, params), remote(method, params)
    eq = a == b
    detail = ""
    if not eq:
        if isinstance(a, dict) and isinstance(b, dict):
            keys = sorted(set(a) | set(b))
            diff = [k for k in keys if a.get(k) != b.get(k)]
            detail = "keys differ: " + ", ".join(f"{k}: local={json.dumps(a.get(k))[:80]} remote={json.dumps(b.get(k))[:80]}" for k in diff[:4])
        else:
            detail = f"local={json.dumps(a)[:120]} remote={json.dumps(b)[:120]}"
    results.append((name, eq, detail))
    print(f"{'EQUAL' if eq else 'DIFF '} {name} {method} {json.dumps(params)[:100]} {detail}", flush=True)
    return a, b

head = int(remote("eth_blockNumber", [])[2:], 16)
lhead = int(local("eth_blockNumber", []), 16)
print(f"remote head {head}, local head {lhead}")
probe("chainId", "eth_chainId", [])
heights = list(range(head - LAG, head - LAG + N))
txs, senders, tos = [], set(), set()
for h in heights:
    hx = hex(h)
    a, _ = probe(f"block {h} full", "eth_getBlockByNumber", [hx, True])
    if not isinstance(a, dict) or "hash" not in a: continue
    probe(f"blockByHash {h}", "eth_getBlockByHash", [a["hash"], False])
    probe(f"blockTxCount {h}", "eth_getBlockTransactionCountByNumber", [hx])
    for tx in a.get("transactions", [])[:3]:
        txs.append((h, tx["hash"]))
        senders.add(tx["from"])
        if tx.get("to"): tos.add(tx["to"])
    if a.get("transactions"):
        probe(f"blockReceipts {h}", "eth_getBlockReceipts", [hx])
for h, txh in txs[:8]:
    probe(f"receipt {txh[:12]}", "eth_getTransactionReceipt", [txh])
    probe(f"tx {txh[:12]}", "eth_getTransactionByHash", [txh])
for h, txh in txs[:4]:
    probe(f"callTracer {txh[:12]}", "debug_traceTransaction", [txh, {"tracer": "callTracer"}])
hx = hex(heights[-1])
for s in sorted(senders)[:4]:
    probe(f"balance {s[:10]} @{heights[-1]}", "eth_getBalance", [s, hx])
    probe(f"nonce {s[:10]} @{heights[-1]}", "eth_getTransactionCount", [s, hx])
for t in sorted(tos)[:3]:
    probe(f"code {t[:10]} @{heights[-1]}", "eth_getCode", [t, hx])
    probe(f"storage0 {t[:10]} @{heights[-1]}", "eth_getStorageAt", [t, "0x0", hx])
probe(f"logs {heights[0]}..{heights[-1]}", "eth_getLogs", [{"fromBlock": hex(heights[0]), "toBlock": hex(heights[-1])}])
# an older height too: state at a height far below the tip
old = 1_000_000
probe(f"block {old} hashes", "eth_getBlockByNumber", [hex(old), False])
for s in sorted(senders)[:2]:
    probe(f"balance {s[:10]} @{old}", "eth_getBalance", [s, hex(old)])
n_eq = sum(1 for _, eq, _ in results if eq)
print(f"SUMMARY {n_eq} of {len(results)} probes equal")
for name, eq, detail in results:
    if not eq: print(f"  DIFF {name}: {detail}")
