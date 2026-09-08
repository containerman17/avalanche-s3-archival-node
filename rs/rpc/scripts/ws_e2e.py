"""End-to-end websocket check against one node (stock or epochdb-rs under cmd/epochdb-host-bench --serve --feed-delay, or epochdb-rpc-serve), then a diff of two captures. Stdlib only (wsc.py is the client).
  capture URL N OUT.json   subscribe newHeads + logs + newPendingTransactions, collect N heads,
                           then getBlockByNumber / batch / unsubscribe / clean close
  diff A.json B.json       compare the notifications height by height, subscription ids removed
  latency CAP.json LOG     Accept-to-receive latency from the harness' `accepted height=` lines
"""
import json, re, sys, time
from urllib.parse import urlparse
from wsc import WS

def capture(url, n, out):
    u = urlparse(url)
    w = WS(u.hostname, u.port, u.path)
    subs = {}
    for name, params in [("newHeads", ["newHeads"]), ("logs", ["logs", {}]), ("pending", ["newPendingTransactions"])]:
        r = w.call("eth_subscribe", params)
        subs[r["result"]] = name
    print("subscribed", subs)
    msgs, heads = [], 0
    while heads < n:
        m = w.recv()
        t = time.time_ns()
        j = json.loads(m)
        assert j.get("method") == "eth_subscription", m[:200]
        kind = subs[j["params"]["subscription"]]
        if kind == "newHeads": heads += 1
        msgs.append({"t_ns": t, "kind": kind, "raw": m})
    print("captured", len(msgs), "notifications,", heads, "heads")
    # Plain calls on the same connection.
    hn = msgs[-1]["raw"]
    last = int(json.loads(hn)["params"]["result"]["number"], 16)
    r = w.call("eth_getBlockByNumber", [hex(last), False], id=11)
    assert r["result"]["number"] == hex(last), r
    w.send(1, json.dumps([{"jsonrpc":"2.0","id":12,"method":"eth_chainId"},{"jsonrpc":"2.0","id":13,"method":"eth_blockNumber"},{"jsonrpc":"2.0","id":14,"method":"eth_getLogs","params":[{"fromBlock":hex(last),"toBlock":hex(last)}]}]))
    # Notifications may interleave with the batch reply.
    while True:
        m = w.recv(); j = json.loads(m)
        if isinstance(j, list): break
        msgs.append({"t_ns": time.time_ns(), "kind": subs[j["params"]["subscription"]], "raw": m})
    assert [x["id"] for x in j] == [12, 13, 14] and j[0]["result"] and int(j[1]["result"], 16) >= last, j
    getlogs = j[2]["result"]
    for sid in list(subs):
        r = w.call("eth_unsubscribe", [sid], id=20)
        assert r["result"] is True, r
    r = w.call("eth_unsubscribe", [list(subs)[0]], id=21)
    assert r["error"]["message"] == "subscription not found", r
    r = w.call("eth_unsubscribe", [], id=22)
    assert r["error"]["code"] == -32602, r
    closef = w.close()
    print("close frame from server:", closef)
    assert closef is not None and closef[:2] == b"\x03\xe8", closef
    json.dump({"url": url, "subs": subs, "msgs": msgs, "getlogs_last": getlogs}, open(out, "w"))
    print("wrote", out)

def norm(cap):
    """{(kind, height, index): raw JSON with the subscription id replaced}."""
    out, per = {}, {}
    for m in cap["msgs"]:
        j = json.loads(m["raw"])
        res = j["params"]["result"]
        h = int(res.get("number") or res.get("blockNumber"), 16)
        k = (m["kind"], h)
        per[k] = per.get(k, 0)
        raw = m["raw"].replace(j["params"]["subscription"], "<SUB>")
        out[(m["kind"], h, per[k])] = raw
        per[k] += 1
    return out

def diff(a, b):
    A, B = norm(json.load(open(a))), norm(json.load(open(b)))
    ha = {k[1] for k in A if k[0] == "newHeads"}; hb = {k[1] for k in B if k[0] == "newHeads"}
    common = ha & hb
    print(f"A heads {min(ha)}..{max(ha)} ({len(ha)}), B heads {min(hb)}..{max(hb)} ({len(hb)}), common {len(common)}")
    ka = {k for k in A if k[1] in common}; kb = {k for k in B if k[1] in common}
    bad = 0
    for k in sorted(ka ^ kb):
        print("only in", "A" if k in ka else "B", k); bad += 1
    equal = 0
    for k in sorted(ka & kb):
        if A[k] == B[k]: equal += 1
        else:
            bad += 1
            if bad <= 5:
                print("DIFF", k); print("  A:", A[k][:400]); print("  B:", B[k][:400])
    kinds = {}
    for k in ka & kb: kinds[k[0]] = kinds.get(k[0], 0) + 1
    print(f"byte-equal notifications: {equal} / {len(ka & kb)} {kinds}; differences: {bad}")
    return bad == 0

def latency(cap, log):
    acc = {}
    for m in re.finditer(r"accepted height=(\d+) start_ns=(\d+) end_ns=(\d+)", open(log).read()):
        acc[int(m.group(1))] = (int(m.group(2)), int(m.group(3)))
    lat = []
    for m in json.load(open(cap))["msgs"]:
        if m["kind"] != "newHeads": continue
        h = int(json.loads(m["raw"])["params"]["result"]["number"], 16)
        if h in acc:
            lat.append(((m["t_ns"] - acc[h][0]) / 1e6, (m["t_ns"] - acc[h][1]) / 1e6))
    lat.sort()
    if not lat: print("no overlap"); return
    s, e = [x[0] for x in lat], [x[1] for x in lat]
    s.sort(); e.sort()
    q = lambda v, p: v[min(len(v) - 1, int(p * len(v)))]
    print(f"n={len(lat)} newHeads: from Accept start median {q(s,.5):.2f} ms p90 {q(s,.9):.2f} max {s[-1]:.2f}; from Accept return median {q(e,.5):.2f} ms p90 {q(e,.9):.2f} max {e[-1]:.2f}")

def static(url):
    """No live heads needed: subscribe answers, plain call, batch, unsubscribe, parse error closes, clean close."""
    u = urlparse(url)
    w = WS(u.hostname, u.port, u.path)
    sid = w.call("eth_subscribe", ["newHeads"])["result"]
    assert sid.startswith("0x") and len(sid) <= 34, sid
    r = w.call("eth_subscribe", ["logs", {"address": "0x0000000000000000000000000000000000000001", "topics": [[]]}])
    assert r["result"].startswith("0x"), r
    assert w.call("eth_subscribe", ["bogus"])["error"] == {"code": -32601, "message": 'no "bogus" subscription in eth namespace'}
    assert w.call("eth_subscribe", ["logs", 5])["error"]["message"] == "invalid argument 1: json: cannot unmarshal number into Go value of type filters.input"
    assert w.call("eth_subscribe", ["logs", {"fromBlock": "0x10", "toBlock": "0x5"}])["error"]["message"] == "invalid from and to block combination: from > to"
    assert w.call("eth_subscribe", ["newPendingTransactions", True])["result"].startswith("0x")
    head = int(w.call("eth_blockNumber")["result"], 16)
    b = w.call("eth_getBlockByNumber", [hex(head), True])["result"]
    assert b["number"] == hex(head) and b["hash"].startswith("0x"), b
    w.send(1, json.dumps([{"jsonrpc":"2.0","id":2,"method":"eth_chainId"},{"jsonrpc":"2.0","id":3,"method":"eth_getBlockByNumber","params":["0x1",False]},{"jsonrpc":"2.0","id":4,"method":"nope"}]))
    j = json.loads(w.recv())
    assert [x["id"] for x in j] == [2, 3, 4] and j[1]["result"]["number"] == "0x1" and j[2]["error"]["code"] == -32601, j
    assert w.call("eth_unsubscribe", [sid])["result"] is True
    assert w.call("eth_unsubscribe", [sid])["error"]["message"] == "subscription not found"
    assert w.call("eth_unsubscribe", [7])["error"]["message"] == "invalid argument 0: json: cannot unmarshal number into Go value of type rpc.ID"
    w.send(1, json.dumps({"jsonrpc":"2.0","method":"eth_chainId"}))  # a notification: no reply
    assert w.call("eth_chainId", id=9)["id"] == 9
    closef = w.close()
    assert closef is not None and closef[:2] == b"\x03\xe8", closef
    # A message that is not JSON: the parse error, then the server ends the connection.
    w = WS(u.hostname, u.port, u.path)
    w.send(1, "not json")
    r = json.loads(w.recv())
    assert r["error"]["code"] == -32700 and r["id"] is None, r
    try:
        rest = w.recv()
    except (EOFError, ConnectionResetError):
        rest = "reset"
    assert rest in (None, "reset"), rest
    print("static checks OK on", url, "(head", head, ", after parse error:", "close frame" if rest is None else rest, ")")

if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "capture": capture(sys.argv[2], int(sys.argv[3]), sys.argv[4])
    elif cmd == "diff": sys.exit(0 if diff(sys.argv[2], sys.argv[3]) else 1)
    elif cmd == "latency": latency(sys.argv[2], sys.argv[3])
    elif cmd == "static": static(sys.argv[2])
