"""wsheads.py HOST PORT PATH N LOCAL_RPC: subscribe newHeads, print N heads with arrival time, cross-check each hash with eth_getBlockByNumber."""
import json, sys, time, urllib.request
sys.path.insert(0, __file__.rsplit("/", 3)[0] + "/rpc/scripts")  # wsc.py
from wsc import WS
host, port, path, n, rpc = sys.argv[1], int(sys.argv[2]), sys.argv[3], int(sys.argv[4]), sys.argv[5]
ws = WS(host, port, path, timeout=300)
ws.send(1, json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["newHeads"]}))
sub = json.loads(ws.recv())
print("subscribed", sub, flush=True)
got = 0
while got < n:
    m = json.loads(ws.recv())
    if m.get("method") != "eth_subscription": print("other", m); continue
    h = m["params"]["result"]
    num = int(h["number"], 16)
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_getBlockByNumber", "params": [h["number"], False]}).encode()
    with urllib.request.urlopen(urllib.request.Request(rpc, body, {"content-type": "application/json", "user-agent": "curl/8.5.0"}), timeout=30) as r:
        blk = json.load(r)["result"]
    print(f"{time.strftime('%H:%M:%S', time.gmtime(time.time()+9*3600))} JST newHead number={num} hash={h['hash']} ts={int(h['timestamp'],16)} age={time.time()-int(h['timestamp'],16):.1f}s rpc_hash_match={blk['hash']==h['hash']}", flush=True)
    got += 1
ws.send(1, json.dumps({"jsonrpc": "2.0", "id": 2, "method": "eth_unsubscribe", "params": [sub["result"]]}))
print("unsubscribe", ws.recv())
