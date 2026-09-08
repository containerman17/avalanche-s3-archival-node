#!/usr/bin/env python3
"""Compare our callTracer JSON (epochdb-exec --traces-out) with the door's
debug_traceBlockByNumber callTracer output, after normalizing both sides
(lowercase hex, drop empty optional fields). Usage:

  trace_diff.py OUR.jsonl CACHE_DIR [--max-heights N] [--raw]

Picks a sample of heights that covers every call shape (nested calls, creates,
errors) plus plain transfers, fetches those blocks' traces from the door (cached
under CACHE_DIR), and prints every semantic difference by field path, then a
summary. --raw also reports byte-level differences (key order, hex case,
omitted-vs-present empties).
"""
import json, os, sys, time, collections, urllib.request

URL = 'https://epochdb-rpc.containerman.me/ext/bc/2jRZvKtXY5nyWTqRwFh1KMHGrCRxJoULu4r2CsayWRnjdDGbV1/rpc?token=8f3c1a9e7d2b4c6f0a1e5d9b3c7f2a4e6b8d0c1f3a5e7b9d2c4f6a8e0b1d3f5a'

def rpc(batch):
    for attempt in range(5):
        try:
            req = urllib.request.Request(URL, data=json.dumps(batch).encode(), headers={'content-type': 'application/json', 'user-agent': 'curl/8.5.0'})
            with urllib.request.urlopen(req, timeout=120) as r:
                return json.load(r)
        except Exception as e:
            print('retry', attempt, e, file=sys.stderr); time.sleep(2 * (attempt + 1))
    raise SystemExit('door down')

def door_traces(heights, cache):
    out = {}
    need = []
    for h in heights:
        p = f'{cache}/door-trace-{h}.json'
        if os.path.exists(p):
            out[h] = json.load(open(p))
        else:
            need.append(h)
    for i in range(0, len(need), 20):
        chunk = need[i:i + 20]
        res = rpc([{"jsonrpc": "2.0", "id": h, "method": "debug_traceBlockByNumber", "params": [hex(h), {"tracer": "callTracer"}]} for h in chunk])
        for r in res:
            if 'error' in r:
                raise SystemExit(f'door {r["id"]}: {r["error"]}')
            json.dump(r['result'], open(f'{cache}/door-trace-{r["id"]}.json', 'w'))
            out[r['id']] = r['result']
        time.sleep(0.2)
    return out

def norm(v):
    if isinstance(v, dict):
        return {k: norm(x) for k, x in v.items() if x not in (None, '', [], {})}
    if isinstance(v, list):
        return [norm(x) for x in v]
    if isinstance(v, str) and v.startswith('0x'):
        return v.lower()
    return v

def diff(a, b, path, out):
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a:
                out.append((path + '.' + k, 'missing in ours', None, b[k] if not isinstance(b[k], (dict, list)) else '...'))
            elif k not in b:
                out.append((path + '.' + k, 'extra in ours', a[k] if not isinstance(a[k], (dict, list)) else '...', None))
            else:
                diff(a[k], b[k], path + '.' + k, out)
    elif isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            out.append((path, f'len {len(a)} vs {len(b)}', None, None))
        for i, (x, y) in enumerate(zip(a, b)):
            diff(x, y, f'{path}[{i}]', out)
    elif a != b:
        out.append((path, 'value', a, b))

def shape(t):
    kinds = {t['type']}
    d = 1
    for c in t.get('calls', []):
        k, dd = shape(c)
        kinds |= k
        d = max(d, dd + 1)
    return kinds, d

def main():
    ours_path, cache = sys.argv[1], sys.argv[2]
    max_heights = int(sys.argv[sys.argv.index('--max-heights') + 1]) if '--max-heights' in sys.argv else 60
    raw = '--raw' in sys.argv
    ours = collections.defaultdict(dict)
    by_shape = collections.defaultdict(list)
    for line in open(ours_path):
        r = json.loads(line)
        ours[r['height']][r['tx']] = r['result']
        t = r['result']
        kinds, depth = shape(t)
        key = (t['type'], depth, ','.join(sorted(kinds)), 'err' if t.get('error') else 'ok', 'input' if t.get('input', '0x') != '0x' else 'plain')
        by_shape[key].append(r['height'])
    heights = set()
    for key, hs in sorted(by_shape.items(), key=lambda kv: len(kv[1])):
        take = 8 if len(hs) > 100 else len(hs)
        for h in sorted(set(hs))[:take]:
            heights.add(h)
        if len(heights) >= max_heights:
            break
    # plus the busiest blocks, so the bulk shapes are covered by volume too
    for h, _ in sorted(ours.items(), key=lambda kv: -len(kv[1]))[:10]:
        heights.add(h)
    heights = sorted(heights)
    print(f'{len(by_shape)} call shapes, sampling {len(heights)} heights: {heights}', file=sys.stderr)
    door = door_traces(heights, cache)
    ntx = 0
    same = 0
    raw_same = 0
    diffs = collections.Counter()
    examples = {}
    for h in heights:
        theirs = {x['txHash'].lower(): x['result'] for x in door[h]}
        for tx, mine in ours[h].items():
            ntx += 1
            th = theirs.get(tx.lower())
            if th is None:
                print(f'h={h} {tx}: not in door output', file=sys.stderr)
                diffs['missing tx'] += 1
                continue
            out = []
            diff(norm(mine), norm(th), '', out)
            if not out:
                same += 1
            for (path, kind, a, b) in out:
                import re
                p = re.sub(r'\[\d+\]', '[]', path)
                diffs[(p, kind)] += 1
                examples.setdefault((p, kind), (h, tx, a, b))
            if raw:
                if json.dumps(mine, separators=(',', ':')) == json.dumps(th, separators=(',', ':')):
                    raw_same += 1
    print(f'{ntx} txs over {len(heights)} heights: {same} identical after normalization' + (f', {raw_same} byte-identical' if raw else ''))
    for k, v in diffs.most_common():
        ex = examples.get(k)
        print(f'  {v:6d}  {k}  e.g. h={ex[0]} {ex[1]} ours={ex[2]!r} door={ex[3]!r}' if ex else f'  {v:6d}  {k}')

if __name__ == '__main__':
    main()
