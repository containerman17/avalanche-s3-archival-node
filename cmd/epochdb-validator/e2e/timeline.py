#!/usr/bin/env python3
"""Per-height cycle timeline from the two ours nodes' chain logs (one wall clock).

usage: timeline.py LOGS_DIR   (the e2e --logs dir: <net>/<NodeID>/*.log)
"""
import bisect, glob, json, os, re, statistics, sys
from datetime import datetime

LINE = re.compile(r'^\[(\d\d-\d\d\|\d\d:\d\d:\d\d\.\d+)\]\s+\S*(\w+)\S*\s+<[^>]*>\s+(\S+)\s+(.*)$')
DUR = re.compile(r'^([\d.]+)(ns|µs|us|ms|s|m)$')

def dur(s):
    m = DUR.match(s)
    if not m: return 0.0
    v = float(m.group(1)); u = m.group(2)
    return v * {'ns': 1e-9, 'µs': 1e-6, 'us': 1e-6, 'ms': 1e-3, 's': 1, 'm': 60}[u]

def ts(s):
    return datetime.strptime('2026-' + s, '%Y-%m-%d|%H:%M:%S.%f').timestamp()

def load(node_dir):
    ev = {k: [] for k in ('accepted', 'built', 'wake', 'parsed', 'verified', 'getblock',
                          'pq_sent', 'pq_recv', 'chits_sent', 'chits_recv', 'wait_build', 'pvm_built', 'adding')}
    for f in sorted(glob.glob(os.path.join(node_dir, '*.log'))):
        base = os.path.basename(f)
        if base.startswith('main') or base.startswith('P') or base.startswith('X') or base.startswith('C'):
            continue
        for line in open(f, errors='replace'):
            m = LINE.match(line)
            if not m: continue
            t = ts(m.group(1)); msg = m.group(4)
            if msg.startswith('validator: '):
                kind, _, rest = msg[len('validator: '):].partition(' ')
                if kind not in ev: continue
                try: d = json.loads(rest)
                except Exception: continue
                ev[kind].append((t, d))
            elif msg.startswith('sent message'):
                if '"messageOp": "push_query"' in msg: ev['pq_sent'].append((t, None))
                elif '"messageOp": "chits"' in msg: ev['chits_sent'].append((t, None))
            elif msg.startswith('forwarding sync message to consensus'):
                if '"messageOp": "push_query"' in msg: ev['pq_recv'].append((t, None))
                elif '"messageOp": "chits"' in msg: ev['chits_recv'].append((t, None))
            elif msg.startswith('built block {') or msg.startswith('adding block {'):
                mm = re.search(r'"blkID": "([^"]+)".*"height": (\d+)', msg)
                if mm: ev['pvm_built' if msg.startswith('built') else 'adding'].append((t, (mm.group(1), int(mm.group(2)))))
            elif msg.startswith('Waiting until we should build a block'):
                mm = re.search(r'"duration": "([^"]+)"', msg)
                ev['wait_build'].append((t, dur(mm.group(1)) if mm else 0))
    for k in ev: ev[k].sort(key=lambda x: x[0])
    return ev

def first_after(lst, t, lo=None):
    keys = [x[0] for x in lst]
    i = bisect.bisect_right(keys, t)
    if i < len(lst): return lst[i][0]
    return None

def last_before(lst, t):
    keys = [x[0] for x in lst]
    i = bisect.bisect_left(keys, t) - 1
    if i >= 0: return lst[i]
    return None

def by_height(lst):
    out = {}
    for t, d in lst:
        h = d.get('height')
        if h is not None and h not in out: out[h] = (t, d)
    return out

def pct(xs, p):
    if not xs: return float('nan')
    xs = sorted(xs); return xs[min(int(len(xs) * p), len(xs) - 1)]

def main():
    root = sys.argv[1]
    nodes = {}
    for d in sorted(glob.glob(os.path.join(root, 'NodeID-*'))):
        ev = load(d)
        if ev['accepted']: nodes[os.path.basename(d)] = ev
    names = list(nodes)
    print('nodes:', names)
    acc = {n: by_height(nodes[n]['accepted']) for n in names}
    built = {n: by_height(nodes[n]['built']) for n in names}
    parsed = {n: by_height(nodes[n]['parsed']) for n in names}
    verified = {n: by_height(nodes[n]['verified']) for n in names}
    heights = sorted(set.intersection(*[set(acc[n]) for n in names]))
    lo = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    hi = int(sys.argv[3]) if len(sys.argv) > 3 else 1 << 62
    heights = [h for h in heights if lo <= h <= hi]
    segs = {}
    def add(k, v):
        if v is not None: segs.setdefault(k, []).append(v)
    rows = []
    # The proposer of h: the node whose proposervm "built block" id at h the
    # other node added to consensus (both nodes may build h; one block wins).
    adding = {n: {(id_, hh) for _, (id_, hh) in nodes[n]['adding']} for n in names}
    for h in heights:
        P, pvm_t = None, None
        for n in names:
            others = [m for m in names if m != n]
            for t, (id_, hh) in nodes[n]['pvm_built']:
                if hh == h and any((id_, hh) in adding[m] for m in others):
                    P, pvm_t = n, t
                    break
            if P: break
        if P is None or h - 1 not in acc[P]: continue
        Q = [n for n in names if n != P]
        if not Q: continue
        Q = Q[0]
        if h not in parsed[Q] or h not in verified[Q]: continue
        e = nodes[P]; eq = nodes[Q]
        a_prev = acc[P][h - 1][0]
        bl = last_before([x for x in e['built'] if x[1].get('height') == h], pvm_t)
        if bl is None: continue
        b_done, bd = bl; b_start = b_done - dur(bd['took'])
        w = last_before(e['wake'], b_start); wake = w[0] if w else None
        pq_sent = first_after(e['pq_sent'], b_done)
        pq_recv = first_after(eq['pq_recv'], pq_sent) if pq_sent else None
        p_done, pd = parsed[Q][h]; p_start = p_done - dur(pd['took'])
        v_done, vd = verified[Q][h]; v_start = v_done - dur(vd['took'])
        ch_sent = first_after(eq['chits_sent'], v_done)
        ch_recv = first_after(e['chits_recv'], ch_sent) if ch_sent else None
        a_p = acc[P][h][0]; a_q = acc[Q][h][0]
        add('cycle accept(h-1)->accept(h) [P]', a_p - a_prev)
        add('accept(h-1)->wake', (wake - a_prev) if wake else None)
        add('wake->build start', (b_start - wake) if wake else None)
        add('build (BuildBlock wall)', b_done - b_start)
        add('build done->PushQuery sent', (pq_sent - b_done) if pq_sent else None)
        add('PushQuery sent->peer handler recv', (pq_recv - pq_sent) if pq_sent and pq_recv else None)
        add('peer recv->parse start', (p_start - pq_recv) if pq_recv else None)
        add('parse', p_done - p_start)
        add('parse done->verify start', v_start - p_done)
        add('verify', v_done - v_start)
        add('verify done->chits sent', (ch_sent - v_done) if ch_sent else None)
        add('chits sent->chits recv [P]', (ch_recv - ch_sent) if ch_sent and ch_recv else None)
        add('chits recv->accept(h) [P]', (a_p - ch_recv) if ch_recv else None)
        add('build done->accept(h) [P]', a_p - b_done)
        add('accept(h) Q - P', a_q - a_p)
        add('accept took [P]', dur(acc[P][h][1].get('took', '0s')))
        add('accept took [Q]', dur(acc[Q][h][1].get('took', '0s')))
        add('build start - accept(h-1) (pipelining: negative = built before parent accepted)', b_start - a_prev)
        rows.append((h, P[-4:], a_prev, wake, b_start, b_done, pq_sent, pq_recv, p_start, p_done, v_start, v_done, ch_sent, ch_recv, a_p, a_q, bd.get('included')))
    print(f'heights analysed: {len(rows)} of {len(heights)}')
    print(f'{"segment":<78} {"median":>9} {"p90":>9} {"n":>5}')
    for k, v in segs.items():
        print(f'{k:<78} {statistics.median(v)*1e3:8.1f}ms {pct(v,.9)*1e3:8.1f}ms {len(v):5d}')
    # per-node build/verify/accept counts and the retry waits
    for n in names:
        e = nodes[n]
        b = [dur(d['took']) for _, d in e['built']]
        v = [dur(d['took']) for _, d in e['verified']]
        a = [dur(d.get('took', '0s')) for _, d in e['accepted']]
        g = [dur(d['gap']) for _, d in e['wake']]
        pw = [dur(d['poolWait']) for _, d in e['wake']]
        gb = [dur(d['took']) for _, d in e['getblock']]
        wb = [d for _, d in e['wait_build']]
        print(f'{n}: builds {len(b)} p50 {pct(b,.5)*1e3:.1f} p99 {pct(b,.99)*1e3:.1f} ms; verifies {len(v)} p50 {pct(v,.5)*1e3:.1f} p99 {pct(v,.99)*1e3:.1f}; '
              f'accepts {len(a)} p50 {pct(a,.5)*1e3:.1f} p99 {pct(a,.99)*1e3:.1f}; wakes {len(g)} gap p50 {pct(g,.5)*1e3:.1f} p90 {pct(g,.9)*1e3:.1f} poolWait p50 {pct(pw,.5)*1e3:.1f}; '
              f'getblock {len(gb)} p50 {pct(gb,.5)*1e3:.2f} ms; proposervm slot waits {len(wb)} p50 {pct(wb,.5)*1e3:.0f} ms max {max(wb)*1e3 if wb else 0:.0f}')
    if '-v' in sys.argv:
        t0 = rows[0][2]
        print('h P a(h-1) wake bstart bdone pqsent pqrecv pstart pdone vstart vdone chsent chrecv accP accQ txs (s since first)')
        for r in rows:
            print(r[0], r[1], ' '.join(f'{(x - t0):7.3f}' if x else '   -   ' for x in r[2:16]), r[16])

main()
