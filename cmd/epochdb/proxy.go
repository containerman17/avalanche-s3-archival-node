package main

// `epochdb proxy` IS THE PUBLIC DOOR OF A FLEET BOX. The chains bind loopback
// ports; this one process listens for everyone, routes avalanchego-shaped URLs
// (`/ext/bc/<blockchainID>/rpc`, `/ext/bc/C/rpc`, `?token=` on the query) to
// the chain's port, PACES each client on independent budgets, and writes one
// JSON line per request. From the outside it is an avalanchego node; from the
// inside it is where the numbers for pricing the dimensions come from.
//
// BUDGETS ARE TOKEN BUCKETS THAT GO NEGATIVE: a request is charged what it
// cost (bytes out only after it ran), and the NEXT request of a client in
// debt WAITS until the bucket is back at zero, up to --max-wait, then gets a
// 429. A request is never refused for its own size, only for what came
// before it, so a big batch passes and is paid for on the next call. Pacing, not rejection: an indexer that overshoots slows
// down, it does not error out. Three dimensions today, measurable at the
// proxy: requests (batch elements count one each), trace calls (debug_trace*,
// the ones that re-execute), and bytes out. The node's own X-Cost headers
// (chunks cold/warm, EVM CPU) plug in here as more buckets when they exist.
//
// THE TOKEN IS ONE AND HARDCODED (user ruling 2026-09-07): with it a client
// has --boost times the anonymous budget. It is not a secret worth a system
// yet; it is the switch between "the public" and "our indexer".
//
// ponytail: one process, budgets in RAM, log to one append-only file; no
// keys, no persistence, no rotation. Add when the log is read by something.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

const proxyToken = "8f3c1a9e7d2b4c6f0a1e5d9b3c7f2a4e6b8d0c1f3a5e7b9d2c4f6a8e0b1d3f5a"

type bucket struct {
	tokens, rate, cap float64
	at                time.Time
}

// take charges n: a bucket may go negative, that is the debt the next
// request waits out. debt is how long until the bucket is back at zero.
func (b *bucket) take(n float64, now time.Time) {
	b.tokens = min(b.cap, b.tokens+b.rate*now.Sub(b.at).Seconds())
	b.at = now
	b.tokens -= n
}

func (b *bucket) debt(now time.Time) time.Duration {
	b.take(0, now)
	if b.tokens >= 0 {
		return 0
	}
	return time.Duration(-b.tokens / b.rate * float64(time.Second))
}

type client struct {
	mu                sync.Mutex
	reqs, traces, out bucket
}

type limits struct{ rps, traceRPS, bytesPerSec float64 }

func (l limits) newClient(now time.Time) *client {
	nb := func(r float64) bucket { return bucket{tokens: r * 10, rate: r, cap: r * 10, at: now} }
	return &client{reqs: nb(l.rps), traces: nb(l.traceRPS), out: nb(l.bytesPerSec)}
}

type proxy struct {
	routes  map[string]*httputil.ReverseProxy // blockchainID (and "C") -> chain
	names   map[string]string                 // blockchainID -> chain name, for the log
	anon    limits
	boost   float64
	maxWait time.Duration
	mu      sync.Mutex
	clients map[string]*client
	logmu   sync.Mutex
	logw    *bufio.Writer
}

func proxyMain(args []string) {
	fs := flag.NewFlagSet("proxy", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:8545", "listen address (cloudflared or a TLS terminator sits in front)")
	ladder := fs.String("ladder", "/data/epochdb-v0/ladder.tsv", "name<TAB>port<TAB>blockchainID per line; the routing table")
	logPath := fs.String("log", "/data/epochdb-v0/proxy.log", "one JSON line per request")
	rps := fs.Float64("rps", 5, "anonymous budget: requests per second (batch elements count one each)")
	traceRPS := fs.Float64("trace-rps", 0.5, "anonymous budget: debug_trace* calls per second")
	mbps := fs.Float64("mbps-out", 2, "anonymous budget: megabytes out per second")
	boost := fs.Float64("boost", 100, "the token's multiplier over the anonymous budgets")
	maxWait := fs.Duration("max-wait", 10*time.Second, "longest a paced request waits before 429")
	fs.Parse(args)

	p := &proxy{
		routes: map[string]*httputil.ReverseProxy{}, names: map[string]string{},
		anon: limits{*rps, *traceRPS, *mbps * 1e6}, boost: *boost, maxWait: *maxWait,
		clients: map[string]*client{},
	}
	raw, err := os.ReadFile(*ladder)
	if err != nil {
		log.Fatalf("epochdb: proxy: %v", err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		f := strings.Split(line, "\t")
		if len(f) < 3 {
			continue
		}
		rp := newRoute("127.0.0.1:" + f[1])
		p.routes[f[2]], p.names[f[2]] = rp, f[0]
		if f[2] == "C" {
			// avalanchego serves mainnet C under its blockchainID too.
			p.routes["2q9e4r6Mu3U68nU1fYjgbR6JvwrRx36CohpAX5UQxse55x1Q5"] = rp
			p.names["2q9e4r6Mu3U68nU1fYjgbR6JvwrRx36CohpAX5UQxse55x1Q5"] = f[0]
		}
	}
	lf, err := os.OpenFile(*logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatalf("epochdb: proxy: %v", err)
	}
	p.logw = bufio.NewWriter(lf)
	go func() {
		for range time.Tick(time.Second) {
			p.logmu.Lock()
			p.logw.Flush()
			p.logmu.Unlock()
		}
	}()
	go p.sweep()
	log.Printf("epochdb: proxy: %d chains on %s, anonymous %g rps / %g trace rps / %g MB/s, token x%g",
		len(p.names), *listen, *rps, *traceRPS, *mbps, *boost)
	log.Fatal(http.ListenAndServe(*listen, p))
}

// newRoute forwards to one chain's JSON-RPC root: the path, the query (the
// token above all) and the Host never reach the node.
func newRoute(host string) *httputil.ReverseProxy {
	u := &url.URL{Scheme: "http", Host: host}
	rp := httputil.NewSingleHostReverseProxy(u)
	rp.Director = func(r *http.Request) {
		r.URL.Scheme, r.URL.Host, r.URL.Path, r.URL.RawQuery, r.Host = "http", host, "/", "", host
	}
	return rp
}

// sweep forgets clients not seen for an hour: their buckets are full again by then.
func (p *proxy) sweep() {
	for range time.Tick(10 * time.Minute) {
		p.mu.Lock()
		for k, c := range p.clients {
			if time.Since(c.reqs.at) > time.Hour {
				delete(p.clients, k)
			}
		}
		p.mu.Unlock()
	}
}

type countingWriter struct {
	http.ResponseWriter
	status int
	n      int64
}

func (w *countingWriter) WriteHeader(s int) { w.status = s; w.ResponseWriter.WriteHeader(s) }
func (w *countingWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.n += int64(n)
	return n, err
}

// countMethods reads a JSON-RPC body (one call or a batch) and returns the
// element count, how many are debug_trace*, and the method names for the log.
func countMethods(body []byte) (n, traces int, methods []string) {
	var one struct {
		Method string `json:"method"`
	}
	var many []struct {
		Method string `json:"method"`
	}
	if bytes.HasPrefix(bytes.TrimSpace(body), []byte("[")) && json.Unmarshal(body, &many) == nil {
		for _, m := range many {
			methods = append(methods, m.Method)
		}
	} else if json.Unmarshal(body, &one) == nil {
		methods = []string{one.Method}
	}
	n = max(1, len(methods))
	for _, m := range methods {
		if strings.HasPrefix(m, "debug_trace") {
			traces++
		}
	}
	return
}

func (p *proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	// /ext/bc/<id>/rpc, exactly as avalanchego. Anything else is a 404 with
	// no fingerprint of what runs behind.
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 4 || parts[0] != "ext" || parts[1] != "bc" || parts[3] != "rpc" {
		http.NotFound(w, r)
		return
	}
	rp, ok := p.routes[parts[2]]
	if !ok {
		http.NotFound(w, r)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 8<<20))
	if err != nil {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	n, traces, methods := countMethods(body)

	// Who: the token, else the client address (cloudflared and any proxy in
	// front put the real one in CF-Connecting-IP / X-Forwarded-For).
	who, lim := r.Header.Get("CF-Connecting-IP"), p.anon
	if who == "" {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			who = strings.TrimSpace(strings.Split(xff, ",")[0])
		} else {
			who, _, _ = net.SplitHostPort(r.RemoteAddr)
		}
	}
	tokened := r.URL.Query().Get("token") == proxyToken
	if tokened {
		who, lim = "token", limits{p.anon.rps * p.boost, p.anon.traceRPS * p.boost, p.anon.bytesPerSec * p.boost}
	}
	p.mu.Lock()
	c := p.clients[who]
	if c == nil {
		c = lim.newClient(start)
		p.clients[who] = c
	}
	p.mu.Unlock()

	// Pace: wait out the debt earlier requests left (in any dimension),
	// then charge this one and run it. A request is never refused for its
	// own size, only for what came before it, so a 100-element batch passes
	// and the client pays for it on its next call.
	c.mu.Lock()
	wait := max(c.reqs.debt(start), c.traces.debt(start), c.out.debt(start))
	c.mu.Unlock()
	var waited time.Duration
	if wait > 0 {
		if wait > p.maxWait {
			w.Header().Set("Retry-After", fmt.Sprintf("%d", int(wait.Seconds())+1))
			http.Error(w, "rate limited", http.StatusTooManyRequests)
			p.logLine(start, who, tokened, p.names[parts[2]], methods, 429, 0, 0, wait)
			return
		}
		time.Sleep(wait)
		waited = wait
	}
	c.mu.Lock()
	c.reqs.take(float64(n), start)
	c.traces.take(float64(traces), start)
	c.mu.Unlock()
	cw := &countingWriter{ResponseWriter: w, status: 200}
	rp.ServeHTTP(cw, r)
	c.mu.Lock()
	c.out.take(float64(cw.n), time.Now())
	c.mu.Unlock()
	p.logLine(start, who, tokened, p.names[parts[2]], methods, cw.status, len(body), cw.n, waited)
}

func (p *proxy) logLine(start time.Time, who string, tokened bool, chain string, methods []string, status int, in int, out int64, waited time.Duration) {
	rec := map[string]any{
		"ts": start.UTC().Format(time.RFC3339Nano), "who": who, "token": tokened, "chain": chain,
		"methods": methods, "status": status, "bytes_in": in, "bytes_out": out,
		"ms": time.Since(start).Milliseconds(), "waited_ms": waited.Milliseconds(),
	}
	b, _ := json.Marshal(rec)
	p.logmu.Lock()
	p.logw.Write(b)
	p.logw.WriteByte('\n')
	p.logmu.Unlock()
}
