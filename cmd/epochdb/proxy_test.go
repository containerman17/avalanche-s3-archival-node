package main

import (
	"bufio"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestProxyRoutesPacesAndLogs(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			t.Errorf("backend saw path %q, want /", r.URL.Path)
		}
		io.WriteString(w, `{"jsonrpc":"2.0","id":1,"result":"0x1"}`)
	}))
	defer backend.Close()
	u, _ := url.Parse(backend.URL)
	dir := t.TempDir()
	lf, _ := os.Create(filepath.Join(dir, "log"))
	p := &proxy{
		routes: map[string]*httputil.ReverseProxy{}, names: map[string]string{"X": "x"},
		anon: limits{rps: 2, traceRPS: 1, bytesPerSec: 1e6}, boost: 100, maxWait: 50 * time.Millisecond,
		clients: map[string]*client{},
	}
	p.routes["X"] = newRoute(u.Host)
	p.logw = bufio.NewWriter(lf)
	srv := httptest.NewServer(p)
	defer srv.Close()

	post := func(path, body string) int {
		res, err := http.Post(srv.URL+path, "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		return res.StatusCode
	}
	if got := post("/ext/bc/nope/rpc", `{}`); got != 404 {
		t.Fatalf("unknown chain: %d", got)
	}
	if got := post("/ext/info", `{}`); got != 404 {
		t.Fatalf("non-avalanchego path: %d", got)
	}
	// Anonymous burst is 10x rps = 20 request-elements; a batch of 30 is
	// served (charged after) and the NEXT one is in debt beyond max-wait.
	batch := "[" + strings.Repeat(`{"method":"eth_blockNumber"},`, 29) + `{"method":"eth_blockNumber"}]`
	if got := post("/ext/bc/X/rpc", batch); got != 200 {
		t.Fatalf("first batch: %d", got)
	}
	if got := post("/ext/bc/X/rpc", `{"method":"eth_blockNumber"}`); got != 429 {
		t.Fatalf("in debt: want 429, got %d", got)
	}
	// The token is another client with 100x the budget.
	if got := post("/ext/bc/X/rpc?token="+proxyToken, batch); got != 200 {
		t.Fatalf("token: %d", got)
	}
	p.logmu.Lock()
	p.logw.Flush()
	p.logmu.Unlock()
	logged, _ := os.ReadFile(lf.Name())
	if lines := strings.Count(string(logged), "\n"); lines != 3 {
		t.Fatalf("log lines: %d, want 3:\n%s", lines, logged)
	}
	if !strings.Contains(string(logged), `"status":429`) || !strings.Contains(string(logged), `"token":true`) {
		t.Fatalf("log misses the 429 or the token line:\n%s", logged)
	}
}
