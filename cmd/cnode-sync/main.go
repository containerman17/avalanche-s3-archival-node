// cnode-sync: the C-chain state sync and nothing else. Learns the validator
// set from the public P-chain RPC, connects to as many validators as it can
// with a throwaway identity, takes the state summary a stake-weighted
// majority agrees on, then pulls every account, storage slot and code blob
// through coreth's own leaf syncer (range proofs verified per response),
// fanned out over all peers with a per-peer cap, straight into the flat
// files the Rust node loads (accounts.bin, storage.bin, code.bin, meta.json).
// meta.json is written last: its presence is the DONE marker.
//
//	cnode-sync -out DIR [-node https://api.avax.network] [-workers 256] [-per-peer 3] [-request-size 1024] [-connect 120s]
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ava-labs/avalanchego/api/info"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/message"
	"github.com/ava-labs/avalanchego/network"
	"github.com/ava-labs/avalanchego/network/p2p"
	p2ppb "github.com/ava-labs/avalanchego/proto/pb/p2p"
	avacommon "github.com/ava-labs/avalanchego/snow/engine/common"
	"github.com/ava-labs/avalanchego/snow/validators"
	"github.com/ava-labs/avalanchego/staking"
	"github.com/ava-labs/avalanchego/subnets"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/utils/set"
	"github.com/ava-labs/avalanchego/version"
	"github.com/ava-labs/avalanchego/vms/platformvm"
	"github.com/prometheus/client_golang/prometheus"

	atomicsync "github.com/ava-labs/avalanchego/graft/coreth/plugin/evm/atomic/sync"
	evmmessage "github.com/ava-labs/avalanchego/graft/evm/message"
	syncclient "github.com/ava-labs/avalanchego/graft/evm/sync/client"
	"github.com/ava-labs/avalanchego/graft/evm/sync/client/stats"
	"github.com/ava-labs/avalanchego/graft/evm/sync/leaf"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"
)

var cChainID = ids.FromStringOrPanic("2q9e4r6Mu3U68nU1fYjgbR6JvwrRx36CohpAX5UQxse55x1Q5")

const requestTimeout = 15 * time.Second

func main() {
	out := flag.String("out", "", "output dir (accounts.bin, storage.bin, code.bin, meta.json)")
	node := flag.String("node", "https://api.avax.network", "public node for platform.getCurrentValidators and info.peers")
	workers := flag.Int("workers", 256, "concurrent leaf requests in flight")
	perPeer := flag.Int("per-peer", 3, "outstanding requests per peer")
	reqSize := flag.Int("request-size", 1024, "leafs per request")
	connect := flag.Duration("connect", 90*time.Second, "how long to gather peers before asking for summaries")
	flag.Parse()
	if *out == "" {
		log.Fatal("-out is required")
	}
	if err := run(*out, *node, *workers, *perPeer, uint16(*reqSize), *connect); err != nil {
		log.Fatalf("cnode-sync: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Peers.

type peers struct {
	net     network.Network
	creator message.OutboundMsgBuilder
	weights map[ids.NodeID]uint64
	total   uint64

	mu          sync.Mutex
	connected   map[ids.NodeID]int // outstanding requests
	failures    map[ids.NodeID]int
	nextReq     uint32
	routes      map[uint32]chan []byte
	frontier    map[uint32]chan frontierAnswer
	accepted    map[uint32]chan acceptedAnswer
	connectedCh chan struct{}
}

type frontierAnswer struct {
	node    ids.NodeID
	summary []byte
}
type acceptedAnswer struct {
	node ids.NodeID
	ids  []ids.ID
}

func (p *peers) Connected(nodeID ids.NodeID, _ *version.Application, _ ids.ID) {
	if _, ok := p.weights[nodeID]; !ok {
		return
	}
	p.mu.Lock()
	if _, ok := p.connected[nodeID]; !ok {
		p.connected[nodeID] = 0
	}
	p.mu.Unlock()
	select {
	case p.connectedCh <- struct{}{}:
	default:
	}
}

func (p *peers) Disconnected(nodeID ids.NodeID) {
	p.mu.Lock()
	delete(p.connected, nodeID)
	p.mu.Unlock()
}

func (p *peers) HandleInbound(_ context.Context, msg *message.InboundMessage) {
	defer msg.OnFinishedHandling()
	switch msg.Op {
	case message.AppResponseOp:
		m, ok := msg.Message.(*p2ppb.AppResponse)
		if !ok {
			return
		}
		p.route(m.RequestId, m.AppBytes)
	case message.AppErrorOp:
		m, ok := msg.Message.(*p2ppb.AppError)
		if !ok {
			return
		}
		p.route(m.RequestId, nil)
	case message.StateSummaryFrontierOp:
		m, ok := msg.Message.(*p2ppb.StateSummaryFrontier)
		if !ok {
			return
		}
		p.mu.Lock()
		ch := p.frontier[m.RequestId]
		p.mu.Unlock()
		if ch != nil {
			select {
			case ch <- frontierAnswer{node: msg.NodeID, summary: m.Summary}:
			default:
			}
		}
	case message.AcceptedStateSummaryOp:
		m, ok := msg.Message.(*p2ppb.AcceptedStateSummary)
		if !ok {
			return
		}
		var got []ids.ID
		for _, b := range m.SummaryIds {
			if id, err := ids.ToID(b); err == nil {
				got = append(got, id)
			}
		}
		p.mu.Lock()
		ch := p.accepted[m.RequestId]
		p.mu.Unlock()
		if ch != nil {
			select {
			case ch <- acceptedAnswer{node: msg.NodeID, ids: got}:
			default:
			}
		}
	}
}

func (p *peers) route(reqID uint32, b []byte) {
	p.mu.Lock()
	ch := p.routes[reqID]
	delete(p.routes, reqID)
	p.mu.Unlock()
	if ch != nil {
		ch <- b
	}
}

func (p *peers) send(msg *message.OutboundMessage, to set.Set[ids.NodeID]) set.Set[ids.NodeID] {
	return p.net.Send(msg, avacommon.SendConfig{NodeIDs: to}, avaconstants.PrimaryNetworkID, subnets.NoOpAllower)
}

func (p *peers) connectedList() []ids.NodeID {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]ids.NodeID, 0, len(p.connected))
	for id := range p.connected {
		out = append(out, id)
	}
	return out
}

func (p *peers) connectedWeight() (int, uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var w uint64
	for id := range p.connected {
		w += p.weights[id]
	}
	return len(p.connected), w
}

// pick returns the connected peer with the fewest outstanding requests that
// is under the cap, reserving a slot; ok=false when every peer is full.
func (p *peers) pick(cap int) (ids.NodeID, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var best ids.NodeID
	bestN := cap
	for id, n := range p.connected {
		if n < bestN && p.failures[id] < 8 {
			best, bestN = id, n
		}
	}
	if bestN == cap {
		return ids.EmptyNodeID, false
	}
	p.connected[best]++
	return best, true
}

func (p *peers) reserve(id ids.NodeID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.connected[id]; !ok {
		return false
	}
	p.connected[id]++
	return true
}

func (p *peers) release(id ids.NodeID) {
	p.mu.Lock()
	if n, ok := p.connected[id]; ok && n > 0 {
		p.connected[id] = n - 1
	}
	p.mu.Unlock()
}

// ---------------------------------------------------------------------------
// syncclient.Network over the peers.

type netClient struct {
	p       *peers
	perPeer int
}

var errNoPeer = errors.New("no peer available")

func (n *netClient) SendSyncedAppRequestAny(ctx context.Context, request []byte) ([]byte, ids.NodeID, error) {
	for {
		id, ok := n.p.pick(n.perPeer)
		if ok {
			b, err := n.request(ctx, id, request)
			return b, id, err
		}
		select {
		case <-ctx.Done():
			return nil, ids.EmptyNodeID, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (n *netClient) SendSyncedAppRequest(ctx context.Context, nodeID ids.NodeID, request []byte) ([]byte, error) {
	if !n.p.reserve(nodeID) {
		return nil, errNoPeer
	}
	return n.request(ctx, nodeID, request)
}

func (n *netClient) request(ctx context.Context, id ids.NodeID, request []byte) ([]byte, error) {
	defer n.p.release(id)
	p := n.p
	p.mu.Lock()
	p.nextReq++
	reqID := p.nextReq
	ch := make(chan []byte, 1)
	p.routes[reqID] = ch
	p.mu.Unlock()
	msg, err := p.creator.AppRequest(cChainID, reqID, requestTimeout, request)
	if err != nil {
		return nil, err
	}
	if sent := p.send(msg, set.Of(id)); sent.Len() == 0 {
		p.mu.Lock()
		delete(p.routes, reqID)
		p.mu.Unlock()
		return nil, fmt.Errorf("%w: send to %s", errNoPeer, id)
	}
	select {
	case b := <-ch:
		if b == nil {
			return nil, fmt.Errorf("app error from %s", id)
		}
		return b, nil
	case <-time.After(requestTimeout):
		p.mu.Lock()
		delete(p.routes, reqID)
		p.mu.Unlock()
		return nil, fmt.Errorf("timeout from %s", id)
	case <-ctx.Done():
		p.mu.Lock()
		delete(p.routes, reqID)
		p.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (n *netClient) RegisterResponse(nodeID ids.NodeID, _ float64) {
	n.p.mu.Lock()
	n.p.failures[nodeID] = 0
	n.p.mu.Unlock()
}

func (n *netClient) RegisterFailure(nodeID ids.NodeID) {
	n.p.mu.Lock()
	n.p.failures[nodeID]++
	n.p.mu.Unlock()
}

func (n *netClient) P2PNetwork() *p2p.Network { return nil }

func (n *netClient) Sample(_ context.Context, limit int) []ids.NodeID {
	l := n.p.connectedList()
	if len(l) > limit {
		l = l[:limit]
	}
	return l
}

// ---------------------------------------------------------------------------
// Output: per-shard files (first byte of the account hash), sorted and
// concatenated at the end so accounts.bin and storage.bin come out in key order.

type shardWriter struct {
	mu    sync.Mutex
	files [256]*os.File
	dir   string
	name  string
}

func newShardWriter(dir, name string) (*shardWriter, error) {
	w := &shardWriter{dir: dir, name: name}
	for i := range w.files {
		f, err := os.Create(filepath.Join(dir, fmt.Sprintf("%s.%02x.part", name, i)))
		if err != nil {
			return nil, err
		}
		w.files[i] = f
	}
	return w, nil
}

func (w *shardWriter) write(shard byte, rec []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := w.files[shard].Write(rec)
	return err
}

// finish sorts every shard by its leading key bytes and appends it to name.
func (w *shardWriter) finish(recSize int, keyLen int) (uint64, error) {
	out, err := os.Create(filepath.Join(w.dir, w.name))
	if err != nil {
		return 0, err
	}
	defer out.Close()
	var n uint64
	for i, f := range w.files {
		f.Close()
		path := filepath.Join(w.dir, fmt.Sprintf("%s.%02x.part", w.name, i))
		b, err := os.ReadFile(path)
		if err != nil {
			return 0, err
		}
		if len(b)%recSize != 0 {
			return 0, fmt.Errorf("%s: %d bytes is not a multiple of %d", path, len(b), recSize)
		}
		cnt := len(b) / recSize
		idx := make([]int, cnt)
		for j := range idx {
			idx[j] = j
		}
		sort.Slice(idx, func(a, c int) bool {
			return bytes.Compare(b[idx[a]*recSize:idx[a]*recSize+keyLen], b[idx[c]*recSize:idx[c]*recSize+keyLen]) < 0
		})
		sorted := make([]byte, 0, len(b))
		for _, j := range idx {
			sorted = append(sorted, b[j*recSize:(j+1)*recSize]...)
		}
		if _, err := out.Write(sorted); err != nil {
			return 0, err
		}
		n += uint64(cnt)
		os.Remove(path)
	}
	return n, out.Sync()
}

// ---------------------------------------------------------------------------
// Leaf tasks.

type slimAccount struct {
	Nonce    uint64
	Balance  []byte
	Root     []byte
	CodeHash []byte
	Rest     []rlp.RawValue `rlp:"tail"`
}

type syncState struct {
	accounts *shardWriter
	storage  *shardWriter
	tasks    chan leaf.SyncTask
	pending  sync.WaitGroup // storage tasks queued but not finished
	codeMu   sync.Mutex
	codes    map[common.Hash]struct{}
	nAcc     atomic.Uint64
	nSlot    atomic.Uint64
	nStor    atomic.Uint64
	nReq     atomic.Uint64
}

type accountTask struct {
	s          *syncState
	root       common.Hash
	start, end []byte
}

func (t *accountTask) Root() common.Hash              { return t.root }
func (t *accountTask) Account() common.Hash           { return common.Hash{} }
func (t *accountTask) Start() []byte                  { return t.start }
func (t *accountTask) End() []byte                    { return t.end }
func (t *accountTask) NodeType() evmmessage.NodeType  { return evmmessage.StateTrieNode }
func (t *accountTask) OnStart() (bool, error)         { return false, nil }
func (t *accountTask) OnFinish(context.Context) error { return nil }
func (t *accountTask) OnLeafs(_ context.Context, keys, vals [][]byte) error {
	t.s.nReq.Add(1)
	for i, k := range keys {
		if len(k) != 32 {
			return fmt.Errorf("account key of %d bytes", len(k))
		}
		var a slimAccount
		if err := rlp.DecodeBytes(vals[i], &a); err != nil {
			return fmt.Errorf("account %x: %w", k, err)
		}
		rec := make([]byte, 105)
		copy(rec[:32], k)
		binary.LittleEndian.PutUint64(rec[32:40], a.Nonce)
		copy(rec[72-len(a.Balance):72], a.Balance)
		if len(a.CodeHash) == 0 {
			copy(rec[72:104], types.EmptyCodeHash[:])
		} else {
			copy(rec[72:104], a.CodeHash)
			h := common.BytesToHash(a.CodeHash)
			t.s.codeMu.Lock()
			t.s.codes[h] = struct{}{}
			t.s.codeMu.Unlock()
		}
		if len(a.Rest) > 0 {
			var multi bool
			if err := rlp.DecodeBytes(a.Rest[0], &multi); err != nil {
				return fmt.Errorf("account %x multicoin: %w", k, err)
			}
			if multi {
				rec[104] = 1
			}
		}
		if err := t.s.accounts.write(k[0], rec); err != nil {
			return err
		}
		t.s.nAcc.Add(1)
		if len(a.Root) == 32 && common.BytesToHash(a.Root) != types.EmptyRootHash {
			// ponytail: identical storage roots are fetched once per account, not shared; dedupe if the duplicates ever dominate.
			t.s.pending.Add(1)
			t.s.nStor.Add(1)
			go func(root common.Hash, acc common.Hash) { t.s.tasks <- &storageTask{s: t.s, root: root, account: acc} }(common.BytesToHash(a.Root), common.BytesToHash(k))
		}
	}
	return nil
}

type storageTask struct {
	s       *syncState
	root    common.Hash
	account common.Hash
}

func (t *storageTask) Root() common.Hash              { return t.root }
func (t *storageTask) Account() common.Hash           { return t.account }
func (t *storageTask) Start() []byte                  { return nil }
func (t *storageTask) End() []byte                    { return nil }
func (t *storageTask) NodeType() evmmessage.NodeType  { return evmmessage.StateTrieNode }
func (t *storageTask) OnStart() (bool, error)         { return false, nil }
func (t *storageTask) OnFinish(context.Context) error { t.s.pending.Done(); return nil }
func (t *storageTask) OnLeafs(_ context.Context, keys, vals [][]byte) error {
	t.s.nReq.Add(1)
	for i, k := range keys {
		if len(k) != 32 {
			return fmt.Errorf("slot key of %d bytes", len(k))
		}
		var v []byte
		if err := rlp.DecodeBytes(vals[i], &v); err != nil {
			return fmt.Errorf("slot %x/%x: %w", t.account, k, err)
		}
		if len(v) > 32 {
			return fmt.Errorf("slot %x/%x: %d-byte value", t.account, k, len(v))
		}
		rec := make([]byte, 96)
		copy(rec[:32], t.account[:])
		copy(rec[32:64], k)
		copy(rec[96-len(v):96], v)
		if err := t.s.storage.write(t.account[0], rec); err != nil {
			return err
		}
		t.s.nSlot.Add(1)
	}
	return nil
}

// ---------------------------------------------------------------------------

func run(out, nodeURI string, workers, perPeer int, reqSize uint16, connect time.Duration) error {
	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}
	ctx := context.Background()

	// 1. Validators and their weights: the only thing we take from the P-chain.
	vdrs, err := platformvm.NewClient(nodeURI).GetCurrentValidators(ctx, avaconstants.PrimaryNetworkID, nil)
	if err != nil {
		return fmt.Errorf("platform.getCurrentValidators: %w", err)
	}
	p := &peers{
		weights:     map[ids.NodeID]uint64{},
		connected:   map[ids.NodeID]int{},
		failures:    map[ids.NodeID]int{},
		routes:      map[uint32]chan []byte{},
		frontier:    map[uint32]chan frontierAnswer{},
		accepted:    map[uint32]chan acceptedAnswer{},
		connectedCh: make(chan struct{}, 1),
	}
	var vdrIDs []ids.NodeID
	for _, v := range vdrs {
		p.weights[v.NodeID] += v.Weight
		p.total += v.Weight
		vdrIDs = append(vdrIDs, v.NodeID)
	}
	log.Printf("validators: %d, total weight %d", len(p.weights), p.total)

	peerInfos, err := info.NewClient(nodeURI).Peers(ctx, nil)
	if err != nil {
		return fmt.Errorf("info.peers: %w", err)
	}
	log.Printf("info.peers: %d entry points", len(peerInfos))

	// 2. The network: a throwaway identity, the validator set loaded so the
	// network wants every validator's signed IP from gossip and dials it.
	mgr := validators.NewManager()
	netCfg, err := network.NewTestNetworkConfig(prometheus.NewRegistry(), avaconstants.MainnetID, mgr, set.Set[ids.ID]{})
	if err != nil {
		return fmt.Errorf("network config: %w", err)
	}
	cert, err := staking.ParseCertificate(netCfg.TLSConfig.Certificates[0].Leaf.Raw)
	if err != nil {
		return err
	}
	netCfg.MyNodeID = ids.NodeIDFromCert(cert)
	net, err := network.NewTestNetwork(logging.NoLog{}, prometheus.NewRegistry(), netCfg, p)
	if err != nil {
		return fmt.Errorf("NewTestNetwork: %w", err)
	}
	p.net = net
	creator, err := message.NewCreator(prometheus.NewRegistry(), avaconstants.DefaultNetworkCompressionType, avaconstants.DefaultNetworkMaximumInboundTimeout)
	if err != nil {
		return err
	}
	p.creator = creator
	dispatchErr := make(chan error, 1)
	go func() { dispatchErr <- net.Dispatch() }()
	for _, v := range vdrs {
		_ = mgr.AddStaker(avaconstants.PrimaryNetworkID, v.NodeID, nil, v.TxID, v.Weight)
	}
	for _, pi := range peerInfos {
		ap := pi.IP
		if pi.PublicIP.IsValid() {
			ap = pi.PublicIP
		}
		if ap.IsValid() && ap != (netip.AddrPort{}) {
			net.ManuallyTrack(pi.ID, ap)
		}
	}
	deadline := time.Now().Add(connect)
	for time.Now().Before(deadline) {
		select {
		case err := <-dispatchErr:
			return fmt.Errorf("network: %w", err)
		case <-time.After(5 * time.Second):
		}
		n, w := p.connectedWeight()
		log.Printf("peers: %d validators connected, %.1f%% of stake", n, 100*float64(w)/float64(p.total))
	}
	if n, _ := p.connectedWeight(); n == 0 {
		return errors.New("no validator connected")
	}

	// 3. The summary: frontier from everyone, then acceptance of the best height.
	summary, err := chooseSummary(p)
	if err != nil {
		return err
	}
	log.Printf("SUMMARY: height %d hash %s root %s (agreed by the stake below)", summary.BlockNumber, summary.BlockHash, summary.BlockRoot)

	// 4. Leafs.
	st := &syncState{tasks: make(chan leaf.SyncTask, 1<<16), codes: map[common.Hash]struct{}{}}
	if st.accounts, err = newShardWriter(out, "accounts.bin"); err != nil {
		return err
	}
	if st.storage, err = newShardWriter(out, "storage.bin"); err != nil {
		return err
	}
	client := syncclient.New(&syncclient.Config{Network: &netClient{p: p, perPeer: perPeer}, Codec: evmmessage.CorethCodec, Stats: stats.NewNoOpStats()})
	syncer := leaf.NewCallbackSyncer(client, st.tasks, &leaf.SyncerConfig{RequestSize: reqSize, NumWorkers: workers, LeafsRequestType: evmmessage.CorethLeafsRequestType})
	syncCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	// 256 account shards, then every storage trie the accounts name; the
	// channel closes when the shards are done and no storage task is pending.
	var shards sync.WaitGroup
	for b := 0; b < 256; b++ {
		shards.Add(1)
		start := []byte{byte(b)}
		start = append(start, make([]byte, 31)...)
		end := bytes.Repeat([]byte{0xff}, 32)
		end[0] = byte(b)
		t := &shardTask{accountTask: accountTask{s: st, root: summary.BlockRoot, start: start, end: end}, wg: &shards}
		st.tasks <- t
	}
	go func() {
		shards.Wait()
		st.pending.Wait()
		close(st.tasks)
	}()
	t0 := time.Now()
	done := make(chan error, 1)
	go func() { done <- syncer.Sync(syncCtx) }()
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		select {
		case err := <-done:
			if err != nil {
				return fmt.Errorf("leaf sync: %w", err)
			}
			goto finished
		case err := <-dispatchErr:
			return fmt.Errorf("network: %w", err)
		case <-tick.C:
			n, w := p.connectedWeight()
			el := time.Since(t0).Seconds()
			log.Printf("sync: %d accounts, %d slots (%d storage tries named), %d responses, %.0f leafs/s, peers %d (%.1f%% stake), %.0fs",
				st.nAcc.Load(), st.nSlot.Load(), st.nStor.Load(), st.nReq.Load(), float64(st.nAcc.Load()+st.nSlot.Load())/el, n, 100*float64(w)/float64(p.total), el)
		}
	}
finished:
	log.Printf("leafs done in %.0fs: %d accounts, %d slots; sorting shards", time.Since(t0).Seconds(), st.nAcc.Load(), st.nSlot.Load())
	nAcc, err := st.accounts.finish(105, 32)
	if err != nil {
		return fmt.Errorf("accounts.bin: %w", err)
	}
	nSlot, err := st.storage.finish(96, 64)
	if err != nil {
		return fmt.Errorf("storage.bin: %w", err)
	}

	// 5. Code.
	hashes := make([]common.Hash, 0, len(st.codes))
	for h := range st.codes {
		hashes = append(hashes, h)
	}
	sort.Slice(hashes, func(i, j int) bool { return bytes.Compare(hashes[i][:], hashes[j][:]) < 0 })
	cf, err := os.Create(filepath.Join(out, "code.bin"))
	if err != nil {
		return err
	}
	log.Printf("code: %d hashes", len(hashes))
	var codeMu sync.Mutex
	var cg sync.WaitGroup
	sem := make(chan struct{}, workers)
	var codeErr atomic.Value
	for i := 0; i < len(hashes); i += evmmessage.MaxCodeHashesPerRequest {
		batch := hashes[i:min(i+evmmessage.MaxCodeHashesPerRequest, len(hashes))]
		cg.Add(1)
		sem <- struct{}{}
		go func(batch []common.Hash) {
			defer cg.Done()
			defer func() { <-sem }()
			codes, err := client.GetCode(ctx, batch)
			if err != nil {
				codeErr.Store(err)
				return
			}
			codeMu.Lock()
			defer codeMu.Unlock()
			for j, c := range codes {
				if crypto.Keccak256Hash(c) != batch[j] {
					codeErr.Store(fmt.Errorf("code %s: hash mismatch", batch[j]))
					return
				}
				var head [36]byte
				copy(head[:32], batch[j][:])
				binary.LittleEndian.PutUint32(head[32:], uint32(len(c)))
				cf.Write(head[:])
				cf.Write(c)
			}
		}(batch)
	}
	cg.Wait()
	if e := codeErr.Load(); e != nil {
		return e.(error)
	}
	if err := cf.Sync(); err != nil {
		return err
	}
	cf.Close()

	// 6. meta.json last: the DONE marker.
	meta := map[string]any{
		"height": summary.BlockNumber, "hash": summary.BlockHash, "state_root": summary.BlockRoot,
		"head_height": summary.BlockNumber, "accounts": nAcc, "slots": nSlot, "codes": len(hashes),
		"synced_at_ms": time.Now().UnixMilli(),
	}
	mb, _ := json.MarshalIndent(meta, "", "  ")
	if err := os.WriteFile(filepath.Join(out, "meta.json.tmp"), mb, 0o644); err != nil {
		return err
	}
	if err := os.Rename(filepath.Join(out, "meta.json.tmp"), filepath.Join(out, "meta.json")); err != nil {
		return err
	}
	log.Printf("DONE: %s", mb)
	net.StartClose()
	return nil
}

type shardTask struct {
	accountTask
	wg *sync.WaitGroup
}

func (t *shardTask) OnFinish(ctx context.Context) error {
	t.wg.Done()
	return t.accountTask.OnFinish(ctx)
}

// chooseSummary: the frontier from every connected validator, grouped by
// summary; then GetAcceptedStateSummary at the best height to everyone; the
// summary is accepted when at least two thirds of the answering stake names
// it and the answering stake is at least a fifth of the network.
func chooseSummary(p *peers) (*evmmessage.BlockSyncSummary, error) {
	provider := &atomicsync.SummaryProvider{}
	list := p.connectedList()
	p.mu.Lock()
	p.nextReq++
	reqID := p.nextReq
	fch := make(chan frontierAnswer, len(list)+16)
	p.frontier[reqID] = fch
	p.mu.Unlock()
	msg, err := p.creator.GetStateSummaryFrontier(cChainID, reqID, requestTimeout)
	if err != nil {
		return nil, err
	}
	sent := p.send(msg, set.Of(list...))
	log.Printf("frontier: asked %d validators", sent.Len())
	type cand struct {
		summary *atomicsync.Summary
		weight  uint64
		n       int
	}
	cands := map[ids.ID]*cand{}
	timeout := time.After(requestTimeout)
	got := 0
collect:
	for got < sent.Len() {
		select {
		case a := <-fch:
			got++
			s, err := provider.Parse(a.summary, nil)
			if err != nil {
				continue
			}
			as := s.(*atomicsync.Summary)
			c := cands[as.ID()]
			if c == nil {
				c = &cand{summary: as}
				cands[as.ID()] = c
			}
			c.weight += p.weights[a.node]
			c.n++
		case <-timeout:
			break collect
		}
	}
	p.mu.Lock()
	delete(p.frontier, reqID)
	p.mu.Unlock()
	if len(cands) == 0 {
		return nil, fmt.Errorf("no state summary from %d validators", sent.Len())
	}
	var best *cand
	for _, c := range cands {
		log.Printf("frontier: height %d root %s: %d validators, %.1f%% stake", c.summary.BlockNumber, c.summary.BlockRoot, c.n, 100*float64(c.weight)/float64(p.total))
		if best == nil || c.summary.BlockNumber > best.summary.BlockNumber || (c.summary.BlockNumber == best.summary.BlockNumber && c.weight > best.weight) {
			best = c
		}
	}
	// Acceptance at that height from everyone connected.
	list = p.connectedList()
	p.mu.Lock()
	p.nextReq++
	reqID = p.nextReq
	ach := make(chan acceptedAnswer, len(list)+16)
	p.accepted[reqID] = ach
	p.mu.Unlock()
	msg, err = p.creator.GetAcceptedStateSummary(cChainID, reqID, requestTimeout, []uint64{best.summary.BlockNumber})
	if err != nil {
		return nil, err
	}
	sent = p.send(msg, set.Of(list...))
	var agree, answered uint64
	var nAgree, nAnswered int
	timeout = time.After(requestTimeout)
	got = 0
collect2:
	for got < sent.Len() {
		select {
		case a := <-ach:
			got++
			answered += p.weights[a.node]
			nAnswered++
			for _, id := range a.ids {
				if id == best.summary.ID() {
					agree += p.weights[a.node]
					nAgree++
				}
			}
		case <-timeout:
			break collect2
		}
	}
	p.mu.Lock()
	delete(p.accepted, reqID)
	p.mu.Unlock()
	log.Printf("accepted: height %d: %d/%d answering validators agree, %.1f%% of answering stake, answering stake %.1f%% of the network",
		best.summary.BlockNumber, nAgree, nAnswered, 100*float64(agree)/float64(max(answered, 1)), 100*float64(answered)/float64(p.total))
	if answered*5 < p.total || agree*3 < answered*2 {
		return nil, fmt.Errorf("summary at %d not accepted: %d/%d agree, answering stake %d of %d", best.summary.BlockNumber, agree, answered, answered, p.total)
	}
	return best.summary.BlockSyncSummary, nil
}
