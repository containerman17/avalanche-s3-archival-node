// Package plainhttp is the plain HTTP adapter: THE METHOD NAME IS THE PATH and
// parameters arrive in ANY form (GET query string, POST form body, POST JSON
// body), because the point of it is being debuggable from a browser and from
// curl (DESIGN "Entry points and adapters", entry point 4).
//
// It is a PEER over the core query layer, NOT a proxy to JSON-RPC: every
// handler calls the same Go methods the library and the gRPC adapter call, and
// no JSON-RPC envelope, method table or parameter encoding is involved.
// Unbranded, and the method set is the gRPC one under the same names.
//
// Bytes go out as 0x-hex, everything else as plain JSON. GET / lists the
// methods and their parameters, which is the published-schema pattern DESIGN
// retained: discovery, not a versioned contract.
package plainhttp

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"strings"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"

	"github.com/containerman17/epochdb"
	"github.com/containerman17/epochdb/rpc"
	"github.com/containerman17/epochdb/store"
)

// method is one entry point: what it takes, and what it does.
type method struct {
	params string
	run    func(*epochdb.Node, params) (any, error)
}

var methods = map[string]method{
	"head":                        {"", head},
	"getBlock":                    {"number|hash, full", getBlock},
	"getTransaction":              {"hash", getTransaction},
	"call":                        {"to, data, from, value, gas, gasPrice, height", call},
	"getState":                    {"address, slot, height, withCode", getState},
	"searchTransactionsByAddress": {"address, cursor, limit, ascending", search},
	"getLogs":                     {"fromBlock, toBlock, address, topic0..topic3", getLogs},
}

// Handler serves the method set under its own prefix. The caller strips the
// prefix (serve mounts it at /v0/).
func Handler(n *epochdb.Node) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.Trim(r.URL.Path, "/")
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ") // a browser reads the answer, so make it readable
		if name == "" {
			enc.Encode(index())
			return
		}
		m, ok := methods[name]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			enc.Encode(map[string]any{"error": "no method " + name, "methods": index()})
			return
		}
		p, err := parse(r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			enc.Encode(map[string]any{"error": err.Error()})
			return
		}
		out, err := m.run(n, p)
		if err != nil {
			// One status for every refused read: the parameters were understood
			// (a 400 above says they were not), the answer could not be given.
			w.WriteHeader(http.StatusBadRequest)
			enc.Encode(map[string]any{"error": err.Error()})
			return
		}
		enc.Encode(out)
	})
}

func index() map[string]string {
	out := make(map[string]string, len(methods))
	for name, m := range methods {
		out[name] = m.params
	}
	return out
}

// --- parameters, in any form --------------------------------------------------

// params is one request's arguments, whichever way they arrived. A JSON body
// keeps its native types; a query string or a form body arrives as strings,
// and every accessor below reads both.
type params map[string]any

// parse merges the query string with the body. A JSON body wins on a key it
// sets, because a caller who sent a JSON document meant it.
func parse(r *http.Request) (params, error) {
	p := params{}
	if err := r.ParseForm(); err != nil {
		return nil, err
	}
	for k, v := range r.Form { // ParseForm holds the query string and the form body
		if len(v) == 1 {
			p[k] = v[0]
		} else {
			p[k] = v
		}
	}
	if r.Body == nil {
		return p, nil
	}
	// A JSON body is not form-encoded, so ParseForm left it untouched (and read
	// it only when the content type said form). Decode it when there is one.
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		return p, nil
	}
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("bad JSON body: %v", err)
	}
	for k, v := range body {
		p[k] = v
	}
	return p, nil
}

func (p params) has(key string) bool { _, ok := p[key]; return ok }

func (p params) str(key string) string {
	switch v := p[key].(type) {
	case nil:
		return ""
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	case []string:
		if len(v) > 0 {
			return v[0]
		}
		return ""
	default:
		return fmt.Sprint(v)
	}
}

// list reads a repeatable parameter: repeated query keys, a comma-separated
// value, or a JSON array, all the same thing.
func (p params) list(key string) []string {
	switch v := p[key].(type) {
	case nil:
		return nil
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, e := range v {
			out = append(out, fmt.Sprint(e))
		}
		return out
	default:
		s := p.str(key)
		if s == "" {
			return nil
		}
		return strings.Split(s, ",")
	}
}

// num reads a height or a count, in decimal or 0x-hex. Missing is 0.
func (p params) num(key string) (uint64, error) {
	s := strings.TrimSpace(p.str(key))
	if s == "" {
		return 0, nil
	}
	if strings.HasPrefix(s, "0x") {
		return hexutil.DecodeUint64(s)
	}
	return strconv.ParseUint(s, 10, 64)
}

func (p params) flag(key string) bool {
	switch s := strings.ToLower(p.str(key)); s {
	case "", "0", "false", "no":
		return false
	default:
		return true
	}
}

func (p params) bytes(key string) ([]byte, error) {
	s := strings.TrimSpace(p.str(key))
	if s == "" {
		return nil, nil
	}
	if !strings.HasPrefix(s, "0x") {
		s = "0x" + s
	}
	return hexutil.Decode(s)
}

func (p params) addr(key string) (common.Address, error) {
	b, err := p.bytes(key)
	if err != nil {
		return common.Address{}, fmt.Errorf("%s: %v", key, err)
	}
	if len(b) != common.AddressLength {
		return common.Address{}, fmt.Errorf("%s: need a 20-byte address, got %d bytes", key, len(b))
	}
	return common.BytesToAddress(b), nil
}

func (p params) hash(key string) (common.Hash, error) {
	b, err := p.bytes(key)
	if err != nil {
		return common.Hash{}, fmt.Errorf("%s: %v", key, err)
	}
	if len(b) != common.HashLength {
		return common.Hash{}, fmt.Errorf("%s: need a 32-byte hash, got %d bytes", key, len(b))
	}
	return common.BytesToHash(b), nil
}

func (p params) bigint(key string) (*big.Int, error) {
	s := strings.TrimSpace(p.str(key))
	if s == "" {
		return new(big.Int), nil
	}
	if strings.HasPrefix(s, "0x") {
		return hexutil.DecodeBig(s)
	}
	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return nil, fmt.Errorf("%s: %q is not a number", key, s)
	}
	return v, nil
}

// height resolves the optional `height` parameter: absent or 0 is the head.
func height(n *epochdb.Node, p params) (uint64, error) {
	h, err := p.num("height")
	if err != nil {
		return 0, fmt.Errorf("height: %v", err)
	}
	if h > 0 {
		return h, nil
	}
	head, err := n.Head()
	return head.Number, err
}

// --- the methods ---------------------------------------------------------------

func head(n *epochdb.Node, _ params) (any, error) {
	h, err := n.Head()
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"number": h.Number, "hash": h.Hash, "timestamp": h.Timestamp,
		"accepted": h.Accepted, "settled": h.Settled,
	}, nil
}

func getBlock(n *epochdb.Node, p params) (any, error) {
	num, err := p.num("number")
	if err != nil {
		return nil, fmt.Errorf("number: %v", err)
	}
	if p.has("hash") {
		h, err := p.hash("hash")
		if err != nil {
			return nil, err
		}
		got, ok, err := n.Core().HeightByHash(h)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("block %s is not on this chain", h)
		}
		num = got
	} else if !p.has("number") {
		if num, err = height(n, p); err != nil {
			return nil, err
		}
	}
	hdr, err := n.Core().HeaderAt(num)
	if err != nil {
		return nil, err
	}
	_, count, _, err := n.Store().BlockTxRange(num)
	if err != nil {
		return nil, err
	}
	out := map[string]any{
		"number": num, "hash": hdr.Hash(), "txCount": count, "header": hdr,
	}
	if p.flag("full") {
		blk, err := n.Core().BlockAt(num)
		if err != nil {
			return nil, err
		}
		out["transactions"] = blk.Transactions()
	}
	return out, nil
}

func getTransaction(n *epochdb.Node, p params) (any, error) {
	hash, err := p.hash("hash")
	if err != nil {
		return nil, err
	}
	blk, idx, found, err := n.Core().FindTx(hash)
	if err != nil {
		return nil, err
	}
	if !found {
		return map[string]any{"found": false}, nil
	}
	out := map[string]any{
		"found": true, "blockNumber": blk.NumberU64(), "blockHash": blk.Hash(),
		"index": idx, "transaction": blk.Transactions()[idx],
	}
	receipts, err := n.Core().BlockReceipts(blk)
	if err != nil {
		return nil, err
	}
	if idx < len(receipts) {
		out["receipt"] = receipts[idx]
	}
	frames, _, err := n.Core().Frames(hash)
	if err != nil {
		return nil, err
	}
	out["frames"] = framesJSON(frames)
	return out, nil
}

// framesJSON renders the stored call frames. Nothing in store.Frame has JSON
// tags, and it should not: the corpus type is not a wire type.
func framesJSON(frames []store.Frame) []map[string]any {
	out := make([]map[string]any, 0, len(frames))
	for _, f := range frames {
		e := map[string]any{
			"kind": f.Kind, "depth": f.Depth, "from": f.From, "to": f.To,
			"gas": f.Gas, "gasUsed": f.GasUsed, "failed": f.Failed,
			"input": hexutil.Bytes(f.Input), "output": hexutil.Bytes(f.Output),
		}
		if f.Value != nil {
			e["value"] = f.Value.String()
		}
		out = append(out, e)
	}
	return out
}

func call(n *epochdb.Node, p params) (any, error) {
	h, err := height(n, p)
	if err != nil {
		return nil, err
	}
	data, err := p.bytes("data")
	if err != nil {
		return nil, fmt.Errorf("data: %v", err)
	}
	value, err := p.bigint("value")
	if err != nil {
		return nil, err
	}
	gasPrice, err := p.bigint("gasPrice")
	if err != nil {
		return nil, err
	}
	gas, err := p.num("gas")
	if err != nil {
		return nil, fmt.Errorf("gas: %v", err)
	}
	if gas == 0 || gas > rpc.GasCap {
		gas = rpc.GasCap
	}
	msg := &rpc.CallMsg{
		Value: value, GasLimit: gas, GasPrice: gasPrice,
		GasFeeCap: new(big.Int), GasTipCap: new(big.Int), Data: data,
	}
	if p.has("from") {
		from, err := p.addr("from")
		if err != nil {
			return nil, err
		}
		msg.From = from
	}
	if p.has("to") {
		to, err := p.addr("to")
		if err != nil {
			return nil, err
		}
		msg.To = &to
	}
	out, err := n.CallAt(msg, h)
	if err != nil {
		return nil, err
	}
	return map[string]any{"height": h, "output": hexutil.Bytes(out)}, nil
}

func getState(n *epochdb.Node, p params) (any, error) {
	h, err := height(n, p)
	if err != nil {
		return nil, err
	}
	addr, err := p.addr("address")
	if err != nil {
		return nil, err
	}
	st, err := n.StateAt(h)
	if err != nil {
		return nil, err
	}
	out := map[string]any{
		"height": h, "address": addr,
		"nonce":    st.GetNonce(addr),
		"balance":  st.GetBalance(addr).ToBig().String(),
		"codeHash": st.GetCodeHash(addr),
	}
	if p.has("slot") {
		slot, err := p.hash("slot")
		if err != nil {
			return nil, err
		}
		out["slot"] = slot
		out["value"] = st.GetState(addr, slot)
	}
	if p.flag("withCode") {
		out["code"] = hexutil.Bytes(st.GetCode(addr))
	}
	if err := st.Error(); err != nil {
		return nil, err
	}
	return out, nil
}

// searchDefaultPage and searchMaxPage bound one page. Keyset paging means the
// caller walks with nextCursor rather than asking for more at once.
const (
	searchDefaultPage = 25
	searchMaxPage     = 1000
)

func search(n *epochdb.Node, p params) (any, error) {
	addr, err := p.addr("address")
	if err != nil {
		return nil, err
	}
	cursor, err := p.num("cursor")
	if err != nil {
		return nil, fmt.Errorf("cursor: %v", err)
	}
	limit, err := p.num("limit")
	if err != nil {
		return nil, fmt.Errorf("limit: %v", err)
	}
	if limit == 0 {
		limit = searchDefaultPage
	}
	if limit > searchMaxPage {
		limit = searchMaxPage
	}
	asc := p.flag("ascending")
	hits, more, err := n.Core().SearchByAddress(addr, cursor, int(limit), !asc)
	if err != nil {
		return nil, err
	}
	rows := make([]map[string]any, 0, len(hits))
	for _, h := range hits {
		rows = append(rows, map[string]any{
			"txNum": h.TxNum, "height": h.Height, "hash": h.Hash,
			"roles": roleNames(h.Roles),
		})
	}
	out := map[string]any{"hits": rows, "more": more}
	if more && len(hits) > 0 {
		// The next page continues from one row past the last one returned, in
		// the direction of the walk.
		last := hits[len(hits)-1].TxNum
		if asc {
			out["nextCursor"] = last + 1
		} else if last > 0 {
			out["nextCursor"] = last - 1
		}
	}
	return out, nil
}

// roleNames spells the posting row's role bits, because "9" tells a human
// nothing and this surface exists for humans.
func roleNames(bits byte) []string {
	out := []string{}
	for _, r := range []struct {
		bit  byte
		name string
	}{
		{store.RoleSender, "sender"},
		{store.RoleRecipient, "recipient"},
		{store.RoleCreated, "created"},
		{store.RoleEmitter, "emitter"},
		{store.RoleFrame, "frame"},
	} {
		if bits&r.bit != 0 {
			out = append(out, r.name)
		}
	}
	return out
}

func getLogs(n *epochdb.Node, p params) (any, error) {
	from, err := p.num("fromBlock")
	if err != nil {
		return nil, fmt.Errorf("fromBlock: %v", err)
	}
	to, err := p.num("toBlock")
	if err != nil {
		return nil, fmt.Errorf("toBlock: %v", err)
	}
	if to == 0 {
		if to, err = height(n, params{}); err != nil {
			return nil, err
		}
	}
	var addrs []common.Address
	for _, s := range p.list("address") {
		a, err := params{"a": s}.addr("a")
		if err != nil {
			return nil, fmt.Errorf("address: %v", err)
		}
		addrs = append(addrs, a)
	}
	// topic0..topic3, each repeatable: one position's values are an OR set,
	// which is the same shape eth_getLogs takes without the nested arrays a
	// query string cannot spell.
	var topics [][]common.Hash
	for i := 0; i < 4; i++ {
		var set []common.Hash
		for _, s := range p.list(fmt.Sprintf("topic%d", i)) {
			h, err := params{"t": s}.hash("t")
			if err != nil {
				return nil, fmt.Errorf("topic%d: %v", i, err)
			}
			set = append(set, h)
		}
		if set == nil && len(topics) == 0 {
			continue // leading empty positions carry no meaning
		}
		topics = append(topics, set)
	}
	logs, err := n.Core().GetLogs(from, to, addrs, topics)
	if err != nil {
		return nil, err
	}
	if logs == nil {
		logs = []*types.Log{}
	}
	return map[string]any{"fromBlock": from, "toBlock": to, "logs": logs}, nil
}
