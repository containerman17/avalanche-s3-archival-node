package rpc

import (
	"encoding/json"

	"github.com/ava-labs/libevm/common"
)

// THE edb_ NAMESPACE: the posting-list log reads of rpc/tokens.go over
// JSON-RPC. Every method takes ONE object parameter (the fields are named, and
// most are optional), and the paged ones answer {logs, more, nextCursor}
// with the same keyset rule as ots_: cursor is the TxNum to continue from,
// 0 means the end the walk starts at, newest-first unless ascending.
//
//	edb_getLogsByEmitter          {emitter, topic0?, cursor?, limit?, ascending?}
//	edb_getLogsByTopicValue       {value, topic0?, positions?, cursor?, limit?, ascending?}
//	edb_getTopicGroups            {value, topic0?}
//	edb_getTokenTransfersByHolder {address, standard, cursor?, limit?, ascending?}
//	edb_getTokenTransfersByContract {token, standard, cursor?, limit?, ascending?}
//	edb_getTokenContracts         {address}
//
// standard is erc20 | erc721 | erc1155; positions is a bitmask of the topic
// positions 1..3 (1, 2, 4), 0 or absent for any. The token methods are
// shortcuts: fixed signatures and positions over the two generic reads.
func (s *Server) edbDispatch(method string, params []json.RawMessage) (any, *rpcError, bool) {
	var run func(edbParams) (any, error)
	switch method {
	case "edb_getLogsByEmitter":
		run = func(p edbParams) (any, error) {
			return s.LogsByEmitter(p.Emitter, p.Topic0, p.Cursor, p.Limit, !p.Ascending)
		}
	case "edb_getLogsByTopicValue":
		run = func(p edbParams) (any, error) {
			return s.LogsByTopicValue(p.Value, p.Topic0, p.Positions, p.Cursor, p.Limit, !p.Ascending)
		}
	case "edb_getTopicGroups":
		run = func(p edbParams) (any, error) { return s.TopicGroups(p.Value, p.Topic0) }
	case "edb_getTokenTransfersByHolder":
		run = func(p edbParams) (any, error) {
			return s.TokenTransfersByHolder(p.Address, p.Standard, p.Cursor, p.Limit, !p.Ascending)
		}
	case "edb_getTokenTransfersByContract":
		run = func(p edbParams) (any, error) {
			return s.TokenTransfersByContract(p.Token, p.Standard, p.Cursor, p.Limit, !p.Ascending)
		}
	case "edb_getTokenContracts":
		run = func(p edbParams) (any, error) { return s.TokenContracts(p.Address) }
	default:
		return nil, nil, false
	}
	var p edbParams
	if len(params) != 1 {
		return nil, errInvalid("%s takes one object parameter", method), true
	}
	if err := json.Unmarshal(params[0], &p); err != nil {
		return nil, errInvalid("bad parameter: %v", err), true
	}
	res, err := run(p)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}, true
	}
	return res, nil, true
}

type edbParams struct {
	Emitter   common.Address `json:"emitter"`
	Token     common.Address `json:"token"`
	Address   common.Address `json:"address"`
	Value     common.Hash    `json:"value"`
	Topic0    *common.Hash   `json:"topic0"`
	Positions byte           `json:"positions"`
	Standard  string         `json:"standard"`
	Cursor    uint64         `json:"cursor"`
	Limit     int            `json:"limit"`
	Ascending bool           `json:"ascending"`
}

// MarshalJSON: the JSON shape of a page and of the group rows (hex numbers
// are not used here; TxNums are plain integers, as ots_ and the plain HTTP
// peer report them).
func (p *PagedLogs) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{"logs": p.Logs, "more": p.More, "nextCursor": p.NextCursor})
}

func (g TopicGroup) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{"topic0": g.Topic0, "emitter": g.Emitter, "firstTxNum": g.First, "lastTxNum": g.Last})
}

func (c TokenContract) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{"standard": c.Standard, "token": c.Token, "firstTxNum": c.First, "lastTxNum": c.Last})
}
