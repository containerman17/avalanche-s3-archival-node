package rpc

// THE SQL DOOR, in its most primitive form: an embedded go-mysql-server over
// the indexes that already exist. There is no new on-disk structure here and
// there will not be one until real queries say which one is missing; that is
// the whole point of shipping the door first (DESIGN, 2026-08-09).
//
// Three read-only tables, each one an existing read path wearing a schema:
//
//	blocks  the headers section, by height
//	txs     the fp48 tx index for `hash = ...`, container decode for a range
//	logs    the epoch log posting lists plus the stored-logs sections
//
// WHAT THE ENGINE IS AND IS NOT DOING. The engine parses, plans and filters;
// it never chooses how we read. After analysis the door walks the plan once
// and takes two things off it: the conjuncts that name a column we can narrow
// on (a block range, an address, a topic, a hash), and the ORDER BY. The
// narrowing is a HINT ONLY: the engine keeps its Filter node, so a hint that
// is a superset (which every posting list is) still returns exactly the right
// rows. That is deliberately the safe side of go-mysql-server's PreciseMatch
// trap, where a backend that claims to have applied a filter exactly and has
// not returns wrong rows in silence.
//
// THE ORDER BY IS REWRITTEN OR REFUSED, never sorted in memory. Every table
// emits in its index order, ascending or descending. A sort the index cannot
// serve would have to buffer, and buffering under a row budget is a WRONG
// ANSWER, not a slow one: the top 5 of the first 100 rows scanned is not the
// top 5. So a servable ORDER BY has its Sort node deleted (the rows already
// arrive in that order) and anything else is an error naming what is servable.
//
// EVERY QUERY HAS A ROW-SCAN BUDGET AND A DEADLINE (user ruling 2026-08-09).
// The budget counts rows this backend actually produced, across every table in
// the query, and the query fails by name when it runs out. Pagination is
// keyset only: OFFSET is refused, because it is O(n) and drains exactly the
// page cache the memory design defends.

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	sqle "github.com/dolthub/go-mysql-server"
	gsql "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/ava-labs/libevm/common"
	ethtypes "github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
)

const (
	// SQLDatabase is the one database name the door answers to.
	SQLDatabase = "epochdb"
	// SQLRowBudget is how many rows a query may scan when it does not say
	// otherwise (?budget=N raises it, up to SQLMaxRowBudget).
	SQLRowBudget = 100
	// SQLMaxRowBudget is the ceiling on ?budget=. A whole-history scan is
	// minutes and competes for the page cache; it is refused, not queued.
	SQLMaxRowBudget = 100_000
	// SQLTimeout is the per-query deadline.
	SQLTimeout = 30 * time.Second
)

// --- schema -----------------------------------------------------------------

var (
	sqlBytes32 = types.MustCreateBinary(sqltypes.VarBinary, 32)
	sqlBytes20 = types.MustCreateBinary(sqltypes.VarBinary, 20)
)

// sqlSchemas is every table the door serves. u256 values are VARBINARY(32)
// big-endian, because byte order IS numeric order there and DECIMAL tops out
// at 65 digits; hashes and addresses are VARBINARY too, never BINARY(n),
// whose NUL padding breaks equality.
var sqlSchemas = map[string]gsql.Schema{
	"blocks": {
		{Name: "number", Type: types.Uint64, Source: "blocks"},
		{Name: "hash", Type: sqlBytes32, Source: "blocks"},
		{Name: "parent_hash", Type: sqlBytes32, Source: "blocks"},
		{Name: "timestamp", Type: types.Uint64, Source: "blocks"},
		{Name: "gas_used", Type: types.Uint64, Source: "blocks"},
		{Name: "gas_limit", Type: types.Uint64, Source: "blocks"},
		{Name: "base_fee", Type: sqlBytes32, Source: "blocks", Nullable: true},
	},
	"txs": {
		{Name: "block_number", Type: types.Uint64, Source: "txs"},
		{Name: "tx_index", Type: types.Uint64, Source: "txs"},
		{Name: "hash", Type: sqlBytes32, Source: "txs"},
		{Name: "from_addr", Type: sqlBytes20, Source: "txs"},
		{Name: "to_addr", Type: sqlBytes20, Source: "txs", Nullable: true},
		{Name: "value", Type: sqlBytes32, Source: "txs"},
		{Name: "nonce", Type: types.Uint64, Source: "txs"},
		{Name: "gas", Type: types.Uint64, Source: "txs"},
		{Name: "gas_price", Type: sqlBytes32, Source: "txs", Nullable: true},
		{Name: "input", Type: types.Blob, Source: "txs"},
	},
	"logs": {
		{Name: "block_number", Type: types.Uint64, Source: "logs"},
		{Name: "log_index", Type: types.Uint64, Source: "logs"},
		{Name: "tx_index", Type: types.Uint64, Source: "logs"},
		{Name: "tx_hash", Type: sqlBytes32, Source: "logs"},
		{Name: "address", Type: sqlBytes20, Source: "logs"},
		{Name: "topic0", Type: sqlBytes32, Source: "logs", Nullable: true},
		{Name: "topic1", Type: sqlBytes32, Source: "logs", Nullable: true},
		{Name: "topic2", Type: sqlBytes32, Source: "logs", Nullable: true},
		{Name: "topic3", Type: sqlBytes32, Source: "logs", Nullable: true},
		{Name: "data", Type: types.Blob, Source: "logs"},
	},
}

// sqlOrder is each table's index order. It is the ONLY ORDER BY the door can
// serve, ascending or descending, on a prefix of these columns.
var sqlOrder = map[string][]string{
	"blocks": {"number"},
	"txs":    {"block_number", "tx_index"},
	"logs":   {"block_number", "log_index"},
}

// --- per-query state --------------------------------------------------------

// sqlScan is what one table was narrowed to. Everything in it is a HINT: the
// engine's own Filter node still runs over the rows we produce.
type sqlScan struct {
	from, to  uint64 // block range, always clamped to [1, head]
	desc      bool
	blockHash *common.Hash
	txHash    *common.Hash
	addrs     []common.Address
	topics    [][]common.Hash // by position, nil = wildcard
	narrowed  bool            // an address or topic was given
}

// sqlQuery is the whole query's state: the head it reads at (captured once,
// so the epoch set cannot move under it), the per-table narrowing, and the
// shared row budget.
type sqlQuery struct {
	head  uint64
	scans map[string]*sqlScan
	// alias maps a query alias back to the table it names. Every column
	// reference in an analyzed plan carries the ALIAS, so without this a
	// `FROM logs l` silently narrows nothing and scans from block 1.
	alias map[string]string
	left  int64
}

type sqlQueryKey struct{}

// scan is the per-table narrowing, keyed by real table name. Two aliases of
// one table share it, which is conservative in the only direction that is
// safe: the engine's Filter node still decides what a row means.
func (q *sqlQuery) scan(name string) *sqlScan {
	if t, ok := q.alias[name]; ok {
		name = t
	}
	sc, ok := q.scans[name]
	if !ok {
		sc = &sqlScan{from: 1, to: q.head}
		q.scans[name] = sc
	}
	return sc
}

func (q *sqlQuery) table(name string) string {
	if t, ok := q.alias[name]; ok {
		return t
	}
	return name
}

func sqlQueryOf(ctx context.Context) *sqlQuery {
	q, _ := ctx.Value(sqlQueryKey{}).(*sqlQuery)
	return q
}

// --- HTTP -------------------------------------------------------------------

// SQLHandler serves POST /sql (the query, as a plain-text body) and
// GET /sql/schema (the published schema description). The schema is a served
// file, not a versioned contract: a client that starts getting errors re-reads
// it (user ruling 2026-08-09).
func (s *Server) SQLHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/schema") {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			io.WriteString(w, s.sqlSchemaDoc())
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "POST the query as the request body; GET /sql/schema describes the tables", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
			return
		}
		budget := int64(SQLRowBudget)
		if v := r.URL.Query().Get("budget"); v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n <= 0 || n > SQLMaxRowBudget {
				http.Error(w, fmt.Sprintf("budget must be 1..%d", SQLMaxRowBudget), http.StatusBadRequest)
				return
			}
			budget = n
		}
		ctx, cancel := context.WithTimeout(r.Context(), SQLTimeout)
		defer cancel()
		cols, rows, err := s.RunSQL(ctx, string(body), budget)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"columns": cols, "rows": rows})
	})
}

// RunSQL runs one query under the given row-scan budget and returns the
// column names and the rows, byte values rendered as 0x-hex.
func (s *Server) RunSQL(ctx context.Context, query string, budget int64) ([]string, [][]any, error) {
	if s.blocks == nil {
		return nil, nil, fmt.Errorf("the /sql door needs the container source (tx APIs enabled)")
	}
	q := &sqlQuery{head: s.hist.Head(), scans: map[string]*sqlScan{}, alias: map[string]string{}, left: budget}
	sctx := gsql.NewContext(context.WithValue(ctx, sqlQueryKey{}, q),
		gsql.WithSession(gsql.NewBaseSession()))
	sctx.SetCurrentDatabase(SQLDatabase)

	eng := s.sqlEngine()
	node, err := eng.AnalyzeQuery(sctx, query)
	if err != nil {
		return nil, nil, err
	}
	node, err = sqlRewrite(node, q)
	if err != nil {
		return nil, nil, err
	}
	schema, iter, _, err := eng.PrepQueryPlanForExecution(sctx, query, node, nil)
	if err != nil {
		return nil, nil, err
	}
	defer iter.Close(sctx)

	cols := make([]string, len(schema))
	for i, c := range schema {
		cols[i] = c.Name
	}
	out := [][]any{}
	for {
		row, err := iter.Next(sctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		vals := make([]any, len(row))
		for i, v := range row {
			vals[i] = sqlJSONValue(v)
		}
		out = append(out, vals)
	}
	return cols, out, nil
}

// sqlJSONValue renders one cell. Bytes go out as 0x-hex, which is what every
// consumer of this chain already reads and writes.
func sqlJSONValue(v any) any {
	switch t := v.(type) {
	case []byte:
		return "0x" + hex.EncodeToString(t)
	default:
		return v
	}
}

func (s *Server) sqlEngine() *sqle.Engine {
	s.sqlOnce.Do(func() {
		s.sqlEng = sqle.New(analyzer.NewDefaultWithVersion(sqlProvider{sqlDB{s}}),
			&sqle.Config{IsReadOnly: true})
	})
	return s.sqlEng
}

// --- the plan rewrite -------------------------------------------------------

// sqlRewrite is the one pass over the analyzed plan: it refuses OFFSET, takes
// the narrowing conjuncts off every Filter, and either deletes a servable
// ORDER BY or refuses it.
func sqlRewrite(node gsql.Node, q *sqlQuery) (gsql.Node, error) {
	transform.Inspect(node, func(n gsql.Node) bool {
		if ta, ok := n.(*plan.TableAlias); ok {
			if rt, ok := ta.Child.(*plan.ResolvedTable); ok {
				q.alias[strings.ToLower(ta.Name())] = strings.ToLower(rt.Name())
			}
		}
		return true
	})
	var bad error
	out, _, err := transform.Node(node, func(n gsql.Node) (gsql.Node, transform.TreeIdentity, error) {
		switch t := n.(type) {
		case *plan.Offset:
			return nil, transform.SameTree, fmt.Errorf(
				"OFFSET is not served: paginate on the last row's key instead (WHERE block_number < :last ORDER BY block_number DESC LIMIT n)")
		case *plan.Filter:
			for _, c := range expression.SplitConjunction(t.Expression) {
				sqlNarrow(c, q)
			}
			return n, transform.SameTree, nil
		case *plan.Sort:
			if err := sqlOrderBy(t.SortFields, q); err != nil {
				bad = err
				return n, transform.SameTree, nil
			}
			return t.Child, transform.NewTree, nil
		case *plan.TopN:
			if err := sqlOrderBy(t.Fields, q); err != nil {
				bad = err
				return n, transform.SameTree, nil
			}
			return plan.NewLimit(t.Limit, t.Child), transform.NewTree, nil
		}
		return n, transform.SameTree, nil
	})
	if err != nil {
		return nil, err
	}
	if bad != nil {
		return nil, bad
	}
	return out, nil
}

// sqlOrderBy accepts an ORDER BY only when it is a prefix of one table's index
// order in a single direction, and records the direction on that table's scan.
func sqlOrderBy(fields gsql.SortFields, q *sqlQuery) error {
	if len(fields) == 0 {
		return nil
	}
	var table string
	desc := fields[0].Order == gsql.Descending
	names := make([]string, 0, len(fields))
	for _, f := range fields {
		gf, ok := f.Column.(*expression.GetField)
		if !ok {
			return fmt.Errorf("ORDER BY %s is not served: only index order is", f.Column)
		}
		if (f.Order == gsql.Descending) != desc {
			return fmt.Errorf("ORDER BY mixes ASC and DESC, which no index order serves")
		}
		if table == "" {
			table = q.table(strings.ToLower(gf.Table()))
		} else if q.table(strings.ToLower(gf.Table())) != table {
			return fmt.Errorf("ORDER BY spans two tables, which no index order serves")
		}
		names = append(names, strings.ToLower(gf.Name()))
	}
	want, ok := sqlOrder[table]
	if !ok || len(names) > len(want) {
		return fmt.Errorf("ORDER BY is not served by %s; its index order is (%s)", table, strings.Join(sqlOrder[table], ", "))
	}
	for i, n := range names {
		if n != want[i] {
			return fmt.Errorf("ORDER BY is not served by %s; its index order is (%s)", table, strings.Join(want, ", "))
		}
	}
	q.scan(table).desc = desc
	return nil
}

// sqlNarrow reads one conjunct and, if it names a column an existing index can
// narrow on, records it. Anything it does not understand is simply left to the
// engine's Filter node, which is why an unrecognised predicate is silently
// fine here and a misread one would not be.
func sqlNarrow(c gsql.Expression, q *sqlQuery) {
	switch e := c.(type) {
	case *expression.Equals:
		if gf, v, ok := sqlSides(e.Left(), e.Right()); ok {
			sqlEq(gf, []any{v}, q)
		}
	case *expression.InTuple:
		gf, ok := e.Left().(*expression.GetField)
		if !ok {
			return
		}
		tup, ok := e.Right().(expression.Tuple)
		if !ok {
			return
		}
		vals := make([]any, 0, len(tup))
		for _, el := range tup {
			lit, ok := el.(*expression.Literal)
			if !ok {
				return
			}
			vals = append(vals, lit.Value())
		}
		sqlEq(gf, vals, q)
	case *expression.GreaterThan:
		sqlBound(e.Left(), e.Right(), q, ">")
	case *expression.GreaterThanOrEqual:
		sqlBound(e.Left(), e.Right(), q, ">=")
	case *expression.LessThan:
		sqlBound(e.Left(), e.Right(), q, "<")
	case *expression.LessThanOrEqual:
		sqlBound(e.Left(), e.Right(), q, "<=")
	case *expression.Between:
		if gf, ok := e.Val.(*expression.GetField); ok && sqlIsHeight(q, gf) {
			if lo, ok := sqlUint(sqlLit(e.Lower)); ok {
				sqlRaiseFrom(q, gf, lo)
			}
			if hi, ok := sqlUint(sqlLit(e.Upper)); ok {
				sqlLowerTo(q, gf, hi)
			}
		}
	}
}

// sqlSides normalises `col = value` and `value = col` to (column, value).
func sqlSides(l, r gsql.Expression) (*expression.GetField, any, bool) {
	if gf, ok := l.(*expression.GetField); ok {
		if lit, ok := r.(*expression.Literal); ok {
			return gf, lit.Value(), true
		}
	}
	if gf, ok := r.(*expression.GetField); ok {
		if lit, ok := l.(*expression.Literal); ok {
			return gf, lit.Value(), true
		}
	}
	return nil, nil, false
}

func sqlLit(e gsql.Expression) any {
	if lit, ok := e.(*expression.Literal); ok {
		return lit.Value()
	}
	return nil
}

// sqlIsHeight reports whether this column is the table's block height, which
// is the one column every table's range narrowing runs on.
func sqlIsHeight(q *sqlQuery, gf *expression.GetField) bool {
	n := strings.ToLower(gf.Name())
	return n == "block_number" || (n == "number" && q.table(strings.ToLower(gf.Table())) == "blocks")
}

// sqlBound applies one inequality on a height column, turning it into the
// inclusive range every read path here takes.
func sqlBound(l, r gsql.Expression, q *sqlQuery, op string) {
	gf, v, ok := sqlSides(l, r)
	if !ok || !sqlIsHeight(q, gf) {
		return
	}
	n, ok := sqlUint(v)
	if !ok {
		return
	}
	// `value > col` is `col < value`: the sides were flipped, so the bound is.
	if _, isField := l.(*expression.GetField); !isField {
		op = map[string]string{">": "<", ">=": "<=", "<": ">", "<=": ">="}[op]
	}
	switch op {
	case ">":
		if n < ^uint64(0) {
			sqlRaiseFrom(q, gf, n+1)
		} else {
			sqlLowerTo(q, gf, 0) // nothing is above the ceiling
		}
	case ">=":
		sqlRaiseFrom(q, gf, n)
	case "<":
		if n > 0 {
			sqlLowerTo(q, gf, n-1)
		} else {
			sqlLowerTo(q, gf, 0) // heights start at 1, so this is empty
		}
	case "<=":
		sqlLowerTo(q, gf, n)
	}
}

func sqlRaiseFrom(q *sqlQuery, gf *expression.GetField, n uint64) {
	sc := q.scan(strings.ToLower(gf.Table()))
	if n > sc.from {
		sc.from = n
	}
}

func sqlLowerTo(q *sqlQuery, gf *expression.GetField, n uint64) {
	sc := q.scan(strings.ToLower(gf.Table()))
	if n < sc.to {
		sc.to = n
	}
}

// sqlEq records an equality (or IN list) on a column an index can serve.
func sqlEq(gf *expression.GetField, vals []any, q *sqlQuery) {
	table, col := q.table(strings.ToLower(gf.Table())), strings.ToLower(gf.Name())
	sc := q.scan(table)
	switch {
	case sqlIsHeight(q, gf):
		if len(vals) != 1 {
			return
		}
		if n, ok := sqlUint(vals[0]); ok {
			sqlRaiseFrom(q, gf, n)
			sqlLowerTo(q, gf, n)
		}
	case col == "hash" && table == "blocks":
		if h, ok := sqlHash(vals[0], 32); ok && len(vals) == 1 {
			hh := common.BytesToHash(h)
			sc.blockHash = &hh
		}
	case col == "hash" && table == "txs":
		if h, ok := sqlHash(vals[0], 32); ok && len(vals) == 1 {
			hh := common.BytesToHash(h)
			sc.txHash = &hh
		}
	case col == "address" && table == "logs":
		for _, v := range vals {
			a, ok := sqlHash(v, 20)
			if !ok {
				return
			}
			sc.addrs = append(sc.addrs, common.BytesToAddress(a))
		}
		sc.narrowed = true
	case table == "logs" && strings.HasPrefix(col, "topic") && len(col) == 6:
		pos := int(col[5] - '0')
		if pos < 0 || pos > 3 {
			return
		}
		for len(sc.topics) <= pos {
			sc.topics = append(sc.topics, nil)
		}
		for _, v := range vals {
			h, ok := sqlHash(v, 32)
			if !ok {
				return
			}
			sc.topics[pos] = append(sc.topics[pos], common.BytesToHash(h))
		}
		sc.narrowed = true
	}
}

// sqlUint reads a numeric literal in any of the shapes the parser produces.
func sqlUint(v any) (uint64, bool) {
	switch n := v.(type) {
	case int:
		return uint64(max(n, 0)), n >= 0
	case int8:
		return uint64(n), n >= 0
	case int16:
		return uint64(n), n >= 0
	case int32:
		return uint64(n), n >= 0
	case int64:
		return uint64(n), n >= 0
	case uint:
		return uint64(n), true
	case uint8:
		return uint64(n), true
	case uint16:
		return uint64(n), true
	case uint32:
		return uint64(n), true
	case uint64:
		return n, true
	case string:
		u, err := strconv.ParseUint(n, 10, 64)
		return u, err == nil
	}
	return 0, false
}

// sqlHash reads a byte literal of exactly n bytes, accepting the raw bytes a
// MySQL hex literal (0x...) produces and the 0x-hex string a JSON client sends.
func sqlHash(v any, n int) ([]byte, bool) {
	var b []byte
	switch t := v.(type) {
	case []byte:
		b = t
	case string:
		s := strings.TrimPrefix(strings.TrimPrefix(t, "0x"), "0X")
		d, err := hex.DecodeString(s)
		if err != nil {
			return nil, false
		}
		b = d
	default:
		return nil, false
	}
	if len(b) != n {
		return nil, false
	}
	return b, true
}

// --- the schema document ----------------------------------------------------

// sqlSchemaDoc is the published description: the columns as the process
// actually has them, plus the rules a caller has to know. Generated from the
// schemas so it cannot drift from what the tables answer.
func (s *Server) sqlSchemaDoc() string {
	var b strings.Builder
	fmt.Fprintf(&b, `# epochdb /sql

POST the query to /sql as the request body. Answers are JSON:
{"columns":[...],"rows":[[...]]}. Byte columns come back as 0x-hex and are
accepted the same way, or as a MySQL hex literal (0x...).

Database: %s. Head block right now: %d.

RULES, all of them enforced:
- Read only. SELECT over the tables below, nothing else.
- Row-scan budget: %d rows per query, over every table in it. Raise it with
  ?budget=N up to %d. Over budget is an error, never a truncated answer.
- Deadline: %s per query.
- ORDER BY must be a prefix of the table's index order (below), in one
  direction. Anything else is refused rather than sorted in memory, because a
  memory sort under a row budget returns the top of the rows scanned, not the
  top of the rows that match.
- OFFSET is refused. Paginate on the last row's key:
  WHERE block_number < <last> ORDER BY block_number DESC LIMIT 50.
- A WHERE that names a block range, a hash, a log address or a log topic is
  used to narrow the read. Anything else still works and still filters, but it
  scans, so it will hit the budget.

TABLES

`, SQLDatabase, s.hist.Head(), SQLRowBudget, SQLMaxRowBudget, SQLTimeout)
	names := make([]string, 0, len(sqlSchemas))
	for n := range sqlSchemas {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Fprintf(&b, "%s (index order: %s)\n", n, strings.Join(sqlOrder[n], ", "))
		for _, c := range sqlSchemas[n] {
			null := ""
			if c.Nullable {
				null = " NULL"
			}
			fmt.Fprintf(&b, "  %-13s %s%s\n", c.Name, c.Type.String(), null)
		}
		b.WriteString("\n")
	}
	b.WriteString(`HOW EACH TABLE IS READ, so you can tell a cheap query from a scan

blocks  one header read per row. WHERE hash = 0x.. resolves through the tx
        index; a range on number reads that many headers.
txs     WHERE hash = 0x.. is the fp48 tx index, one lookup. Anything else
        decodes every container in the block range and recovers every sender,
        which is the expensive path.
logs    WHERE address = / topic0 = over a SEALED range is served by the epoch
        posting lists, which are supersets; the exact filter runs on the rows.
        The unsealed tail is scanned per block, so a range that reaches the
        tail is capped at 10000 blocks, and so is any range with no address or
        topic to narrow it. The posting lists name BLOCKS, not log positions,
        so every log of a matching block is read and spends budget.

BINARY COLUMNS: compare them whole (address = 0x..). HEX(col) is right, but
LEFT/SUBSTRING/LENGTH over a binary column go through a character set and
come back mangled, so slice bytes on your side, not in the query.

NOT HERE YET, and known: no wallet history by address (the address index does
not exist), no token balances or holdings, no internal calls or traces (frames
are not captured yet), no receipt status or per-tx gas used, no aggregates by
time. Ask for them and they get built as index families, not as scans.
`)
	return b.String()
}

// --- the database, the tables, the rows -------------------------------------

type sqlDB struct{ s *Server }

func (d sqlDB) Name() string { return SQLDatabase }

func (d sqlDB) GetTableNames(*gsql.Context) ([]string, error) {
	names := make([]string, 0, len(sqlSchemas))
	for n := range sqlSchemas {
		names = append(names, n)
	}
	sort.Strings(names)
	return names, nil
}

func (d sqlDB) GetTableInsensitive(_ *gsql.Context, name string) (gsql.Table, bool, error) {
	n := strings.ToLower(name)
	if _, ok := sqlSchemas[n]; !ok {
		return nil, false, nil
	}
	return sqlTable{s: d.s, name: n}, true, nil
}

type sqlProvider struct{ db sqlDB }

func (p sqlProvider) Database(_ *gsql.Context, name string) (gsql.Database, error) {
	if strings.EqualFold(name, SQLDatabase) {
		return p.db, nil
	}
	return nil, gsql.ErrDatabaseNotFound.New(name)
}

func (p sqlProvider) HasDatabase(_ *gsql.Context, name string) bool {
	return strings.EqualFold(name, SQLDatabase)
}

func (p sqlProvider) AllDatabases(*gsql.Context) []gsql.Database {
	return []gsql.Database{p.db}
}

type sqlTable struct {
	s    *Server
	name string
}

func (t sqlTable) Name() string                { return t.name }
func (t sqlTable) String() string              { return t.name }
func (t sqlTable) Schema() gsql.Schema         { return sqlSchemas[t.name] }
func (t sqlTable) Collation() gsql.CollationID { return gsql.Collation_binary }

type sqlPartition struct{}

func (sqlPartition) Key() []byte { return []byte("all") }

func (t sqlTable) Partitions(*gsql.Context) (gsql.PartitionIter, error) {
	return gsql.PartitionsToPartitionIter(sqlPartition{}), nil
}

func (t sqlTable) PartitionRows(ctx *gsql.Context, _ gsql.Partition) (gsql.RowIter, error) {
	q := sqlQueryOf(ctx)
	if q == nil {
		return nil, fmt.Errorf("the %s table is only readable through the /sql door", t.name)
	}
	sc := q.scan(t.name)
	if sc.to > q.head {
		sc.to = q.head
	}
	var (
		next func() (gsql.Row, error)
		err  error
	)
	switch t.name {
	case "blocks":
		next, err = t.s.sqlBlockRows(sc)
	case "txs":
		next, err = t.s.sqlTxRows(sc)
	case "logs":
		next, err = t.s.sqlLogRows(sc)
	}
	if err != nil {
		return nil, err
	}
	return &sqlRowIter{q: q, next: next}, nil
}

// sqlRowIter is where the budget and the deadline are actually enforced: one
// place, every table, counting the rows this backend produced.
type sqlRowIter struct {
	q    *sqlQuery
	next func() (gsql.Row, error)
}

func (r *sqlRowIter) Next(ctx *gsql.Context) (gsql.Row, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("query deadline: %w", err)
	}
	row, err := r.next()
	if err != nil {
		return nil, err
	}
	if atomic.AddInt64(&r.q.left, -1) < 0 {
		return nil, fmt.Errorf("row-scan budget exhausted: narrow the query (block range, hash, log address or topic) or raise ?budget=")
	}
	return row, nil
}

func (r *sqlRowIter) Close(*gsql.Context) error { return nil }

// heights walks a scan's block range in its index order.
func (sc *sqlScan) heights() func() (uint64, bool) {
	n, done := sc.from, sc.from > sc.to
	if sc.desc {
		n = sc.to
	}
	return func() (uint64, bool) {
		if done {
			return 0, false
		}
		h := n
		if sc.desc {
			done = h == sc.from
			n--
		} else {
			done = h == sc.to
			n++
		}
		return h, true
	}
}

// list walks an explicit ascending block list in the scan's index order.
func (sc *sqlScan) list(blocks []uint64) func() (uint64, bool) {
	i := 0
	return func() (uint64, bool) {
		if i >= len(blocks) {
			return 0, false
		}
		i++
		if sc.desc {
			return blocks[len(blocks)-i], true
		}
		return blocks[i-1], true
	}
}

func (s *Server) sqlBlockRows(sc *sqlScan) (func() (gsql.Row, error), error) {
	next := sc.heights()
	if sc.blockHash != nil {
		n, ok, err := s.HeightByHash(*sc.blockHash)
		if err != nil {
			return nil, err
		}
		if !ok || n < sc.from || n > sc.to {
			return func() (gsql.Row, error) { return nil, io.EOF }, nil
		}
		next = sc.list([]uint64{n})
	}
	return func() (gsql.Row, error) {
		for {
			n, ok := next()
			if !ok {
				return nil, io.EOF
			}
			raw, ok, err := s.hist.HeaderRLP(n)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			var h ethtypes.Header
			if err := rlp.DecodeBytes(raw, &h); err != nil {
				return nil, fmt.Errorf("decode header %d: %w", n, err)
			}
			base, rerr := s.headerBaseFee(&h)
			if rerr != nil {
				return nil, rerr
			}
			return gsql.Row{
				n, h.Hash().Bytes(), h.ParentHash.Bytes(), h.Time,
				h.GasUsed, h.GasLimit, u256be(base),
			}, nil
		}
	}, nil
}

func (s *Server) sqlTxRows(sc *sqlScan) (func() (gsql.Row, error), error) {
	next := sc.heights()
	if sc.txHash != nil {
		// THE fp48 TX INDEX, which is the whole reason this column is worth
		// having: an unknown hash is rejected by the per-epoch blooms without
		// loading an index, and a known one stops at the first epoch.
		blk, i, found, err := s.findTx(*sc.txHash)
		if err != nil {
			return nil, err
		}
		if !found || blk.NumberU64() < sc.from || blk.NumberU64() > sc.to {
			return func() (gsql.Row, error) { return nil, io.EOF }, nil
		}
		done := false
		return func() (gsql.Row, error) {
			if done {
				return nil, io.EOF
			}
			done = true
			return s.sqlTxRow(blk, i)
		}, nil
	}
	var (
		blk *ethtypes.Block
		i   int
	)
	return func() (gsql.Row, error) {
		for {
			if blk != nil && i < len(blk.Transactions()) {
				pos := i
				if sc.desc {
					pos = len(blk.Transactions()) - 1 - i
				}
				i++
				return s.sqlTxRow(blk, pos)
			}
			n, ok := next()
			if !ok {
				return nil, io.EOF
			}
			raw, ok, err := s.blocks.GetByHeight(n)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, fmt.Errorf("container %d is not readable on this node", n)
			}
			if blk, err = s.parse(raw); err != nil {
				return nil, err
			}
			i = 0
		}
	}, nil
}

func (s *Server) sqlTxRow(blk *ethtypes.Block, i int) (gsql.Row, error) {
	tx := blk.Transactions()[i]
	header := blk.Header()
	base, rerr := s.headerBaseFee(header)
	if rerr != nil {
		return nil, rerr
	}
	// A sender that cannot be recovered is an ERROR, never a null: null means
	// "not on this chain" everywhere else in this process.
	signer := ethtypes.MakeSigner(s.chainCfg, header.Number, header.Time)
	from, err := ethtypes.Sender(signer, tx)
	if err != nil {
		return nil, fmt.Errorf("recover sender of %s in block %d: %w", tx.Hash(), blk.NumberU64(), err)
	}
	var to any
	if t := tx.To(); t != nil {
		to = t.Bytes()
	}
	return gsql.Row{
		blk.NumberU64(), uint64(i), tx.Hash().Bytes(), from.Bytes(), to,
		u256be(tx.Value()), tx.Nonce(), tx.Gas(),
		u256be(effectiveGasPrice(tx, base)), tx.Data(),
	}, nil
}

func (s *Server) sqlLogRows(sc *sqlScan) (func() (gsql.Row, error), error) {
	if sc.from > sc.to {
		return func() (gsql.Row, error) { return nil, io.EOF }, nil
	}
	// The tail has no posting lists, so a range that reaches it is scanned per
	// block; the same 10k cap eth_getLogs carries applies, and so does a range
	// with nothing to narrow it.
	sealedEnd := uint64(0)
	if end, ok := s.hist.Epochs().SealedEnd(); ok {
		sealedEnd = end
	}
	if tailFrom := max(sc.from, sealedEnd+1); sc.to >= tailFrom && sc.to-tailFrom+1 > GetLogsMaxRange {
		return nil, fmt.Errorf("blocks %d..%d are not sealed and no posting list covers them: narrow block_number to at most %d unsealed blocks",
			tailFrom, sc.to, GetLogsMaxRange)
	}
	if !sc.narrowed && sc.to-sc.from+1 > GetLogsMaxRange {
		return nil, fmt.Errorf("block range %d exceeds %d: give an address or a topic0, or narrow block_number",
			sc.to-sc.from+1, GetLogsMaxRange)
	}
	candidates, rerr := s.logCandidates(sc.from, sc.to, sc.addrs, sc.topics)
	if rerr != nil {
		return nil, rerr
	}
	next := sc.list(candidates)
	var (
		logs []*ethtypes.Log
		i    int
	)
	return func() (gsql.Row, error) {
		for {
			if i < len(logs) {
				pos := i
				if sc.desc {
					pos = len(logs) - 1 - i
				}
				i++
				return sqlLogRow(logs[pos]), nil
			}
			n, ok := next()
			if !ok {
				return nil, io.EOF
			}
			var rerr *rpcError
			if logs, rerr = s.logsOfBlock(n); rerr != nil {
				return nil, rerr
			}
			i = 0
		}
	}, nil
}

func sqlLogRow(l *ethtypes.Log) gsql.Row {
	row := gsql.Row{l.BlockNumber, uint64(l.Index), uint64(l.TxIndex), l.TxHash.Bytes(), l.Address.Bytes()}
	for i := range 4 {
		if i < len(l.Topics) {
			row = append(row, l.Topics[i].Bytes())
		} else {
			row = append(row, nil)
		}
	}
	return append(row, l.Data)
}

// u256be is the VARBINARY(32) encoding: 32 bytes big-endian, so byte order is
// numeric order. A nil number is a NULL cell.
func u256be(v *big.Int) any {
	if v == nil {
		return nil
	}
	var b [32]byte
	v.FillBytes(b[:])
	return b[:]
}
