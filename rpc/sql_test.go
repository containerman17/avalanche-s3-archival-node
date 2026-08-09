package rpc

import (
	"context"
	"strings"
	"sync"
	"testing"
)

// The three things that make the /sql door safe rather than merely working:
// the row-scan budget, the deadline, and keyset-only pagination with an
// index-served ORDER BY. Each of them is a wrong answer or an unbounded read
// when it breaks, so each gets a test. The corpus is the block-hash env's:
// blocks 1..8 sealed, 9..12 in the tail.

func sqlRows(t *testing.T, s *Server, q string, budget int64) [][]any {
	t.Helper()
	_, rows, err := s.RunSQL(context.Background(), q, nil, budget)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return rows
}

func sqlErr(t *testing.T, s *Server, q string, budget int64) string {
	t.Helper()
	_, _, err := s.RunSQL(context.Background(), q, nil, budget)
	if err == nil {
		t.Fatalf("%s: expected an error, got none", q)
	}
	return err.Error()
}

func TestSQLRowBudget(t *testing.T) {
	srv := newBlockHashEnv(t).srv

	// An unnarrowed scan dies at the budget, by name, rather than reading the
	// whole table.
	if msg := sqlErr(t, srv, "SELECT number FROM blocks", 5); !strings.Contains(msg, "budget") {
		t.Fatalf("expected a budget refusal, got %q", msg)
	}
	// The same query narrowed to fewer rows than the budget is served, which
	// is what makes the budget a narrowing incentive and not a wall.
	if rows := sqlRows(t, srv, "SELECT number FROM blocks WHERE number BETWEEN 3 AND 6", 5); len(rows) != 4 {
		t.Fatalf("narrowed scan returned %d rows, want 4", len(rows))
	}
	// A LIMIT inside the budget stops the scan early: the budget counts rows
	// this backend produced, so a query the engine cuts short never spends it.
	if rows := sqlRows(t, srv, "SELECT number FROM blocks LIMIT 4", 5); len(rows) != 4 {
		t.Fatalf("LIMIT 4 returned %d rows, want 4", len(rows))
	}
}

func TestSQLDeadline(t *testing.T) {
	srv := newBlockHashEnv(t).srv
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := srv.RunSQL(ctx, "SELECT number FROM blocks WHERE number = 3", nil, SQLRowBudget); err == nil {
		t.Fatal("a cancelled query returned rows")
	}
}

func TestSQLKeysetPaginationOnly(t *testing.T) {
	srv := newBlockHashEnv(t).srv

	// OFFSET is refused by name: it is O(n) and drains the page cache.
	if msg := sqlErr(t, srv, "SELECT number FROM blocks LIMIT 2 OFFSET 3", SQLRowBudget); !strings.Contains(msg, "OFFSET") {
		t.Fatalf("expected an OFFSET refusal, got %q", msg)
	}
	// The keyset form is served, descending, off the index order.
	rows := sqlRows(t, srv, "SELECT number FROM blocks WHERE number < 10 ORDER BY number DESC LIMIT 3", 5)
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(rows))
	}
	for i, want := range []uint64{9, 8, 7} {
		if got := rows[i][0].(uint64); got != want {
			t.Fatalf("row %d is block %d, want %d", i, got, want)
		}
	}
	// And the page after it, keyed on the last row, is the next three.
	rows = sqlRows(t, srv, "SELECT number FROM blocks WHERE number < 7 ORDER BY number DESC LIMIT 3", 5)
	for i, want := range []uint64{6, 5, 4} {
		if got := rows[i][0].(uint64); got != want {
			t.Fatalf("page 2 row %d is block %d, want %d", i, got, want)
		}
	}
}

func TestSQLOrderByMustBeIndexOrder(t *testing.T) {
	srv := newBlockHashEnv(t).srv

	// A sort the index cannot serve would buffer, and buffering under a row
	// budget answers the top of what was scanned, not the top of what matches.
	// It is refused, and the message names what IS servable.
	msg := sqlErr(t, srv, "SELECT number FROM blocks ORDER BY gas_used DESC LIMIT 3", SQLRowBudget)
	if !strings.Contains(msg, "index order") {
		t.Fatalf("expected an index-order refusal, got %q", msg)
	}
	// Ascending index order is served, and stays ascending.
	rows := sqlRows(t, srv, "SELECT number FROM blocks WHERE number <= 3 ORDER BY number ASC", 5)
	for i, want := range []uint64{1, 2, 3} {
		if got := rows[i][0].(uint64); got != want {
			t.Fatalf("row %d is block %d, want %d", i, got, want)
		}
	}
	// An alias is the same index order. Every join writes one, and refusing
	// it would take keyset pagination away from exactly those queries.
	rows = sqlRows(t, srv, "SELECT b.number FROM blocks b WHERE b.number < 5 ORDER BY b.number DESC LIMIT 2", 5)
	if len(rows) != 2 || rows[0][0].(uint64) != 4 {
		t.Fatalf("aliased keyset page = %v, want blocks 4 and 3", rows)
	}
}

// TestSQLParamsAndPlanCache is the one thing a plan cache can get wrong: the
// cached plan must be everything the query TEXT decides and nothing the
// PARAMETERS decide. Two executions of one text with different values must
// read different rows, and the ORDER BY the first execution stripped off the
// plan must still be honoured by the second.
func TestSQLParamsAndPlanCache(t *testing.T) {
	srv := newBlockHashEnv(t).srv

	const q = "SELECT number FROM blocks WHERE number = ?"
	for _, want := range []uint64{3, 5, 3, 11} {
		rows := sqlRowsP(t, srv, q, []any{want}, 5)
		if len(rows) != 1 || rows[0][0].(uint64) != want {
			t.Fatalf("param %d returned %v", want, rows)
		}
	}
	// A stale cached NARROWING is invisible in the rows above only if the
	// engine's Filter saves it, so check the read was narrowed too: the same
	// text under a budget of one row can only pass if it read one block.
	if rows := sqlRowsP(t, srv, q, []any{7}, 1); len(rows) != 1 || rows[0][0].(uint64) != 7 {
		t.Fatalf("narrowed lookup under a 1-row budget returned %v", rows)
	}

	// The ORDER BY is deleted from the cached plan (the rows already arrive in
	// index order), so the direction has to be restored from the cache on every
	// later execution or the second page comes back ascending.
	const desc = "SELECT number FROM blocks WHERE number < ? ORDER BY number DESC LIMIT 3"
	for _, lt := range []uint64{10, 7} {
		rows := sqlRowsP(t, srv, desc, []any{lt}, 5)
		if len(rows) != 3 {
			t.Fatalf("keyset page below %d: %v", lt, rows)
		}
		for i := range rows {
			if got, want := rows[i][0].(uint64), lt-1-uint64(i); got != want {
				t.Fatalf("page below %d row %d is %d, want %d", lt, i, got, want)
			}
		}
	}

	// A parameter count that does not match the placeholders is an error, not
	// a plan with an unbound variable in it.
	if _, _, err := srv.RunSQL(context.Background(), q, nil, 5); err == nil {
		t.Fatal("a missing parameter was accepted")
	}
	if _, _, err := srv.RunSQL(context.Background(), q, []any{1, 2}, 5); err == nil {
		t.Fatal("a surplus parameter was accepted")
	}

	// The raw-SQL body still works beside the JSON one, and both reach RunSQL
	// with the same query.
	if q, p, err := sqlParseBody([]byte("SELECT 1")); err != nil || q != "SELECT 1" || p != nil {
		t.Fatalf("raw body: %q %v %v", q, p, err)
	}
	q2, p2, err := sqlParseBody([]byte(`{"query":"SELECT ?","params":[7]}`))
	if err != nil || q2 != "SELECT ?" || len(p2) != 1 {
		t.Fatalf("json body: %q %v %v", q2, p2, err)
	}
	if _, _, err := sqlParseBody([]byte(`{"nope":1}`)); err == nil {
		t.Fatal("a JSON body with no query was accepted")
	}
}

// TestSQLPlanCacheConcurrent: one cached plan, many goroutines, each with its
// own parameters. Under -race this is what catches a plan tree being mutated
// in place instead of bound into a private copy.
func TestSQLPlanCacheConcurrent(t *testing.T) {
	srv := newBlockHashEnv(t).srv
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			want := uint64(w%bhTailEnd) + 1
			for range 20 {
				_, rows, err := srv.RunSQL(context.Background(),
					"SELECT number FROM blocks WHERE number = ?", []any{want}, 5)
				if err != nil {
					t.Errorf("param %d: %v", want, err)
					return
				}
				if len(rows) != 1 || rows[0][0].(uint64) != want {
					t.Errorf("param %d returned %v", want, rows)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func sqlRowsP(t *testing.T, s *Server, q string, params []any, budget int64) [][]any {
	t.Helper()
	_, rows, err := s.RunSQL(context.Background(), q, params, budget)
	if err != nil {
		t.Fatalf("%s %v: %v", q, params, err)
	}
	return rows
}
