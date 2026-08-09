package rpc

import (
	"context"
	"strings"
	"testing"
)

// The three things that make the /sql door safe rather than merely working:
// the row-scan budget, the deadline, and keyset-only pagination with an
// index-served ORDER BY. Each of them is a wrong answer or an unbounded read
// when it breaks, so each gets a test. The corpus is the block-hash env's:
// blocks 1..8 sealed, 9..12 in the tail.

func sqlRows(t *testing.T, s *Server, q string, budget int64) [][]any {
	t.Helper()
	_, rows, err := s.RunSQL(context.Background(), q, budget)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return rows
}

func sqlErr(t *testing.T, s *Server, q string, budget int64) string {
	t.Helper()
	_, _, err := s.RunSQL(context.Background(), q, budget)
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
	if _, _, err := srv.RunSQL(ctx, "SELECT number FROM blocks WHERE number = 3", SQLRowBudget); err == nil {
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
}
