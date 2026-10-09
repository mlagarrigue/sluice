package postgres

import (
	"strconv"
	"testing"
)

// The cost of a cache miss against a *properly bounded* cache, with no
// protocol in the way.
//
// internal/bench measures the miss through the wire, where an over-generous
// bound lets the map grow for the whole run and the growth lands in the
// figure. That is a real failure mode and it is measured there under a name
// that says so. This is the other one: a bound small enough that the cache is
// permanently full, every query distinct, so each one is a lookup that fails,
// an eviction, and an insert — steady state, no growth.
//
// What is missing from it, and cannot be added here: the eviction's Close and
// its CloseComplete. Both travel with the query that caused the eviction, so
// they add bytes but no round trip; the number here is the client-side floor,
// not the cost.
func BenchmarkStmtCacheChurn(b *testing.B) {
	const bound = 64
	c := newStmtCache(bound)
	// Fill it, so every iteration below is the steady state rather than the
	// first sixty-four inserts into an empty map.
	for i := range bound {
		c.add(stmtKey{sql: "SELECT " + strconv.Itoa(i)})
	}

	var n int
	b.ReportAllocs()
	for b.Loop() {
		n++
		key := stmtKey{sql: "SELECT generated " + strconv.Itoa(n)}
		if _, ok := c.lookup(key); ok {
			b.Fatal("a generated statement was found in the cache")
		}
		evictKey, _, ok := c.evictee()
		if !ok {
			b.Fatal("the cache is full but named no evictee")
		}
		c.remove(evictKey)
		c.add(key)
	}
	b.StopTimer()
	if len(c.byKey) != bound {
		b.Fatalf("the cache holds %d entries, want its bound of %d", len(c.byKey), bound)
	}
}

// And the hit, which is what the cache costs on the path it exists for.
func BenchmarkStmtCacheHit(b *testing.B) {
	c := newStmtCache(64)
	key := stmtKey{sql: longBenchSQL}
	c.add(key)
	b.ReportAllocs()
	for b.Loop() {
		if _, ok := c.lookup(key); !ok {
			b.Fatal("miss")
		}
	}
}

const longBenchSQL = `SELECT o.order_id, o.line_index, o.sale_qty, o.price
FROM order_lines o
JOIN unnest($1::bigint[], $2::int[]) AS k(order_id, line_index)
  ON o.order_id = k.order_id AND o.line_index = k.line_index
WHERE o.sale_qty > 0 AND o.price IS NOT NULL
ORDER BY o.order_id, o.line_index`
