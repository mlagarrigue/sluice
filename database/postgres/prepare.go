package postgres

import (
	"errors"
	"fmt"
	"strconv"
)

// ErrStalePlan reports a prepared statement the server refused because the
// schema it was planned against has changed — SQLSTATE 0A000, "cached plan
// must not change result type".
//
// It is surfaced rather than retried silently. A retry would usually work and
// would occasionally hide the thing worth knowing: something migrated the
// schema under a live connection, and the next surprise from it may not be
// recoverable. The cache drops the entry either way, so the same query
// re-prepares and succeeds on the caller's own retry.
var ErrStalePlan = errors.New("postgres: a cached plan no longer matches the schema it was prepared against")

// stmtKey identifies a prepared statement. The parameter OIDs are part of it
// because they are part of what was planned: the same text with different
// declared types is a different statement, and reusing one for the other is
// how a query silently starts comparing an int4 against a bigint column.
type stmtKey struct {
	sql  string
	oids string // the OIDs, packed — a slice cannot be a map key
}

// stmtCache is a bounded LRU of statements prepared on the server.
//
// It is a map plus an intrusive list rather than a container/list, because the
// hot path is "hit, move to front" and that wants no allocation. The list is
// two slices of indices into entries, which keeps the whole thing in three
// allocations for the connection's lifetime rather than one per statement.
type stmtCache struct {
	max     int
	byKey   map[stmtKey]int // key → index into entries
	entries []stmtEntry
	head    int // most recently used, -1 when empty
	tail    int // least recently used, -1 when empty
	free    []int
	nextID  uint64

	// closing holds evicted statements whose Close the server has not
	// confirmed yet. An eviction is pipelined with the query that caused it
	// rather than paid as a round trip of its own, so the entry leaves the
	// map at once and its name waits here until CloseComplete arrives. A
	// Close the server never confirmed — the batch failed before it, or the
	// connection was abandoned mid-read — stays here and goes out again with
	// the next miss, so the server's statement count stays bounded.
	closing []string

	// Hits and misses, for the benchmark that has to be able to say the cache
	// lost. They are counters, not instrumentation: no allocation, no lock,
	// read from the same goroutine that writes them.
	hits, misses uint64
}

type stmtEntry struct {
	key        stmtKey
	name       string
	prev, next int
}

func newStmtCache(bound int) *stmtCache {
	return &stmtCache{
		max:   bound,
		byKey: make(map[stmtKey]int, bound),
		head:  -1,
		tail:  -1,
	}
}

// packOIDs makes a map-comparable key out of the declared parameter types.
func packOIDs(oids []uint32) string {
	if len(oids) == 0 {
		return ""
	}
	b := make([]byte, 0, len(oids)*4)
	for _, o := range oids {
		//nolint:gosec // G115: packing a uint32 into four bytes, which is what the truncation is for
		b = append(b, byte(o>>24), byte(o>>16), byte(o>>8), byte(o))
	}
	return string(b)
}

// lookup returns the name a statement is already prepared under, if any.
func (c *stmtCache) lookup(key stmtKey) (string, bool) {
	i, ok := c.byKey[key]
	if !ok {
		c.misses++
		return "", false
	}
	c.hits++
	c.moveToFront(i)
	return c.entries[i].name, true
}

// evictee names the statement that must be closed on the server to make room,
// or "" when there is room already. It does not remove it: the caller decides
// whether the Close is synchronous or pipelined.
func (c *stmtCache) evictee() (stmtKey, string, bool) {
	if len(c.byKey) < c.max || c.tail < 0 {
		return stmtKey{}, "", false
	}
	e := c.entries[c.tail]
	return e.key, e.name, true
}

// add records a statement now prepared on the server under a fresh name.
func (c *stmtCache) add(key stmtKey) string {
	name := "sluice_" + strconv.FormatUint(c.nextID, 10)
	c.nextID++

	var i int
	if n := len(c.free); n > 0 {
		i = c.free[n-1]
		c.free = c.free[:n-1]
		c.entries[i] = stmtEntry{key: key, name: name, prev: -1, next: -1}
	} else {
		c.entries = append(c.entries, stmtEntry{key: key, name: name, prev: -1, next: -1})
		i = len(c.entries) - 1
	}
	c.byKey[key] = i
	c.pushFront(i)
	return name
}

// remove forgets a statement. Called after the server has been told to close
// it, and also when the server rejects it as stale.
func (c *stmtCache) remove(key stmtKey) {
	i, ok := c.byKey[key]
	if !ok {
		return
	}
	c.unlink(i)
	delete(c.byKey, key)
	c.entries[i] = stmtEntry{prev: -1, next: -1}
	c.free = append(c.free, i)
}

func (c *stmtCache) pushFront(i int) {
	c.entries[i].prev = -1
	c.entries[i].next = c.head
	if c.head >= 0 {
		c.entries[c.head].prev = i
	}
	c.head = i
	if c.tail < 0 {
		c.tail = i
	}
}

func (c *stmtCache) unlink(i int) {
	e := c.entries[i]
	if e.prev >= 0 {
		c.entries[e.prev].next = e.next
	} else if c.head == i {
		c.head = e.next
	}
	if e.next >= 0 {
		c.entries[e.next].prev = e.prev
	} else if c.tail == i {
		c.tail = e.prev
	}
}

func (c *stmtCache) moveToFront(i int) {
	if c.head == i {
		return
	}
	c.unlink(i)
	c.pushFront(i)
}

// PrepareStatements turns the statement cache on for this connection. bound
// is the largest number of statements kept prepared on the server at once;
// beyond it, the least recently used one is closed in the same write that
// prepares the new one, so an eviction costs no round trip of its own.
//
//	conn.PrepareStatements(64)
//
// # Why this is a number and not a boolean
//
// Every driver has a statement cache because parsing the same SQL for every
// batch measures the parser rather than the transport. But a prepared
// statement is server-side state, and an unbounded cache is a backend growing
// a plan per distinct query text until something notices — which, for
// generated SQL, is never. So the bound is required and there is no default
// that suits every workload, in the same spirit as [QueryConfig].
//
// Zero or less disables the cache: every query uses the unnamed statement, the
// server re-parses it, and nothing is held between queries. That is the right
// setting for a connection running mostly-distinct SQL, and it is what this
// package did before the cache existed.
//
// # Where it loses, which is not a footnote
//
// Two places, both real:
//
//   - **Distinct SQL.** A workload that never repeats a statement pays the
//     naming, the cache lookup and the server-side memory, and reuses nothing.
//     Measured in the benchmarks module: see BenchmarkPGPrepareMiss.
//   - **The generic plan.** After five executions PostgreSQL may replan a
//     prepared statement without looking at the parameters, and keep that plan
//     for the connection's life. On a column with skewed values that is how a
//     query that was fast for four calls becomes slow forever, and no amount
//     of client-side caching will show it. It is not a reason to avoid the
//     cache; it is a reason to know that "prepared" is a planning decision and
//     not only a parsing one.
//
// It is a method rather than a [StartupConfig] field because it applies to a
// [Conn] however it was built — including one from [NewConn], which never saw
// a startup exchange. Calling it twice with a smaller bound evicts down to it;
// calling it with zero or less closes everything and turns the cache off.
//
// The statements are closed on the server as they are evicted, so what this
// bounds is real memory in a backend rather than a map in this process.
func (c *Conn) PrepareStatements(bound int) error {
	if c.broken != nil {
		return c.broken
	}
	// Eviction and shutdown below talk to the server, so this is a
	// wire-touching entry point like Exec: refused while a result is being
	// consumed. The eviction inside prepareFor is not routed through here —
	// it runs while query.run legitimately holds the guard.
	if err := c.beginBusy(); err != nil {
		return err
	}
	defer c.endBusy()
	if bound <= 0 {
		err := c.closeAllStatements()
		c.stmts = nil
		return err
	}
	if c.stmts == nil {
		c.stmts = newStmtCache(bound)
		return nil
	}
	c.stmts.max = bound
	for len(c.stmts.byKey) > bound {
		key, name, ok := c.stmts.evictee()
		if !ok {
			break
		}
		if err := c.closeStatement(name); err != nil {
			return err
		}
		c.stmts.remove(key)
	}
	return nil
}

// StatementCacheStats reports how the cache has behaved: how many statements
// are prepared, and how often a query found one.
//
// It exists so a benchmark can show the cache losing rather than assert that
// it wins — a hit rate near zero on a workload of distinct SQL is the honest
// half of the feature.
func (c *Conn) StatementCacheStats() (prepared int, hits, misses uint64) {
	if c.stmts == nil {
		return 0, 0, 0
	}
	return len(c.stmts.byKey), c.stmts.hits, c.stmts.misses
}

// closeStatement tells the server to release a prepared statement.
func (c *Conn) closeStatement(name string) error {
	c.w.Close('S', name)
	c.w.Sync()
	if err := c.w.Flush(); err != nil {
		return c.breakConn(fmt.Errorf("closing prepared statement %s: %w", name, err))
	}
	return c.drainToReady()
}

// closeAllStatements releases every statement this connection prepared. Best
// effort past the first failure: the connection is being reconfigured or torn
// down, and stopping halfway would leave more behind than carrying on.
func (c *Conn) closeAllStatements() error {
	if c.stmts == nil {
		return nil
	}
	var first error
	// Evicted names whose pipelined Close was never confirmed are closed here
	// too: the server may still hold them, and this is the last chance.
	for _, name := range c.stmts.closing {
		if c.broken == nil {
			if err := c.closeStatement(name); err != nil && first == nil {
				first = err
			}
		}
	}
	c.stmts.closing = c.stmts.closing[:0]
	for key, i := range c.stmts.byKey {
		// Once the connection is broken nothing more goes to the wire: the
		// remaining statements die with the backend session anyway.
		if c.broken == nil {
			if err := c.closeStatement(c.stmts.entries[i].name); err != nil && first == nil {
				first = err
			}
		}
		delete(c.stmts.byKey, key)
	}
	c.stmts.entries = c.stmts.entries[:0]
	c.stmts.free = c.stmts.free[:0]
	c.stmts.head, c.stmts.tail = -1, -1
	return first
}

// prepareFor returns the statement name to bind against, preparing one if the
// cache is on and this query is not in it.
//
// An empty name means the unnamed statement: either the cache is off, or this
// query is being parsed inline as before. The caller sends the Parse in that
// case; when a name comes back with needsParse set, the caller sends a Parse
// under that name instead, and it stays prepared afterwards.
//
// An entry recorded here is **provisional**: the server has not seen the
// Parse yet. The caller's read loop confirms it with [Conn.parseCompleted]
// when ParseComplete arrives, and [Conn.settleParse] — called once the query
// is over — drops an entry that was never confirmed. The three belong
// together: a caller that consults the cache without settling reintroduces
// the poisoned entry this protocol exists to prevent.
func (c *Conn) prepareFor(sql string, oids []uint32) (name string, needsParse bool, err error) {
	if c.stmts == nil {
		return "", true, nil // unnamed, parsed per query, as before
	}
	key := stmtKey{sql: sql, oids: packOIDs(oids)}
	if name, ok := c.stmts.lookup(key); ok {
		return name, false, nil
	}
	// Make room before adding. The Close is queued in front of the Parse and
	// leaves in the same write, so a miss on a full cache costs no extra round
	// trip: closing synchronously first cost a whole round trip per miss, as
	// much again as the query itself on a point read. The server answers
	// CloseComplete before anything the query draws; until it does, the name
	// stays in stmtCache.closing, so an eviction is not lost if it never does.
	if evictKey, evictName, ok := c.stmts.evictee(); ok {
		c.stmts.remove(evictKey)
		c.stmts.closing = append(c.stmts.closing, evictName)
	}
	for _, name := range c.stmts.closing {
		c.w.Close('S', name)
	}
	c.closesSent, c.closesDone = len(c.stmts.closing), 0
	c.pendingParse, c.pendingKey = true, key
	return c.stmts.add(key), true, nil
}

// closeCompleted counts a CloseComplete for one of the Closes prepareFor
// queued. The server processes them in order, so the count names a prefix of
// stmtCache.closing.
func (c *Conn) closeCompleted() {
	if c.closesDone < c.closesSent {
		c.closesDone++
	}
}

// parseCompleted confirms the provisional cache entry: the server acknowledged
// the Parse, so the statement exists under its name.
func (c *Conn) parseCompleted() { c.pendingParse = false }

// settleParse resolves the provisional entry once its query is over — after
// the resync has read to ReadyForQuery, so the server has had its last word.
//
// An entry whose Parse never completed is dropped: the server rejected it — a
// syntax error, or any query inside an aborted transaction — and holds
// nothing under its name. Keeping it would bind every later execution of this
// SQL against a statement that does not exist, failing with SQLSTATE 26000
// forever: a poisoned cache, from one bad moment.
//
// The pipelined Closes are settled at the same point: those the server
// confirmed are forgotten, the rest stay queued for the next miss.
func (c *Conn) settleParse(queryErr error) {
	if c.pendingParse && queryErr != nil && c.stmts != nil {
		c.stmts.remove(c.pendingKey)
	}
	if c.closesDone > 0 && c.stmts != nil {
		n := copy(c.stmts.closing, c.stmts.closing[c.closesDone:])
		clear(c.stmts.closing[n:])
		c.stmts.closing = c.stmts.closing[:n]
	}
	c.pendingParse, c.pendingKey = false, stmtKey{}
	c.closesSent, c.closesDone = 0, 0
}

// forgetStatement drops a cached statement after the server rejected it. The
// server has already discarded the plan, so there is nothing to close.
func (c *Conn) forgetStatement(sql string, oids []uint32) {
	if c.stmts == nil {
		return
	}
	c.stmts.remove(stmtKey{sql: sql, oids: packOIDs(oids)})
}

// isStalePlan reports the server error that means a cached plan no longer
// matches the schema. PostgreSQL raises it as feature_not_supported.
//
// The code alone is not enough: 0A000 is every "not supported" the server can
// raise, and treating them all as stale plans dropped healthy cache entries
// and mislabelled ordinary errors as schema changes. The source routine,
// RevalidateCachedQuery, is what identifies this one and is not localised;
// the English message is the fallback for an intermediary that strips the
// routine field.
func isStalePlan(err error) bool {
	pgErr, ok := errors.AsType[*Error](err)
	if !ok || pgErr.Code != "0A000" {
		return false
	}
	return pgErr.Routine == "RevalidateCachedQuery" ||
		pgErr.Message == "cached plan must not change result type"
}

// isMissingStatement reports SQLSTATE 26000, invalid_sql_statement_name: the
// server no longer holds a statement this cache believes is prepared — a
// pooler ran DISCARD ALL, a backend was swapped, something else reset the
// session. Without dropping the entry, every later execution of that SQL
// binds the dead name and fails with the same code forever; dropped, the
// caller's own retry re-prepares and succeeds.
func isMissingStatement(err error) bool {
	pgErr, ok := errors.AsType[*Error](err)
	return ok && pgErr.Code == "26000"
}
