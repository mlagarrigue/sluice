package postgres

import (
	"errors"
	"fmt"
	"strconv"
	"time"
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
	// the next miss, so the server's statement count stays bounded. A batch
	// read to ReadyForQuery without an error must have confirmed every Close
	// in front of it, or the connection breaks, so a live connection holds at
	// most max+1 names here.
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

	// lastRows is how many rows the statement's last execution produced, when
	// that execution was read to its CommandComplete; -1 when it is unknown —
	// never run to its end, stopped early, failed. It is what lets a query
	// that fits in one batch ask for every row and pay one round trip: see
	// [FetchAuto].
	lastRows int

	// ahead is how many batches a batched read of the statement kept asked
	// for, set at its first PortalSuspended; zero until one suspended. A
	// portal that suspended once is one the backend fetches from, so the
	// next execution sends that many Executes with the query itself rather
	// than a round trip later: see [query.run].
	ahead int

	// batch is the shortest time a batched read of the statement took to
	// produce its first batch, from the BindComplete; zero until one did.
	// ahead is set from it: see [depth].
	batch time.Duration
}

// defaultStatementCacheBound is the cache a connection starts with — the
// bound pgx uses. See [Config.StatementCacheSize] for why there is one at all.
const defaultStatementCacheBound = 512

func newStmtCache(bound int) *stmtCache {
	return &stmtCache{
		max: bound,
		// Not sized to the bound: every connection starts with a cache now,
		// and most never prepare anywhere near 512 statements.
		byKey: make(map[stmtKey]int),
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

// lookup returns the entry a statement is already prepared under, if any.
func (c *stmtCache) lookup(key stmtKey) (int, bool) {
	i, ok := c.byKey[key]
	if !ok {
		c.misses++
		return -1, false
	}
	c.hits++
	c.moveToFront(i)
	return i, true
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

// add records a statement now prepared on the server under a fresh name, and
// returns its entry.
func (c *stmtCache) add(key stmtKey) int {
	name := "sluice_" + strconv.FormatUint(c.nextID, 10)
	c.nextID++

	var i int
	if n := len(c.free); n > 0 {
		i = c.free[n-1]
		c.free = c.free[:n-1]
		c.entries[i] = stmtEntry{key: key, name: name, prev: -1, next: -1, lastRows: -1}
	} else {
		c.entries = append(c.entries, stmtEntry{key: key, name: name, prev: -1, next: -1, lastRows: -1})
		i = len(c.entries) - 1
	}
	c.byKey[key] = i
	c.pushFront(i)
	return i
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

// StatementCacheStats is how a connection's statement cache has behaved:
// how many statements it holds prepared on the server, and how often a query
// found its statement there or had to prepare it. See
// [Config.StatementCacheSize].
//
// It exists so a benchmark can show the cache losing rather than assert that
// it wins — a hit rate near zero on a workload of distinct SQL is the honest
// half of the feature.
type StatementCacheStats struct {
	Prepared     int
	Hits, Misses uint64
}

// StatementCacheStats reports how the cache has behaved; all zero when it
// is off.
func (c *Conn) StatementCacheStats() StatementCacheStats {
	if c.stmts == nil {
		return StatementCacheStats{}
	}
	return StatementCacheStats{Prepared: len(c.stmts.byKey), Hits: c.stmts.hits, Misses: c.stmts.misses}
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
//
// entry is the statement's place in the cache, -1 when the cache is off; the
// query reads and records the statement's last row count through it. It stays
// valid for the length of the query: nothing else evicts while it runs.
func (c *Conn) prepareFor(sql string, oids []uint32) (name string, needsParse bool, entry int) {
	if c.stmts == nil {
		return "", true, -1 // unnamed, parsed per query
	}
	key := stmtKey{sql: sql, oids: packOIDs(oids)}
	if i, ok := c.stmts.lookup(key); ok {
		return c.stmts.entries[i].name, false, i
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
	i := c.stmts.add(key)
	return c.stmts.entries[i].name, true, i
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
// confirmed are forgotten, the rest stay queued for the next miss. The server
// answers in order, so a batch that succeeded has seen a CloseComplete for
// every Close sent in front of it; one that did not comes from a peer that
// swallows Closes, and keeping their names would grow the list by one per miss
// for the life of the connection. That breaks the connection, and the error is
// returned to the query.
func (c *Conn) settleParse(queryErr error) error {
	var err error
	if queryErr == nil && c.broken == nil && c.closesDone < c.closesSent {
		err = c.breakConn(fmt.Errorf("%w: the server answered the batch and confirmed %d of the %d Close messages sent before it",
			ErrProtocol, c.closesDone, c.closesSent))
	}
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
	return err
}

// forgetStatement drops a cached statement after the server rejected it.
//
// held says whether the server still holds a statement under the name. After
// a stale plan it does: the server discarded the plan, not the statement,
// which lasts until the session ends or a Close releases it (PostgreSQL 18
// §54.2.3). Forgetting it here alone would leave one more statement on the
// backend per schema change under a live connection, outside the bound
// StatementCacheSize promises on the server as well. So the name is queued in
// stmtCache.closing and travels in front of the next miss's Parse, as an
// eviction does: no round trip of its own. After 26000 the server holds
// nothing under the name — a pooler's DISCARD ALL, a swapped backend — and a
// Close, harmless on an unknown name, would only be noise.
func (c *Conn) forgetStatement(sql string, oids []uint32, held bool) {
	if c.stmts == nil {
		return
	}
	key := stmtKey{sql: sql, oids: packOIDs(oids)}
	if i, ok := c.stmts.byKey[key]; ok && held {
		c.stmts.closing = append(c.stmts.closing, c.stmts.entries[i].name)
	}
	c.stmts.remove(key)
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
