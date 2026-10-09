package join

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/diagnostics"
	"github.com/mlagarrigue/sluice/internal/streamtest"
)

type customer struct {
	id   int
	name string
}

type order struct {
	id         int
	customerID int
}

func customerID(c customer) int { return c.id }
func orderCustomer(o order) int { return o.customerID }

func joinLabel(o order, c customer) string {
	return c.name + ":" + strconv.Itoa(o.id)
}

func okLimit(n int) BuildLimit {
	return BuildLimit{MaxEntries: n, OnOverflow: sluice.Fail}
}

func TestJoinStreamTable(t *testing.T) {
	customers := []customer{{1, "ana"}, {2, "bo"}}

	tests := []struct {
		name   string
		orders []order
		want   []string
	}{
		{
			"every probe element matches",
			[]order{{10, 1}, {11, 2}},
			[]string{"ana:10", "bo:11"},
		},
		{
			"a probe element with no match produces nothing",
			[]order{{10, 1}, {12, 99}, {11, 2}},
			[]string{"ana:10", "bo:11"},
		},
		{
			"several probe elements share a build row",
			[]order{{10, 1}, {11, 1}},
			[]string{"ana:10", "ana:11"},
		},
		{"empty probe side", nil, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := sluice.Collect(StreamTable(
				sluice.Of(tc.orders, 2), sluice.Of(customers, 2),
				orderCustomer, customerID, joinLabel, okLimit(100)))
			if !slices.Equal(got, tc.want) {
				t.Errorf("join = %v, want %v", got, tc.want)
			}
		})
	}
}

// An empty build side makes every probe element unmatched, which for an inner
// join means no output at all — not the probe side passed through.
func TestJoinEmptyBuildSide(t *testing.T) {
	got := sluice.Collect(StreamTable(
		sluice.Of([]order{{10, 1}}, 2), sluice.Empty[customer](),
		orderCustomer, customerID, joinLabel, okLimit(100)))
	if got != nil {
		t.Errorf("join against an empty table = %v, want nothing", got)
	}
}

// A key held by several build rows produces one output per row, in build-side
// arrival order. Returning only the first would be a silently wrong result.
func TestJoinDuplicateBuildKeys(t *testing.T) {
	customers := []customer{{1, "ana"}, {1, "alt"}, {2, "bo"}}

	got := sluice.Collect(StreamTable(
		sluice.Of([]order{{10, 1}, {11, 2}}, 2), sluice.Of(customers, 2),
		orderCustomer, customerID, joinLabel, okLimit(100)))

	want := []string{"ana:10", "alt:10", "bo:11"}
	if !slices.Equal(got, want) {
		t.Errorf("join = %v, want %v: every build row for a key must produce a result", got, want)
	}
}

// A skewed key must not inflate the output batches: one probe batch whose
// every element matches a repeated build key produces len(batch)×matches
// results, and emitting them as one batch would hand downstream a buffer of
// that size to reuse forever — the O(build) claim quietly broken on the
// output side. The join emits through the shared emitter, so no batch may
// exceed sluice.DefaultBatchSize, and the results themselves must be complete and
// in probe × build-arrival order.
func TestJoinRebatchesSkewedOutput(t *testing.T) {
	const (
		matches = 40 // build rows behind the one key
		probeN  = 64 // one probe batch
	)
	customers := make([]customer, matches)
	for i := range customers {
		customers[i] = customer{id: 1, name: "c" + strconv.Itoa(i)}
	}
	orders := make([]order, probeN)
	for i := range orders {
		orders[i] = order{id: i, customerID: 1}
	}

	var got []string
	StreamTable(
		sluice.Of(orders, probeN), sluice.Of(customers, matches),
		orderCustomer, customerID, joinLabel, okLimit(matches),
	)(func(b sluice.Batch[string]) bool {
		if b.Len() > sluice.DefaultBatchSize {
			t.Errorf("an output batch carries %d elements, over DefaultBatchSize (%d): "+
				"the cross product was not re-batched", b.Len(), sluice.DefaultBatchSize)
		}
		got = append(got, b.Items...)
		return true
	})

	want := make([]string, 0, probeN*matches)
	for _, o := range orders {
		for _, c := range customers {
			want = append(want, joinLabel(o, c))
		}
	}
	if !slices.Equal(got, want) {
		t.Errorf("join produced %d results, want %d in probe × arrival order", len(got), len(want))
	}
}

// The bound counts values rather than distinct keys: one key with many rows
// behind it is exactly the shape a key-based bound would not see.
func TestJoinLimitCountsValuesNotKeys(t *testing.T) {
	// Three rows, one key. A bound of two must trip.
	customers := []customer{{1, "a"}, {1, "b"}, {1, "c"}}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected an overflow: three values under one key exceed a bound of two")
		}
		if !streamtest.IsErr(r, sluice.ErrOverflow) {
			t.Errorf("panicked with %v, want ErrOverflow", r)
		}
	}()

	sluice.Collect(StreamTable(
		sluice.Of([]order{{10, 1}}, 1), sluice.Of(customers, 1),
		orderCustomer, customerID, joinLabel,
		BuildLimit{MaxEntries: 2, OnOverflow: sluice.Fail}))
}

func TestJoinOverflowFail(t *testing.T) {
	customers := []customer{{1, "a"}, {2, "b"}, {3, "c"}}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected a panic under the Fail policy")
		}
		if !streamtest.IsErr(r, sluice.ErrOverflow) {
			t.Errorf("panicked with %v, want ErrOverflow", r)
		}
	}()

	sluice.Collect(StreamTable(
		sluice.Of([]order{{10, 1}}, 1), sluice.Of(customers, 1),
		orderCustomer, customerID, joinLabel,
		BuildLimit{MaxEntries: 2, OnOverflow: sluice.Fail}))
}

// Under sluice.DropNewest the join runs against a truncated table: rows are missing,
// never wrong. The diagnostic is what tells the caller the difference.
func TestJoinOverflowDropNewest(t *testing.T) {
	customers := []customer{{1, "a"}, {2, "b"}, {3, "c"}}
	orders := []order{{10, 1}, {11, 2}, {12, 3}}

	diags := diagnostics.NewCollector(10)
	got := sluice.Collect(StreamTable(
		sluice.Of(orders, 3), sluice.Of(customers, 1),
		orderCustomer, customerID, joinLabel,
		BuildLimit{MaxEntries: 2, OnOverflow: sluice.DropNewest, Report: reportTo(diags)}))

	// Customers 1 and 2 made it into the table; 3 did not.
	if want := []string{"a:10", "b:11"}; !slices.Equal(got, want) {
		t.Errorf("join = %v, want %v: the result must be missing rows, not wrong ones", got, want)
	}

	if diags.Len() != 1 {
		t.Fatalf("recorded %d diagnostics, want exactly 1", diags.Len())
	}
	d := diags.All()[0]
	if d.Severity != diagnostics.Critical {
		t.Errorf("severity = %v, want Critical: §9.1 asks for overflow to be loud", d.Severity)
	}
	if d.Code != "Sluice.Join.BuildTableOverflow" {
		t.Errorf("code = %q, want the overflow code", d.Code)
	}
	if d.Args["limit"] != 2 {
		t.Errorf("args = %v, want the limit that was exceeded", d.Args)
	}
}

// sluice.Overflow is reported once, not once per dropped row. A build side far larger
// than the limit would otherwise produce the pathological batch §4.5 exists to
// prevent.
func TestJoinOverflowReportedOnce(t *testing.T) {
	customers := make([]customer, 1000)
	for i := range customers {
		customers[i] = customer{id: i, name: "x"}
	}

	diags := diagnostics.NewCollector(100)
	sluice.Collect(StreamTable(
		sluice.Of([]order{{10, 0}}, 1), sluice.Of(customers, 16),
		orderCustomer, customerID, joinLabel,
		BuildLimit{MaxEntries: 4, OnOverflow: sluice.DropNewest, Report: reportTo(diags)}))

	if diags.Len() != 1 {
		t.Errorf("recorded %d diagnostics for one overflow, want 1", diags.Len())
	}
	if diags.Truncated() != 0 {
		t.Errorf("truncated %d, want 0: one report must not fill the collector", diags.Truncated())
	}
}

// A dropping policy with no collector is legal — sluice.Fail is not the only way to
// learn of an overflow — and must not panic on the nil.
func TestJoinOverflowWithoutCollector(t *testing.T) {
	customers := []customer{{1, "a"}, {2, "b"}, {3, "c"}}

	got := sluice.Collect(StreamTable(
		sluice.Of([]order{{10, 1}}, 1), sluice.Of(customers, 1),
		orderCustomer, customerID, joinLabel,
		BuildLimit{MaxEntries: 2, OnOverflow: sluice.DropNewest}))

	if want := []string{"a:10"}; !slices.Equal(got, want) {
		t.Errorf("join = %v, want %v", got, want)
	}
}

// The build side stops being read once the table is full under a dropping
// policy: walking the rest changes nothing and may be expensive.
func TestJoinStopsReadingBuildOnceFull(t *testing.T) {
	produced := 0
	build := sluice.Stream[customer](func(yield func(sluice.Batch[customer]) bool) {
		for i := range 1000 {
			produced++
			if !yield(sluice.Batch[customer]{Items: []customer{{id: i, name: "x"}}}) {
				return
			}
		}
	})

	sluice.Collect(StreamTable(
		sluice.Of([]order{{10, 0}}, 1), build,
		orderCustomer, customerID, joinLabel,
		BuildLimit{MaxEntries: 4, OnOverflow: sluice.DropNewest}))

	if produced > 8 {
		t.Errorf("build side produced %d batches for a limit of 4: it kept reading past the bound", produced)
	}
}

// A join that is never consumed must read nothing, like every other operator
// here. Materialising the build side at construction would break that.
func TestJoinIsLazy(t *testing.T) {
	touched := false
	build := sluice.Stream[customer](func(yield func(sluice.Batch[customer]) bool) {
		touched = true
		yield(sluice.Batch[customer]{Items: []customer{{1, "a"}}})
	})

	_ = StreamTable(sluice.Of([]order{{10, 1}}, 1), build,
		orderCustomer, customerID, joinLabel, okLimit(10))

	if touched {
		t.Error("constructing a join read its build side: it must be lazy like every other operator")
	}
}

// The consumer's refusal must reach the probe side (S10). Output is
// re-batched through the emitter, so the refusal propagates at the next
// batch boundary: the single build key here carries enough rows that the
// first probe element alone fills a batch, which is what lets the refusal
// reach the probe on its very first yield.
func TestJoinPropagatesEarlyStop(t *testing.T) {
	build := make([]customer, 2*sluice.DefaultBatchSize)
	for i := range build {
		build[i] = customer{id: 1, name: "a"}
	}

	released := false
	produced := 0
	probe := sluice.Stream[order](func(yield func(sluice.Batch[order]) bool) {
		defer func() { released = true }()
		for i := range 1000 {
			produced++
			if !yield(sluice.Batch[order]{Items: []order{{id: i, customerID: 1}}}) {
				return
			}
		}
	})

	n := 0
	StreamTable(probe, sluice.Of(build, sluice.DefaultBatchSize),
		orderCustomer, customerID, joinLabel, okLimit(len(build)))(
		func(sluice.Batch[string]) bool {
			n++
			return false
		})

	if n != 1 {
		t.Errorf("consumer saw %d batches after refusing the first, want 1", n)
	}
	if produced != 1 {
		t.Errorf("probe produced %d batches, want 1: the refusal did not reach it", produced)
	}
	if !released {
		t.Error("the probe side never unwound")
	}
}

func TestJoinRejectsBadArguments(t *testing.T) {
	probe := sluice.Of([]order{{10, 1}}, 1)
	build := sluice.Of([]customer{{1, "a"}}, 1)

	tests := []struct {
		name string
		call func()
	}{
		{"nil keyA", func() {
			StreamTable[order, customer, int, string](probe, build, nil, customerID, joinLabel, okLimit(10))
		}},
		{"nil keyB", func() {
			StreamTable[order, customer, int, string](probe, build, orderCustomer, nil, joinLabel, okLimit(10))
		}},
		{"nil merge", func() {
			StreamTable[order, customer, int, string](probe, build, orderCustomer, customerID, nil, okLimit(10))
		}},
		{"zero limit", func() {
			StreamTable(probe, build, orderCustomer, customerID, joinLabel, BuildLimit{})
		}},
		{"negative MaxEntries", func() {
			StreamTable(probe, build, orderCustomer, customerID, joinLabel,
				BuildLimit{MaxEntries: -1, OnOverflow: sluice.Fail})
		}},
		{
			// A hash table has no oldest entry; accepting the policy and
			// behaving as sluice.DropNewest would offer a choice that does nothing.
			"DropOldest is refused rather than silently reinterpreted",
			func() {
				StreamTable(probe, build, orderCustomer, customerID, joinLabel,
					BuildLimit{MaxEntries: 10, OnOverflow: sluice.DropOldest})
			},
		},
		{"undeclared policy", func() {
			StreamTable(probe, build, orderCustomer, customerID, joinLabel,
				BuildLimit{MaxEntries: 10, OnOverflow: sluice.Overflow(99)})
		}},
		{"nil table", func() {
			StreamTableWith[order, customer, int, string](probe, build,
				orderCustomer, customerID, joinLabel, okLimit(10), nil)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("expected a panic at construction rather than a later surprise")
				}
			}()
			tc.call()
		})
	}
}

// countingTable proves the storage seam is real: a caller can supply their own
// and the join uses it rather than its default.
type countingTable[K comparable, B any] struct {
	inner *mapTable[K, B]
	puts  int
	gets  int
}

func (t *countingTable[K, B]) Put(k K, v B) { t.puts++; t.inner.Put(k, v) }
func (t *countingTable[K, B]) Get(k K) []B  { t.gets++; return t.inner.Get(k) }
func (t *countingTable[K, B]) Len() int     { return t.inner.Len() }

func TestJoinStreamTableWithCustomTable(t *testing.T) {
	table := &countingTable[int, customer]{inner: newMapTable[int, customer](8)}

	got := sluice.Collect(StreamTableWith(
		sluice.Of([]order{{10, 1}, {11, 2}}, 2), sluice.Of([]customer{{1, "ana"}, {2, "bo"}}, 2),
		orderCustomer, customerID, joinLabel, okLimit(10), table))

	if want := []string{"ana:10", "bo:11"}; !slices.Equal(got, want) {
		t.Errorf("join = %v, want %v", got, want)
	}
	if table.puts != 2 {
		t.Errorf("the supplied table saw %d puts, want 2: the join used its own storage instead", table.puts)
	}
	if table.gets != 2 {
		t.Errorf("the supplied table saw %d gets, want 2", table.gets)
	}
}

func TestMapTable(t *testing.T) {
	tbl := newMapTable[int, string](4)

	if tbl.Len() != 0 || tbl.Get(1) != nil {
		t.Error("a new table must be empty")
	}

	tbl.Put(1, "a")
	tbl.Put(1, "b")
	tbl.Put(2, "c")

	if got, want := tbl.Len(), 3; got != want {
		t.Errorf("Len = %d, want %d: it counts values, not keys", got, want)
	}
	if got := tbl.Get(1); !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("Get(1) = %v, want [a b] in insertion order", got)
	}
	if got := tbl.Get(99); got != nil {
		t.Errorf("Get on a missing key = %v, want nil", got)
	}
}

// The default table addresses its chain with int32, so a limit past that range
// is rejected at construction rather than left to overflow silently. A caller
// who needs more supplies their own table.
func TestJoinRejectsLimitPastTheChainRange(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("expected a panic: a limit past the default table's range must be refused")
		}
	}()

	StreamTable(sluice.Of([]order{{10, 1}}, 1), sluice.Of([]customer{{1, "a"}}, 1),
		orderCustomer, customerID, joinLabel,
		BuildLimit{MaxEntries: maxBuildEntries + 1, OnOverflow: sluice.Fail})
}

// The ceiling is the default table's, not the join's: a caller-supplied table
// is the documented way past it, so StreamTableWith must accept the limit
// StreamTable refuses, and the refusal must name the ceiling it applied.
func TestJoinLimitCeilingBelongsToTheDefaultTable(t *testing.T) {
	big := BuildLimit{MaxEntries: maxBuildEntries + 1, OnOverflow: sluice.Fail}

	func() {
		defer func() {
			msg, _ := recover().(string)
			if !strings.Contains(msg, "2^31-1") {
				t.Errorf("StreamTable panicked with %q, want a message naming the 2^31-1 ceiling", msg)
			}
		}()
		StreamTable(sluice.Of([]order{{10, 1}}, 1), sluice.Of([]customer{{1, "a"}}, 1),
			orderCustomer, customerID, joinLabel, big)
	}()

	table := &countingTable[int, customer]{inner: newMapTable[int, customer](8)}
	got := sluice.Collect(StreamTableWith(
		sluice.Of([]order{{10, 1}}, 1), sluice.Of([]customer{{1, "ana"}}, 1),
		orderCustomer, customerID, joinLabel, big, table))
	if want := []string{"ana:10"}; !slices.Equal(got, want) {
		t.Errorf("join over a custom table = %v, want %v", got, want)
	}
}

// The build table doubles its map toward the caller's limit rather than
// letting the runtime grow it one insertion at a time, and that growth path
// was reached by benchmarks alone — which do not count. A join wide enough to
// cross several doubling boundaries exercises it, and the assertion is that
// every value is still retrievable afterwards: a rehash that lost or
// misplaced an entry is exactly what a growth path gets wrong.
func TestMapTableGrowsAndKeepsEveryValue(t *testing.T) {
	const (
		keys       = 500 // several doublings past initialBuildTable
		perKey     = 3   // and a chain under each, so the reversal is exercised too
		maxEntries = keys * perKey
	)
	table := newMapTable[int64, int64](maxEntries)

	for k := range int64(keys) {
		for j := range int64(perKey) {
			table.Put(k, k*100+j)
		}
	}
	if table.Len() != keys*perKey {
		t.Fatalf("Len = %d, want %d", table.Len(), keys*perKey)
	}
	// The map was grown deliberately rather than left to the runtime.
	if table.reserved <= initialBuildTable {
		t.Errorf("reserved = %d, want it grown past the initial %d", table.reserved, initialBuildTable)
	}
	if table.reserved > maxEntries {
		t.Errorf("reserved = %d, past the limit of %d", table.reserved, maxEntries)
	}

	for k := range int64(keys) {
		got := table.Get(k)
		if len(got) != perKey {
			t.Fatalf("key %d has %d values, want %d", k, len(got), perKey)
		}
		// Values come back in insertion order, which is what makes the output
		// deterministic.
		for j, v := range got {
			if want := k*100 + int64(j); v != want {
				t.Fatalf("key %d value %d is %d, want %d", k, j, v, want)
			}
		}
	}
	if got := table.Get(keys + 1); got != nil {
		t.Errorf("an absent key returned %v", got)
	}
}

// Growth stops at the limit: a table sized from a large MaxEntries must not
// keep doubling past what the join will ever hold.
func TestMapTableGrowthStopsAtTheLimit(t *testing.T) {
	const limit = 100
	table := newMapTable[int64, int64](limit)
	for k := range int64(limit) {
		table.Put(k, k)
	}
	if table.reserved > limit {
		t.Errorf("reserved = %d, want at most the limit %d", table.reserved, limit)
	}
	// And a limit below the initial reservation is honoured rather than
	// rounded up to it.
	small := newMapTable[int64, int64](4)
	if small.reserved != 4 {
		t.Errorf("a limit of 4 reserved %d", small.reserved)
	}
}

// reportTo is the Report callback the tests wire: it raises the Critical
// diagnostic a caller choosing a dropping policy would raise.
func reportTo(diags *diagnostics.Collector) func(limit, held int) {
	return func(limit, held int) {
		diags.Add(diagnostics.NewDiagnostic(diagnostics.Critical, "Sluice.Join.BuildTableOverflow", diagnostics.Path{}).
			WithMessage("BuildTableOverflow", map[string]any{"limit": limit, "held": held}))
	}
}
