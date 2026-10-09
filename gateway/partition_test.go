package gateway

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice"
)

type req struct {
	tenant string
	id     int
}

func batchOf(items ...req) sluice.Batch[req] { return sluice.Batch[req]{Items: items} }

func tenantOf(r req) string { return r.tenant }

// collect runs Each and records what each group saw, in the order the groups
// were offered.
func collect(t *testing.T, p *Partitions[req, string], b sluice.Batch[req]) (keys []string, groups [][]int) {
	t.Helper()
	err := p.Each(b, tenantOf, func(k string, group []req) error {
		ids := make([]int, 0, len(group))
		for _, r := range group {
			ids = append(ids, r.id)
		}
		keys = append(keys, k)
		groups = append(groups, ids)
		return nil
	})
	if err != nil {
		t.Fatalf("Each returned %v", err)
	}
	return keys, groups
}

func TestPartitionGroupsByKey(t *testing.T) {
	var p Partitions[req, string]
	keys, groups := collect(t, &p, batchOf(
		req{"acme", 1}, req{"globex", 2}, req{"acme", 3}, req{"acme", 4}, req{"globex", 5},
	))

	if !slices.Equal(keys, []string{"acme", "globex"}) {
		t.Fatalf("keys = %v, want [acme globex]", keys)
	}
	if !slices.Equal(groups[0], []int{1, 3, 4}) {
		t.Errorf("acme saw %v, want [1 3 4]", groups[0])
	}
	if !slices.Equal(groups[1], []int{2, 5}) {
		t.Errorf("globex saw %v, want [2 5]", groups[1])
	}
	if p.Len() != 2 {
		t.Errorf("Len = %d, want 2 — this is the round-trip count §0.3 measures", p.Len())
	}
}

// Map order in Go is deliberately randomised. A pipeline built on it would
// issue its queries in a different sequence every run: untestable, and
// unreproducible the day two of them deadlock against each other.
func TestPartitionOrderIsFirstAppearanceNotMapOrder(t *testing.T) {
	b := batchOf(
		req{"zulu", 1}, req{"alpha", 2}, req{"mike", 3}, req{"alpha", 4},
		req{"zulu", 5}, req{"bravo", 6}, req{"kilo", 7}, req{"delta", 8},
	)
	want := []string{"zulu", "alpha", "mike", "bravo", "kilo", "delta"}

	// Many runs, and a fresh Partitions each time, so a map iteration that
	// happened to be stable once cannot pass for determinism.
	for range 200 {
		var p Partitions[req, string]
		keys, _ := collect(t, &p, b)
		if !slices.Equal(keys, want) {
			t.Fatalf("keys = %v, want %v", keys, want)
		}
	}
}

// The rule that makes a batched gateway safe is that no call is dropped.
// Stopping at the first failing group would leave the rest untouched, and an
// untouched group is a set of callers waiting on a deadline that has nothing
// to do with them.
func TestPartitionOffersEveryGroupEvenAfterAFailure(t *testing.T) {
	var p Partitions[req, string]
	b := batchOf(req{"a", 1}, req{"b", 2}, req{"c", 3}, req{"d", 4})

	boom := errors.New("backend refused")
	var seen []string
	err := p.Each(b, tenantOf, func(k string, _ []req) error {
		seen = append(seen, k)
		if k == "b" {
			return boom
		}
		return nil
	})

	if !slices.Equal(seen, []string{"a", "b", "c", "d"}) {
		t.Fatalf("groups offered = %v; the ones after the failure were skipped and their callers are waiting", seen)
	}
	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want it to wrap the group's failure", err)
	}
	if !strings.Contains(err.Error(), `partition b`) {
		t.Errorf("err = %q; it does not name the failing partition", err)
	}
}

// Several failures must all survive into the result: a caller deciding what to
// report cannot do it from one of three.
func TestPartitionJoinsEveryFailure(t *testing.T) {
	var p Partitions[req, string]
	b := batchOf(req{"a", 1}, req{"b", 2}, req{"c", 3})

	first, second := errors.New("first"), errors.New("second")
	err := p.Each(b, tenantOf, func(k string, _ []req) error {
		switch k {
		case "a":
			return first
		case "c":
			return second
		}
		return nil
	})
	if !errors.Is(err, first) || !errors.Is(err, second) {
		t.Fatalf("err = %v, want both failures joined", err)
	}
}

// The buffers are reused across batches, which is the point — and reuse is
// exactly how a previous batch's elements leak into the next one's groups.
func TestPartitionReusesBuffersWithoutLeakingBetweenBatches(t *testing.T) {
	var p Partitions[req, string]

	_, groups := collect(t, &p, batchOf(req{"acme", 1}, req{"acme", 2}, req{"globex", 3}))
	if !slices.Equal(groups[0], []int{1, 2}) {
		t.Fatalf("first batch, acme = %v", groups[0])
	}

	// A second batch with fewer elements per key, and a key the first batch
	// never had. If the group slices were not truncated, acme would still be
	// carrying 1 and 2.
	keys, groups := collect(t, &p, batchOf(req{"acme", 9}, req{"initech", 10}))
	if !slices.Equal(keys, []string{"acme", "initech"}) {
		t.Fatalf("keys = %v", keys)
	}
	if !slices.Equal(groups[0], []int{9}) {
		t.Errorf("acme = %v, want [9] — the previous batch's elements survived", groups[0])
	}
	if !slices.Equal(groups[1], []int{10}) {
		t.Errorf("initech = %v, want [10]", groups[1])
	}
	if p.Len() != 2 {
		t.Errorf("Len = %d, want 2", p.Len())
	}
}

// A single-principal batch is the case the architecture is betting on: one
// group, one round trip, nothing lost to partitioning.
func TestPartitionSingleKeyIsOneGroup(t *testing.T) {
	var p Partitions[req, string]
	keys, groups := collect(t, &p, batchOf(req{"acme", 1}, req{"acme", 2}, req{"acme", 3}))
	if len(keys) != 1 || p.Len() != 1 {
		t.Fatalf("a single-tenant batch produced %d groups", p.Len())
	}
	if !slices.Equal(groups[0], []int{1, 2, 3}) {
		t.Errorf("group = %v", groups[0])
	}
}

func TestPartitionEmptyBatch(t *testing.T) {
	var p Partitions[req, string]
	called := false
	err := p.Each(sluice.Batch[req]{}, tenantOf, func(string, []req) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Error("a callback fired for an empty batch")
	}
	if p.Len() != 0 {
		t.Errorf("Len = %d, want 0", p.Len())
	}
}

func TestPartitionRequiresItsFunctions(t *testing.T) {
	for _, tc := range []struct {
		name string
		key  func(req) string
		fn   func(string, []req) error
	}{
		{"no key", nil, func(string, []req) error { return nil }},
		{"no callback", tenantOf, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("Each accepted a nil function")
				}
			}()
			var p Partitions[req, string]
			_ = p.Each(batchOf(req{"a", 1}), tc.key, tc.fn)
		})
	}
}

// The whole point measured: partitioning a batch that is mostly one tenant
// must not allocate per batch, or the mechanism costs more than the round trip
// it saves on the batch where it saves nothing.
func BenchmarkPartitionSingleTenant(b *testing.B) {
	items := make([]req, 64)
	for i := range items {
		items[i] = req{tenant: "acme", id: i}
	}
	batch := sluice.Batch[req]{Items: items}
	var p Partitions[req, string]

	b.ReportAllocs()
	for b.Loop() {
		_ = p.Each(batch, tenantOf, func(string, []req) error { return nil })
	}
}

// And the shape that costs: sixty-four tenants in a batch of sixty-four, which
// is where §0.3 says the gateway has bought nothing.
func BenchmarkPartitionAllDistinct(b *testing.B) {
	items := make([]req, 64)
	for i := range items {
		items[i] = req{tenant: fmt.Sprintf("tenant-%02d", i), id: i}
	}
	batch := sluice.Batch[req]{Items: items}
	var p Partitions[req, string]

	b.ReportAllocs()
	for b.Loop() {
		_ = p.Each(batch, tenantOf, func(string, []req) error { return nil })
	}
}

// retained reports how many element slots the group buffers still hold, over
// their whole capacity rather than their length: a slice truncated to zero
// keeps every pointer its backing array ever held, and that is exactly the
// retention this looks for.
func retained[K comparable](p *Partitions[*req, K]) int {
	n := 0
	for _, g := range p.groups {
		g = g[:cap(g)]
		for _, item := range g {
			if item != nil {
				n++
			}
		}
	}
	return n
}

// The elements are *Call values in the case this type exists for, each holding
// a request payload and the channel its answer goes back through. Truncating
// the group slices between batches leaves all of them live in the backing
// arrays — up to a full batch of requests pinned for as long as the gateway
// then sits idle, which nothing in the API tells the caller about.
func TestPartitionDoesNotRetainTheBatchAfterEach(t *testing.T) {
	var p Partitions[*req, string]
	items := []*req{{"acme", 1}, {"acme", 2}, {"globex", 3}}

	err := p.Each(sluice.Batch[*req]{Items: items}, func(r *req) string { return r.tenant },
		func(string, []*req) error { return nil })
	if err != nil {
		t.Fatal(err)
	}

	if n := retained(&p); n != 0 {
		t.Errorf("%d elements still referenced by the group buffers after Each", n)
	}
	// The buffers themselves are kept: dropping them would allocate per
	// principal per batch, which is allocating per request.
	if len(p.groups) == 0 {
		t.Error("the group buffers were dropped rather than emptied")
	}
}

// The scrub runs deferred, so the one case that most needs it is covered: a
// callback that panics is a pipeline going down, and the batch it was serving
// would otherwise stay pinned for however long the process keeps running.
func TestPartitionScrubsEvenWhenTheCallbackPanics(t *testing.T) {
	var p Partitions[*req, string]
	items := []*req{{"acme", 1}, {"globex", 2}}

	func() {
		defer func() {
			if recover() == nil {
				t.Error("the panic was swallowed")
			}
		}()
		_ = p.Each(sluice.Batch[*req]{Items: items}, func(r *req) string { return r.tenant },
			func(string, []*req) error { panic("the caller's own bug") })
	}()

	if n := retained(&p); n != 0 {
		t.Errorf("%d elements still referenced after a panicking callback", n)
	}
}
