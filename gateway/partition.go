package gateway

import (
	"errors"
	"fmt"

	"github.com/mlagarrigue/sluice"
)

// Partitions is experimental: the mechanism works and is tested, but no
// measurement yet says when it pays, so its shape may change.
//
// Partitions splits a batch into groups sharing a key, reusing its buffers
// across batches.
//
// # Why this exists
//
// PostgreSQL's row-level security decides visibility from session or
// transaction state — `SET LOCAL app.tenant_id` — which is per connection, not
// per row. Batch two tenants into one transaction and there is no setting that
// is correct for both: set neither and the server hides everything, set one and
// the other tenant's rows are silently absent, set the union and row-level
// security stops being a boundary and becomes a filter the application is now
// responsible for applying correctly.
//
// The last of those is how most systems do it, and it is the one this project
// refuses: it moves a security boundary out of the database and into code, and
// says nothing while doing it. So the security principal is part of the
// batching key. This type is what makes that cheap enough to mean it.
//
//	var parts gateway.Partitions[*gateway.Call[Req, Order], string]
//	calls(func(b sluice.Batch[*gateway.Call[Req, Order]]) bool {
//	    parts.Each(b, func(c *gateway.Call[Req, Order]) string { return c.In.Tenant },
//	        func(tenant string, group []*gateway.Call[Req, Order]) error {
//	            // one SET LOCAL and one query, for this tenant's calls only
//	            return serve(tenant, group)
//	        })
//	    return true
//	})
//
// # What it costs, and where the answer is not in yet
//
// One query per distinct principal in a batch instead of one per batch. On a
// single-goroutine connection those are **sequential** round trips, so the cost
// lands on latency rather than throughput and it lands on every caller in the
// batch — including the ones whose principal was resolved first. Whether that
// is affordable depends on how many distinct principals a real batch carries,
// which is a measurement this repository has not been able to take yet.
// See docs/BRIEF-PARITY.md §0.3: the number decides whether batching survives
// multi-tenancy, and until it exists this type is a mechanism without its
// verdict.
//
// A Partitions is not safe for concurrent use. It is meant to live beside a
// pipeline, on the one goroutine that pipeline runs on.
type Partitions[T any, K comparable] struct {
	index  map[K]int
	keys   []K
	groups [][]T
}

// Each groups the batch by key and calls fn once per distinct key, in the order
// the keys first appear.
//
// The order is first-appearance rather than map order, and that is not
// cosmetic: map order in Go is deliberately randomised, so a pipeline built on
// it would issue its queries in a different sequence every run — untestable,
// and unreproducible the day one of them deadlocks against another.
//
// # Every group is offered, even after one fails
//
// fn is called for **all** groups regardless of what earlier ones returned, and
// the failures are joined into the result. Stopping at the first error would
// leave the remaining groups untouched, and in a request/reply gateway an
// untouched group is a set of callers waiting for a context deadline that has
// nothing to do with them. The rule that makes a batched gateway safe is that
// no call is dropped — see the package documentation — and short-circuiting
// here would break it on the one path where it matters most.
//
// Each failure is wrapped as "partition <key>: <err>", so the joined error
// names every failing key. When the key is a tenant or another principal,
// that error is for the log: relaying it to a client would tell one tenant
// which others shared its batch.
//
// The slice handed to fn borrows this type's storage and is valid only for the
// duration of the call, like every batch in this library.
func (p *Partitions[T, K]) Each(b sluice.Batch[T], key func(T) K, fn func(K, []T) error) error {
	if key == nil || fn == nil {
		panic("gateway: Partitions.Each requires a key function and a callback")
	}
	p.reset()

	// The scrub runs deferred, so a panic in key or fn cannot skip it: the
	// elements are typically *Call values holding a request payload and a
	// reply channel, and a backing array that kept them would pin up to a
	// full batch of payloads across however long the gateway then sits idle —
	// longest exactly when a panic just took the pipeline down.
	//
	// clear over len, not cap: everything past len is already zero, by
	// induction — a fresh backing array starts zero, and every earlier Each
	// zeroed what it wrote. Clearing to cap would pay a memclr proportional
	// to the largest batch ever seen, on every batch forever after.
	//
	// The keys keep their length — [Partitions.Len] answers for the last
	// batch — and lose their values; the index is cleared with them.
	defer func() {
		for i := range p.groups {
			g := p.groups[i]
			clear(g)
			p.groups[i] = g[:0]
		}
		clear(p.keys)
		clear(p.index)
	}()

	for _, item := range b.Items {
		k := key(item)
		i, seen := p.index[k]
		if !seen {
			i = len(p.keys)
			p.index[k] = i
			p.keys = append(p.keys, k)
			if i < len(p.groups) {
				p.groups[i] = p.groups[i][:0] // reuse the slice from a previous batch
			} else {
				p.groups = append(p.groups, nil)
			}
		}
		p.groups[i] = append(p.groups[i], item)
	}

	var errs []error
	for i, k := range p.keys {
		if err := fn(k, p.groups[i]); err != nil {
			errs = append(errs, fmt.Errorf("partition %v: %w", k, err))
		}
	}
	return errors.Join(errs...)
}

// Len reports how many distinct keys the last [Partitions.Each] saw.
//
// It is the number §0.3 says has to be measured rather than assumed — the
// round trips a batch costs — so it is readable rather than inferred from
// counting callbacks.
func (p *Partitions[T, K]) Len() int { return len(p.keys) }

func (p *Partitions[T, K]) reset() {
	if p.index == nil {
		p.index = make(map[K]int)
	} else {
		clear(p.index)
	}
	// The group slices were zeroed and truncated by the previous
	// [Partitions.Each]'s deferred scrub; the keys were zeroed there too and
	// only need truncating here.
	p.keys = p.keys[:0]
}
