// Package vertical is the whole architecture in one file: an HTTP request
// authenticated, authorised, batched with others by principal, decided by one
// business rule, read from PostgreSQL under row-level security, and answered
// with its problems and the remedies for them.
//
// It is an example rather than API. What it exists to show is that the pieces
// compose without a layer in between, and — because a claim nobody can check is
// decoration — it counts the round trips it makes and says so.
//
// It runs on the experimental transport: the architecture's request table
// with every arrow a stream stage, [httpstream] carrying the outer ones and
// [github.com/mlagarrigue/sluice/web/stream] the
// middle, so a batch of pipelined requests reaches the gateway as one
// submission and costs one query per tenant rather than one per request. The
// supported path's twin is [example/orders], over net/http — that example is
// the proof the supported path keeps working, and this one is the proof the
// experiment serves the objective it was built for.
//
// # Read it top to bottom
//
// The order below is the order a request travels. Nothing calls back upwards.
//
//	Amend          the business rule, and nothing else
//	Service        the gateway: many callers, one batch, one query per tenant
//	StreamHandler  the transport: route, verify, authorise, decode, answer
//
// # The one thing this file does not do
//
// It does not filter by tenant. Not once — there is no `WHERE tenant = ...`
// anywhere in it. The boundary is the database's: a row-level-security policy
// reads the transaction-local setting the pipeline established, and decides
// what exists. Deleting a filter from here could not weaken it, because there
// is none to delete. That is asserted in vertical_test.go by pointing a second
// principal at the same rows.
package vertical

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/diagnostics"
	"github.com/mlagarrigue/sluice/gateway"
	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/web"
	"github.com/mlagarrigue/sluice/web/stream"
)

// --- the business rule ---------------------------------------------------------

// Line is an order line as the database holds it.
type Line struct {
	OrderID int64
	Index   int
	SaleQty int64
	Price   int64 // cents, because a price is not a float64
}

// Amendment is what a caller asks for.
type Amendment struct {
	OrderID int64 `json:"orderId"`
	Index   int   `json:"lineIndex"`
	SaleQty int64 `json:"saleQty"`
}

// Amend decides whether an amendment may be applied, and says why not.
//
// This is the whole business rule, and it is the point of the file: it takes a
// line and a request, returns a line and its problems, and knows nothing about
// HTTP, batching, tenants or SQL. If this function were hard to find in here,
// the architecture would have failed its own claim.
func Amend(current Line, want Amendment) (Line, []diagnostics.Diagnostic, []diagnostics.Affordance) {
	at := diagnostics.Path{}.Field("saleQty")

	switch {
	case want.SaleQty <= 0:
		refused := diagnostics.NewDiagnostic(diagnostics.Error, "Sales.Order.QtyMustBePositive", at).
			WithMessage("QtyMustBeOverZero", map[string]any{"got": want.SaleQty, "min": 1}).
			WithOrigin(diagnostics.OriginBusinessRule)
		correct := diagnostics.Affordance{
			Path: at, Action: "Sales.Order.CorrectQty", Method: "PATCH",
			Target: fmt.Sprintf("/orders/%d/lines/%d", want.OrderID, want.Index),
			Inputs: []diagnostics.Input{{Name: "saleQty", Kind: "integer", Required: true}},
		}
		return current, []diagnostics.Diagnostic{refused}, []diagnostics.Affordance{correct}

	case want.SaleQty > current.SaleQty*10 && current.SaleQty > 0:
		// Accepted, and flagged. An element can be valid, carry a warning, and
		// keep moving — which is why this returns a line rather than an error.
		was := current.SaleQty
		current.SaleQty = want.SaleQty
		return current, []diagnostics.Diagnostic{
			diagnostics.NewDiagnostic(diagnostics.Warning, "Sales.Order.QtyFarAbovePrevious", at).
				WithMessage("QtyUnusuallyLarge", map[string]any{"was": was, "now": want.SaleQty}).
				WithOrigin(diagnostics.OriginBusinessRule),
		}, nil
	}

	current.SaleQty = want.SaleQty
	return current, nil, nil
}

// --- the pipeline --------------------------------------------------------------

// request is one caller's work as it travels through the gateway: what they
// asked for, and who they are.
type request struct {
	tenant string
	want   Amendment
}

// Result is what comes back. Accepted reports whether the business rule
// accepted the amendment — nothing is written back: the vertical reads,
// decides and answers, and its transaction ends in ROLLBACK by design (see
// serve). A caller holding an accepted Result holds a decision, not a change.
type Result struct {
	Line        Line
	Accepted    bool
	Diagnostics []diagnostics.Diagnostic
	Affordances []diagnostics.Affordance
}

// Service is the gateway and the connection behind it.
type Service struct {
	gw   *gateway.Gateway[request, Result]
	conn *postgres.Conn

	// roundTrips counts every round trip the pipeline makes — BEGIN and the
	// statement_timeout it sets, each tenant's SET LOCAL and query, the final
	// ROLLBACK — so the claim that a batch costs a fixed overhead plus two
	// per tenant is readable rather than asserted.
	roundTrips atomic.Int64
	batches    atomic.Int64
}

const readLines = `
SELECT order_id, line_index, sale_qty, price
FROM order_lines
WHERE (order_id, line_index) IN (SELECT * FROM unnest($1::bigint[], $2::int[]))`

// NewService builds the pipeline once, at start-up. Building it per request
// costs half of what a request costs; this is the shape the measurements
// asked for rather than the obvious one.
func NewService(conn *postgres.Conn, size int, within time.Duration) *Service {
	s := &Service{conn: conn}

	s.gw = gateway.New(gateway.Config{Size: size, Within: within},
		func(calls sluice.Stream[*gateway.Call[request, Result]]) {
			var parts gateway.Partitions[*gateway.Call[request, Result], string]

			calls(func(batch sluice.Batch[*gateway.Call[request, Result]]) bool {
				s.batches.Add(1)
				s.serve(&parts, batch)
				return true
			})
		})
	return s
}

// serve handles one batch: partition by tenant, one query per tenant, and the
// business rule applied to what comes back. Every call in the batch is
// answered exactly once, whichever way its group went.
func (s *Service) serve(parts *gateway.Partitions[*gateway.Call[request, Result], string], batch sluice.Batch[*gateway.Call[request, Result]]) {
	// Detached from any caller's ctx on purpose: one caller's cancellation
	// must not cancel the batch of 64 others riding with it. Bounded
	// instead by the transaction's own Timeout, below.
	ctx := context.Background()

	tx, err := s.conn.Begin(ctx, postgres.TxConfig{Timeout: 5 * time.Second})
	if err != nil {
		// No group was offered yet, so nobody has been answered: the whole
		// batch fails together, and nobody is left waiting on a context.
		for _, c := range batch.Items {
			c.Fail(err)
		}
		return
	}
	// Begin with a Timeout is two round trips: BEGIN, then the SET LOCAL
	// statement_timeout it issues. The ROLLBACK below is the third, counted
	// now rather than in the defer: by the time the defer runs, the callers
	// have their answers, and a reader of Stats would see a count one short.
	s.roundTrips.Add(3)

	// The vertical reads and decides; it never writes. The transaction exists
	// to scope the per-tenant SET LOCAL below, so it ends in ROLLBACK by
	// design — there is no Commit to miss.
	defer func() { _ = tx.Rollback() }()

	dispatch(parts, batch, func(tenant string, group []*gateway.Call[request, Result]) error {
		return s.serveGroup(ctx, tx, tenant, group)
	})
}

// dispatch partitions a batch by tenant and answers failures group by group.
// Partitions.Each keeps offering groups after one fails, so by the time it
// returns, the groups that succeeded have already replied to their callers —
// failing the whole batch at that point would answer those calls a second
// time, which panics and takes the gateway down for good. The failing group's
// calls are failed here, inside the callback, where the group is still known.
func dispatch(
	parts *gateway.Partitions[*gateway.Call[request, Result], string],
	batch sluice.Batch[*gateway.Call[request, Result]],
	run func(tenant string, group []*gateway.Call[request, Result]) error,
) {
	// The joined error is dropped knowingly: every call it concerns has
	// already been answered with its own group's failure.
	_ = parts.Each(batch, func(c *gateway.Call[request, Result]) string {
		return c.In.tenant
	}, func(tenant string, group []*gateway.Call[request, Result]) error {
		err := run(tenant, group)
		if err != nil {
			for _, c := range group {
				c.Fail(err)
			}
		}
		return err
	})
}

// serveGroup serves one tenant's calls: establish the principal, one query,
// the business rule on what comes back.
func (s *Service) serveGroup(ctx context.Context, tx *postgres.Tx, tenant string, group []*gateway.Call[request, Result]) error {
	// The security principal, established on the connection. From here the
	// server decides what exists — this code never filters.
	s.roundTrips.Add(1)
	if err := tx.SetLocal(ctx, "app.tenant_id", tenant); err != nil {
		return err
	}

	orderIDs, indexes := lineKeys(group)

	s.roundTrips.Add(1)
	lines := make(map[[2]int64]Line, len(group))
	src := tx.Query(ctx, readLines, [][]byte{
		postgres.AppendInt8Array(nil, orderIDs),
		postgres.AppendInt4Array(nil, indexes),
	}, postgres.QueryConfig{
		BatchRows: 256, AllRows: true,
		ParamOIDs: []uint32{postgres.OIDInt8Array, postgres.OIDInt4Array},
	})
	var decodeErr error
	src.Stream()(func(rows sluice.Batch[postgres.Row]) bool {
		for _, r := range rows.Items {
			line, err := decodeLine(r)
			if err != nil {
				// A row that cannot be decoded is not a row that is absent:
				// answering "not found" for it would turn a read failure into
				// a business answer. Remember the first failure and keep
				// draining, so the connection comes back usable.
				if decodeErr == nil {
					decodeErr = err
				}
				continue
			}
			lines[[2]int64{line.OrderID, int64(line.Index)}] = line
		}
		return true
	})
	if err := src.Err(); err != nil {
		return err
	}
	if decodeErr != nil {
		// Fails the whole group, which dispatch answers as the group's own
		// error — distinct from LineNotFound, which stays reserved for a row
		// the server did not show.
		return fmt.Errorf("decoding an order line: %w", decodeErr)
	}

	for _, c := range group {
		key := [2]int64{c.In.want.OrderID, int64(c.In.want.Index)}
		current, found := lines[key]
		if !found {
			// Absent for this principal. Whether the row does not exist or
			// belongs to someone else is not a distinction this code can
			// make, and that is the boundary working: the server did not
			// show it, so there is nothing here to leak.
			c.Reply(notFound(c.In.want))
			continue
		}
		line, diags, remedies := Amend(current, c.In.want)
		c.Reply(Result{
			Line: line, Accepted: !hasError(diags),
			Diagnostics: diags, Affordances: remedies,
		})
	}
	return nil
}

// lineKeys builds the query's parallel key arrays. line_index is an int4
// column, so an index outside int32 names no row: it is left out of the
// query rather than truncated into one that names a different line, and the
// lookup after the query answers it LineNotFound like any absent row.
func lineKeys(group []*gateway.Call[request, Result]) (orderIDs []int64, indexes []int32) {
	orderIDs = make([]int64, 0, len(group))
	indexes = make([]int32, 0, len(group))
	for _, c := range group {
		idx := c.In.want.Index
		if idx < math.MinInt32 || idx > math.MaxInt32 {
			continue
		}
		orderIDs = append(orderIDs, c.In.want.OrderID)
		indexes = append(indexes, int32(idx))
	}
	return orderIDs, indexes
}

func notFound(want Amendment) Result {
	at := diagnostics.Path{}.Field("orderId")
	return Result{Diagnostics: []diagnostics.Diagnostic{
		diagnostics.NewDiagnostic(diagnostics.Critical, "Sales.Order.LineNotFound", at).
			WithMessage("LineNotFound", map[string]any{
				"orderId": want.OrderID, "lineIndex": want.Index,
			}).
			WithOrigin(diagnostics.OriginBusinessRule),
	}}
}

func hasError(ds []diagnostics.Diagnostic) bool {
	for _, d := range ds {
		if d.Severity >= diagnostics.Error {
			return true
		}
	}
	return false
}

func decodeLine(r postgres.Row) (Line, error) {
	var l Line
	var err error
	if v, isNull := r.Value(0); isNull {
		return l, errors.New("column 0 (order_id) is NULL")
	} else if l.OrderID, err = postgres.DecodeInt8(v); err != nil {
		return l, err
	}
	if v, isNull := r.Value(1); isNull {
		return l, errors.New("column 1 (line_index) is NULL")
	} else {
		idx, err := postgres.DecodeInt4(v)
		if err != nil {
			return l, err
		}
		l.Index = int(idx)
	}
	if v, isNull := r.Value(2); isNull {
		return l, errors.New("column 2 (sale_qty) is NULL")
	} else if l.SaleQty, err = postgres.DecodeInt8(v); err != nil {
		return l, err
	}
	if v, isNull := r.Value(3); !isNull {
		if l.Price, err = postgres.DecodeInt8(v); err != nil {
			return l, err
		}
	}
	return l, nil
}

// Apply submits one amendment and waits for its answer. Called from any number
// of goroutines; the gateway is what turns them into batches.
func (s *Service) Apply(ctx context.Context, tenant string, want Amendment) (Result, error) {
	return s.gw.Do(ctx, request{tenant: tenant, want: want})
}

// Stats reports what the pipeline has done: how many batches it served, and
// how many round trips those cost — every one of them, transaction control
// included. A batch costs three for its frame (BEGIN, its statement_timeout,
// ROLLBACK) plus two per distinct tenant (SET LOCAL, the query). The ratio is
// the architecture's claim, in a form anybody can print.
func (s *Service) Stats() (batches, roundTrips int64) {
	return s.batches.Load(), s.roundTrips.Load()
}

// Close drains the gateway.
func (s *Service) Close() error { return s.gw.Close() }

// --- the transport -------------------------------------------------------------

// StreamHandler joins the experimental transport to the vertical. The status
// code and the encoding stay visible in the render callback on purpose: they
// are the two decisions that matter, and an adapter would hide them.
//
// The shape is the objective in one function: a transport batch arrives, the
// web/stream stages walk it — a refused request keeps its place and its
// diagnostics, never dropped — and what survives goes to the gateway as one
// submission. The company a lone request would wait Within for has already
// arrived on the wire.
//
// timeout bounds one batch's wait on the gateway, the deadline r.Context()
// carried on the supported path; the handler has no request context because
// the transport correlates by position, not by goroutine.
func StreamHandler(s *Service, v web.Verifier, a web.Authorizer, timeout time.Duration) (httpstream.Handler, error) {
	rt, err := stream.NewRouter(
		stream.Route{Method: "PATCH", Pattern: "/orders/{order}/lines/{line}"},
	)
	if err != nil {
		return nil, err
	}

	return func(b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
		ex := stream.Exchanges(nil, b)
		rt.Route(ex)
		stream.Authenticate(v, ex)
		stream.Authorize(a, ex)

		// Decode what is still open: every exchange either is refused, its
		// diagnostics staying in ex, or contributes exactly one call. That is
		// what lets RenderOpen pair the k-th open exchange with outcome k
		// without a mapping kept here.
		calls := make([]request, 0, len(ex))
		for i := range ex {
			e := &ex[i]
			if e.Refused() {
				continue
			}
			order, ok := stream.PathInt64(e, 0, "order")
			if !ok {
				continue
			}
			line, ok := stream.PathInt(e, 1, "line")
			if !ok {
				continue
			}
			var body struct {
				SaleQty int64 `json:"saleQty"`
			}
			if !stream.DecodeJSON(e, 1<<16, &body) {
				continue
			}
			// The tenant comes from the *verified* token by way of the grant,
			// never from anything the request could set.
			calls = append(calls, request{tenant: e.Grant.Tenant, want: Amendment{
				OrderID: order, Index: line, SaleQty: body.SaleQty,
			}})
		}

		// One submission for the transport batch — this line is the claim the
		// package documents, made with the real API: requests that arrived
		// together reach the pipeline together, and one query per tenant
		// answers all of them.
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		outcomes := s.gw.DoBatch(ctx, calls)

		return stream.RenderOpen(ex, func(_, open int) (int, any) {
			o := outcomes[open]
			if o.Err != nil {
				return 503, web.InternalReport()
			}
			return web.Status(o.Out.Diagnostics), map[string]any{
				"accepted": o.Out.Accepted,
				"saleQty":  o.Out.Line.SaleQty,
				"problems": web.Problems(o.Out.Diagnostics),
				"remedies": web.Remedies(o.Out.Affordances),
			}
		})
	}, nil
}
