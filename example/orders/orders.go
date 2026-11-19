// Package orders is the architecture's web vertical, working: an HTTP PATCH served by a
// pipeline that is built once and fed requests as elements.
//
// It is an example rather than API — nothing here promises compatibility —
// and it exists because a specification that has never carried a request is a
// specification nobody has tested. This is where `Source`, `Diagnostic` and
// `Affordance` meet something real.
//
// Three decisions in it come from measurements taken earlier, and they are the
// reason it is shaped this way rather than the obvious way.
//
// **The pipeline is built once, at startup.** Building six operators per
// request costs 138 ns and seven allocations of the 280 ns and thirteen a
// request pays (measured, see docs/benchmarks.md) — half the cost, for a pipeline
// shape the model does not require. Here the pipeline exists before the server
// does and requests enter it as elements.
//
// **The store is asked once per batch, not once per request.** That is the
// whole thesis, and the only part of it that matters: against a bounded
// backend, batching measured ×11.9 the throughput of one query per call
// (docs/benchmarks.md). Sixty-four requests, one lookup.
//
// **A rejected request is answered, never filtered out.** The gateway routes a
// reply through the element itself, so an element removed from the stream is a
// caller left waiting for its context to expire. Rejection travels as
// diagnostics attached to the entity, which is what the architecture means by "an element
// can be valid, carry three warnings, and keep moving forward" — and each
// diagnostic carries the affordance that says how to fix it.
//
// # Run it
//
// `go test ./example/orders/` runs it on the in-memory store. With
// SLUICE_PG set — the devcontainer sets it; CONTRIBUTING.md says how to
// start a server elsewhere — TestIntegration runs the same path against
// PostgreSQL.
package orders

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/diagnostics"
	"github.com/mlagarrigue/sluice/gateway"
)

// Line is one order line, the thing being amended.
type Line struct {
	OrderID int64
	Index   int
	SaleQty int64
	Price   int64 // in cents, because a price is not a float64
}

// Amendment is what a request asks for: a new quantity on one line.
type Amendment struct {
	OrderID int64
	Index   int
	SaleQty int64
}

// Result is what comes back: the amended line when it was applied, and always
// the diagnostics and the remedies for whatever was not.
type Result struct {
	Line        Line
	Applied     bool
	Diagnostics []diagnostics.Diagnostic
	Affordances []diagnostics.Affordance
}

// Store is the batched read the vertical is built around.
//
// It takes every key of a batch at once, which is the only shape that lets one
// round trip answer many requests. The PostgreSQL connector plugs in here:
// `postgres.Conn.Query` over the keys passed as arrays — `unnest` of the
// order and line arrays in PGStore — is exactly this signature with a
// network in the middle.
type Store interface {
	// LoadLines returns the lines for the given (order, index) pairs, in one
	// call. Missing keys are simply absent from the result.
	LoadLines(ctx context.Context, keys []LineKey) (map[LineKey]Line, error)

	// SaveLines writes amended lines, in one call.
	SaveLines(ctx context.Context, lines []Line) error
}

// LineKey identifies one line.
type LineKey struct {
	OrderID int64
	Index   int
}

// maxQty is the business rule the example carries: a sale quantity above it is
// refused, with an affordance saying how to ask for it properly.
const maxQty int64 = 1000

// Service holds the pipeline and the gateway in front of it. Build one at
// startup with [New] and serve every request through it.
type Service struct {
	gw *gateway.Gateway[Amendment, Result]
}

// validate applies the business rules, attaching a remedy to each refusal.
func validate(a Amendment, st *state) bool {
	st.valid = true
	at := pathOf(a)

	if a.Index < 0 {
		st.reject(diagnostics.SeverityError, "Sales.Line.BadIndex", "IndexMustNotBeNegative",
			diagnostics.Field("lines").Index(a.Index), diagnostics.OriginBusinessRule,
			diagnostics.Affordance{})
	}
	// The store keeps the index as an int4 (pgstore.go): one past its range
	// would wrap into another line's index rather than fail.
	if a.Index > math.MaxInt32 {
		st.reject(diagnostics.SeverityError, "Sales.Line.BadIndex", "IndexTooLarge",
			diagnostics.Field("lines").Index(a.Index), diagnostics.OriginBusinessRule,
			diagnostics.Affordance{})
	}
	if a.SaleQty <= 0 {
		st.reject(diagnostics.SeverityError, "Sales.Line.InvalidQty", "QtyMustBeOverZero",
			at, diagnostics.OriginBusinessRule,
			diagnostics.Affordance{
				Path:   at,
				Action: "Sales.Line.CorrectQty",
				Method: "PATCH",
				Target: fmt.Sprintf("/orders/%d/lines/%d", a.OrderID, a.Index),
			}.WithInput(diagnostics.Input{
				Name: "saleQty", Kind: "number", Required: true,
				Pattern: `^[1-9][0-9]*$`,
			}))
	}
	if a.SaleQty > maxQty {
		st.reject(diagnostics.SeverityWarning, "Sales.Line.QtyAboveLimit", "QtyNeedsApproval",
			at, diagnostics.OriginBusinessRule,
			diagnostics.Affordance{
				Path:   at,
				Action: "Sales.Line.RequestApproval",
				Method: "POST",
				Target: fmt.Sprintf("/orders/%d/approvals", a.OrderID),
			}.WithInput(diagnostics.Input{Name: "saleQty", Kind: "number", Required: true, Value: strconv.FormatInt(a.SaleQty, 10)}).
				WithInput(diagnostics.Input{Name: "justification", Kind: "string", Required: true}))
	}
	return st.valid
}

// New builds the vertical over a store.
//
// batch and within are the gateway's: how many requests share a round trip,
// and how long the first of them may wait for company. Both are the caller's,
// because neither has a value that suits every deployment — 64 and 2 ms is a
// reasonable starting point for a service in front of a database.
func New(store Store, batch int, within time.Duration) *Service {
	return NewWithStoreTimeout(store, batch, within, DefaultStoreTimeout)
}

// DefaultStoreTimeout is the bound [New] puts on each batch's store calls.
const DefaultStoreTimeout = 5 * time.Second

// NewWithStoreTimeout is [New] with the bound on each batch's LoadLines and
// SaveLines chosen by the caller.
//
// The batch's store calls run detached from every caller's context — one
// caller cancelling must not cancel the others riding with it — so this
// timeout is the only thing that bounds them. What suits a local database
// does not suit one across a region, which is why it is not fixed here.
func NewWithStoreTimeout(store Store, batch int, within, storeTimeout time.Duration) *Service {
	// The pipeline, built here rather than per request. Everything below runs
	// on the gateway's goroutine, one batch at a time.
	run := &batchRun{store: store, storeTimeout: storeTimeout}
	pipeline := func(calls sluice.Stream[*gateway.Call[Amendment, Result]]) {
		calls(func(b sluice.Batch[*gateway.Call[Amendment, Result]]) bool {
			run.serve(b)
			return true
		})
	}
	return &Service{gw: gateway.New(gateway.Config{Size: batch, Within: within}, pipeline)}
}

// batchRun is the pipeline's work on one batch, in five steps. Its buffers
// are reused across every batch for the life of the service, which is what
// the batch model is for and what a per-request pipeline cannot have.
type batchRun struct {
	store        Store
	storeTimeout time.Duration
	keys         []LineKey
	saves        []Line
}

// serve runs the five steps over one batch: validate, one read, apply, one
// write, answer. Nothing is dropped: every call is answered or failed.
func (r *batchRun) serve(b sluice.Batch[*gateway.Call[Amendment, Result]]) {
	states := r.validate(b)

	// One read for the whole batch — the point of all of this. Detached
	// from the callers' own contexts on purpose: one caller's cancellation
	// must not cancel the batch of 64 others riding with it, so this is
	// bounded by the service's own timeout.
	ctx, cancel := context.WithTimeout(context.Background(), r.storeTimeout)
	defer cancel()
	found, err := r.store.LoadLines(ctx, r.keys)
	if err != nil {
		failAll(b, fmt.Errorf("loading lines: %w", err))
		return
	}

	r.apply(b, states, found)

	if len(r.saves) > 0 {
		if err := r.store.SaveLines(ctx, r.saves); err != nil {
			failAll(b, fmt.Errorf("saving lines: %w", err))
			return
		}
	}

	answer(b, states)
}

// validate checks every amendment of the batch and gathers the keys to read.
// An amendment that fails a rule is marked, keeps its place in the batch,
// and is answered.
func (r *batchRun) validate(b sluice.Batch[*gateway.Call[Amendment, Result]]) []state {
	states := make([]state, b.Len())
	r.keys = r.keys[:0]
	for i, c := range b.Items {
		states[i].valid = validate(c.In, &states[i])
		if states[i].valid {
			r.keys = append(r.keys, LineKey{OrderID: c.In.OrderID, Index: c.In.Index})
		}
	}
	return states
}

// apply amends what was found and gathers what to save; a line that was not
// found is refused.
func (r *batchRun) apply(b sluice.Batch[*gateway.Call[Amendment, Result]], states []state, found map[LineKey]Line) {
	r.saves = r.saves[:0]
	for i, c := range b.Items {
		st := &states[i]
		if !st.valid {
			continue
		}
		line, ok := found[LineKey{OrderID: c.In.OrderID, Index: c.In.Index}]
		if !ok {
			st.reject(diagnostics.SeverityCritical, "Sales.Line.NotFound", "LineDoesNotExist",
				pathOf(c.In), diagnostics.OriginSQL, diagnostics.Affordance{})
			continue
		}
		line.SaleQty = c.In.SaleQty
		st.line, st.applied = line, true
		r.saves = append(r.saves, line)
	}
}

// failAll tells every caller of the batch that the store failed: it is not a
// validation problem, and none is left waiting.
func failAll(b sluice.Batch[*gateway.Call[Amendment, Result]], err error) {
	for _, c := range b.Items {
		c.Fail(err)
	}
}

// answer replies to everyone, applied or not.
func answer(b sluice.Batch[*gateway.Call[Amendment, Result]], states []state) {
	for i, c := range b.Items {
		st := &states[i]
		c.Reply(Result{
			Line:        st.line,
			Applied:     st.applied,
			Diagnostics: st.diags,
			Affordances: st.affs,
		})
	}
}

// Apply amends one line and returns what happened. It is safe to call from any
// number of goroutines, which is the point: they share a round trip.
func (s *Service) Apply(ctx context.Context, a Amendment) (Result, error) {
	return s.gw.Do(ctx, a)
}

// Shutdown stops the service and releases its pipeline, waiting for it until
// ctx ends.
func (s *Service) Shutdown(ctx context.Context) error { return s.gw.Shutdown(ctx) }

// ErrClosed reports a service that is shutting down.
var ErrClosed = gateway.ErrClosed

// state is one request's working area inside a batch.
type state struct {
	valid   bool
	applied bool
	line    Line
	diags   []diagnostics.Diagnostic
	affs    []diagnostics.Affordance
}

// reject records why a request was refused and how it could be fixed, which
// travel together because they describe the same value.
func (s *state) reject(sev diagnostics.Severity, code, msg string, at diagnostics.Path, origin diagnostics.Origin, remedy diagnostics.Affordance) {
	s.valid = false
	s.diags = append(s.diags, diagnostics.NewDiagnostic(sev, code, at).
		WithMessage(msg, nil).
		WithOrigin(origin))
	if !remedy.IsZero() {
		s.affs = append(s.affs, remedy)
	}
}

// pathOf locates an amendment in the business model, which is what a client
// needs to show the problem where the user typed it.
func pathOf(a Amendment) diagnostics.Path {
	return diagnostics.Field("lines").Index(a.Index).Field("saleQty")
}
