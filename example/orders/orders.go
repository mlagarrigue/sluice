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
// request costs 122 ns and seven allocations of the 249 ns and thirteen a
// request pays (measured, see docs/benchmarks.md) — half the cost, for a pipeline
// shape the model does not require. Here the pipeline exists before the server
// does and requests enter it as elements.
//
// **The store is asked once per batch, not once per request.** That is the
// whole thesis, and the only part of it that matters: against a bounded
// backend the same handlers measured ×6.9 batched (docs/benchmarks.md). Sixty-four
// requests, one lookup.
//
// **A rejected request is answered, never filtered out.** The gateway routes a
// reply through the element itself, so an element removed from the stream is a
// caller left waiting for its context to expire. Rejection travels as
// diagnostics attached to the entity, which is what the architecture means by "an element
// can be valid, carry three warnings, and keep moving forward" — and each
// diagnostic carries the affordance that says how to fix it.
package orders

import (
	"context"
	"fmt"
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
// `postgres.Conn.Query` with `= ANY($1)` over the keys is exactly this
// signature with a network in the middle.
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
	pipeline := func(calls sluice.Stream[*gateway.Call[Amendment, Result]]) {
		// Buffers reused across every batch for the life of the service, which
		// is what the batch model is for and what a per-request pipeline
		// cannot have.
		var (
			keys  []LineKey
			saves []Line
		)

		calls(func(b sluice.Batch[*gateway.Call[Amendment, Result]]) bool {
			// 1. Validate. Nothing is dropped: an amendment that fails a rule
			// is marked, keeps its place in the batch, and is answered.
			states := make([]state, b.Len())
			keys = keys[:0]
			for i, c := range b.Items {
				states[i].valid = validate(c.In, &states[i])
				if states[i].valid {
					keys = append(keys, LineKey{OrderID: c.In.OrderID, Index: c.In.Index})
				}
			}

			// 2. One read for the whole batch — the point of all of this.
			// Detached from the caller's own ctx on purpose: one caller's
			// cancellation must not cancel the batch of 64 others riding
			// with it, so this is bounded by the service's own timeout.
			ctx, cancel := context.WithTimeout(context.Background(), storeTimeout)
			defer cancel()
			found, err := store.LoadLines(ctx, keys)
			if err != nil {
				// A store failure is not a validation problem: every caller in
				// the batch is told, and none is left waiting.
				for _, c := range b.Items {
					c.Fail(fmt.Errorf("loading lines: %w", err))
				}
				return true
			}

			// 3. Apply the amendment to what was found, gathering what to save.
			saves = saves[:0]
			for i, c := range b.Items {
				st := &states[i]
				if !st.valid {
					continue
				}
				key := LineKey{OrderID: c.In.OrderID, Index: c.In.Index}
				line, ok := found[key]
				if !ok {
					st.reject(diagnostics.Critical, "Sales.Line.NotFound", "LineDoesNotExist",
						pathOf(c.In), diagnostics.OriginSQL, diagnostics.Affordance{})
					continue
				}
				line.SaleQty = c.In.SaleQty
				st.line, st.applied = line, true
				saves = append(saves, line)
			}

			// 4. One write for the whole batch.
			if len(saves) > 0 {
				if err := store.SaveLines(ctx, saves); err != nil {
					for _, c := range b.Items {
						c.Fail(fmt.Errorf("saving lines: %w", err))
					}
					return true
				}
			}

			// 5. Answer everyone, applied or not.
			for i, c := range b.Items {
				st := &states[i]
				c.Reply(Result{
					Line:        st.line,
					Applied:     st.applied,
					Diagnostics: st.diags,
					Affordances: st.affs,
				})
			}
			return true
		})
	}

	return &Service{gw: gateway.New(gateway.Config{Size: batch, Within: within}, pipeline)}
}

// Apply amends one line and returns what happened. It is safe to call from any
// number of goroutines, which is the point: they share a round trip.
func (s *Service) Apply(ctx context.Context, a Amendment) (Result, error) {
	return s.gw.Do(ctx, a)
}

// Close stops the service and releases its pipeline.
func (s *Service) Close() error { return s.gw.Close() }

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

// validate applies the business rules, attaching a remedy to each refusal.
func validate(a Amendment, st *state) bool {
	st.valid = true
	at := pathOf(a)

	if a.Index < 0 {
		st.reject(diagnostics.Error, "Sales.Line.BadIndex", "IndexMustNotBeNegative",
			diagnostics.Field("lines").Index(a.Index), diagnostics.OriginBusinessRule,
			diagnostics.Affordance{})
	}
	if a.SaleQty <= 0 {
		st.reject(diagnostics.Error, "Sales.Line.InvalidQty", "QtyMustBeOverZero",
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
		st.reject(diagnostics.Warning, "Sales.Line.QtyAboveLimit", "QtyNeedsApproval",
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
