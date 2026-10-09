package orders_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mlagarrigue/sluice/diagnostics"
	"github.com/mlagarrigue/sluice/example/orders"
)

// memStore counts its calls, which is how the batching claim is asserted
// rather than described.
type memStore struct {
	mu     sync.Mutex
	lines  map[orders.LineKey]orders.Line
	loads  atomic.Int64
	saves  atomic.Int64
	rows   atomic.Int64
	failOn error
}

func newStore(lines ...orders.Line) *memStore {
	s := &memStore{lines: map[orders.LineKey]orders.Line{}}
	for _, l := range lines {
		s.lines[orders.LineKey{OrderID: l.OrderID, Index: l.Index}] = l
	}
	return s
}

func (s *memStore) LoadLines(_ context.Context, keys []orders.LineKey) (map[orders.LineKey]orders.Line, error) {
	if s.failOn != nil {
		return nil, s.failOn
	}
	s.loads.Add(1)
	s.rows.Add(int64(len(keys)))
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[orders.LineKey]orders.Line, len(keys))
	for _, k := range keys {
		if l, ok := s.lines[k]; ok {
			out[k] = l
		}
	}
	return out, nil
}

func (s *memStore) SaveLines(_ context.Context, lines []orders.Line) error {
	if s.failOn != nil {
		return s.failOn
	}
	s.saves.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, l := range lines {
		s.lines[orders.LineKey{OrderID: l.OrderID, Index: l.Index}] = l
	}
	return nil
}

func ctx5(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 5*time.Second)
}

func TestApplyAmendsALine(t *testing.T) {
	store := newStore(orders.Line{OrderID: 42, Index: 7, SaleQty: 1, Price: 500})
	svc := orders.New(store, 8, 2*time.Millisecond)
	defer func() { _ = svc.Close() }()

	ctx, cancel := ctx5(t)
	defer cancel()

	got, err := svc.Apply(ctx, orders.Amendment{OrderID: 42, Index: 7, SaleQty: 9})
	if err != nil {
		t.Fatalf("Apply returned %v", err)
	}
	if !got.Applied {
		t.Fatalf("the amendment was not applied: %+v", got)
	}
	if got.Line.SaleQty != 9 {
		t.Errorf("SaleQty = %d, want 9", got.Line.SaleQty)
	}
	if got.Line.Price != 500 {
		t.Errorf("Price = %d, want the stored 500 — the read did not reach the entity", got.Line.Price)
	}
	if len(got.Diagnostics) != 0 {
		t.Errorf("a valid amendment carried %d diagnostics", len(got.Diagnostics))
	}
}

// The bet of §4.4, exercised: a refusal carries both why and how to fix it, at
// the same path.
func TestRejectionCarriesItsRemedy(t *testing.T) {
	store := newStore(orders.Line{OrderID: 42, Index: 7, SaleQty: 1})
	svc := orders.New(store, 8, 2*time.Millisecond)
	defer func() { _ = svc.Close() }()

	ctx, cancel := ctx5(t)
	defer cancel()

	got, err := svc.Apply(ctx, orders.Amendment{OrderID: 42, Index: 7, SaleQty: 0})
	if err != nil {
		t.Fatalf("Apply returned %v — a rejected amendment is an answer, not an error", err)
	}
	if got.Applied {
		t.Fatal("an invalid amendment was applied")
	}
	if len(got.Diagnostics) != 1 || len(got.Affordances) != 1 {
		t.Fatalf("got %d diagnostics and %d affordances, want one of each",
			len(got.Diagnostics), len(got.Affordances))
	}

	d, a := got.Diagnostics[0], got.Affordances[0]
	if d.Code != "Sales.Line.InvalidQty" {
		t.Errorf("Code = %q", d.Code)
	}
	// The join: the remedy is at the problem's path, matched by comparison.
	if !a.Path.Equal(d.Path) {
		t.Errorf("the remedy is at %v, the problem at %v", a.Path, d.Path)
	}
	if a.Action != "Sales.Line.CorrectQty" || a.Method != "PATCH" {
		t.Errorf("affordance = %+v", a)
	}
	if len(a.Inputs) != 1 || a.Inputs[0].Name != "saleQty" || !a.Inputs[0].Required {
		t.Errorf("inputs = %+v", a.Inputs)
	}
	if want := "lines[7].saleQty"; d.Path.String() != want {
		t.Errorf("path renders as %q, want %q", d.Path.String(), want)
	}
}

// A warning is not a refusal in the diagnostics model, but this rule treats it
// as one — what matters here is that the element kept its place in the batch
// and came back answered rather than being filtered out.
func TestWarningAlsoCarriesARemedy(t *testing.T) {
	store := newStore(orders.Line{OrderID: 42, Index: 7, SaleQty: 1})
	svc := orders.New(store, 8, 2*time.Millisecond)
	defer func() { _ = svc.Close() }()

	ctx, cancel := ctx5(t)
	defer cancel()

	got, _ := svc.Apply(ctx, orders.Amendment{OrderID: 42, Index: 7, SaleQty: 5000})
	if len(got.Affordances) != 1 || got.Affordances[0].Action != "Sales.Line.RequestApproval" {
		t.Fatalf("affordances = %+v", got.Affordances)
	}
	if n := len(got.Affordances[0].Inputs); n != 2 {
		t.Errorf("the approval action takes %d inputs, want 2", n)
	}
	if got.Diagnostics[0].Severity != diagnostics.Warning {
		t.Errorf("severity = %v, want Warning", got.Diagnostics[0].Severity)
	}
}

// A line the store does not have is refused with a Critical from OriginSQL —
// the origin is what tells a reader the rule came from storage rather than
// from the domain.
func TestMissingLineIsCriticalFromSQL(t *testing.T) {
	svc := orders.New(newStore(), 8, 2*time.Millisecond)
	defer func() { _ = svc.Close() }()

	ctx, cancel := ctx5(t)
	defer cancel()

	got, err := svc.Apply(ctx, orders.Amendment{OrderID: 1, Index: 0, SaleQty: 3})
	if err != nil {
		t.Fatalf("Apply returned %v", err)
	}
	if got.Applied {
		t.Fatal("an amendment to a missing line was applied")
	}
	d := got.Diagnostics[0]
	if d.Severity != diagnostics.Critical || d.Origin != diagnostics.OriginSQL {
		t.Errorf("diagnostic = %v/%v, want Critical/OriginSQL", d.Severity, d.Origin)
	}
}

// The thesis, asserted: concurrent requests share one round trip.
func TestConcurrentRequestsShareOneRoundTrip(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const n = 32
		var lines []orders.Line
		for i := range n {
			lines = append(lines, orders.Line{OrderID: 1, Index: i, SaleQty: 1})
		}
		store := newStore(lines...)
		svc := orders.New(store, n, 50*time.Millisecond)
		defer func() { _ = svc.Close() }()

		var wg sync.WaitGroup
		errs := make(chan error, n)
		for i := range n {
			wg.Go(func() {
				ctx, cancel := ctx5(t)
				defer cancel()
				got, err := svc.Apply(ctx, orders.Amendment{OrderID: 1, Index: i, SaleQty: int64(i + 1)})
				if err != nil {
					errs <- err
					return
				}
				if !got.Applied || got.Line.SaleQty != int64(i+1) {
					errs <- fmt.Errorf("request %d got %+v", i, got)
				}
			})
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}

		if loads := store.loads.Load(); loads != 1 {
			t.Errorf("%d requests caused %d loads, want 1 — the batch did not form", n, loads)
		}
		if rows := store.rows.Load(); rows != n {
			t.Errorf("the single load asked for %d keys, want %d", rows, n)
		}
		if saves := store.saves.Load(); saves != 1 {
			t.Errorf("%d writes, want 1", saves)
		}
	})
}

// Invalid requests do not reach the store, and they do not stop the valid ones
// in their batch from being served.
func TestInvalidRequestsDoNotReachTheStore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newStore(orders.Line{OrderID: 1, Index: 0, SaleQty: 1})
		svc := orders.New(store, 4, 30*time.Millisecond)
		defer func() { _ = svc.Close() }()

		var wg sync.WaitGroup
		results := make([]orders.Result, 2)
		for i, a := range []orders.Amendment{
			{OrderID: 1, Index: 0, SaleQty: 5}, // valid
			{OrderID: 1, Index: 0, SaleQty: 0}, // invalid
		} {
			wg.Go(func() {
				ctx, cancel := ctx5(t)
				defer cancel()
				results[i], _ = svc.Apply(ctx, a)
			})
		}
		wg.Wait()

		if !results[0].Applied {
			t.Error("the valid amendment was not applied")
		}
		if results[1].Applied {
			t.Error("the invalid amendment was applied")
		}
		if rows := store.rows.Load(); rows != 1 {
			t.Errorf("the store was asked for %d keys, want 1 — the invalid request reached it", rows)
		}
	})
}

// A store failure is not a validation problem, and every caller in the batch
// is told rather than left waiting.
func TestStoreFailureReachesEveryCaller(t *testing.T) {
	store := newStore(orders.Line{OrderID: 1, Index: 0, SaleQty: 1})
	store.failOn = errors.New("connection reset")
	svc := orders.New(store, 4, 10*time.Millisecond)
	defer func() { _ = svc.Close() }()

	ctx, cancel := ctx5(t)
	defer cancel()

	_, err := svc.Apply(ctx, orders.Amendment{OrderID: 1, Index: 0, SaleQty: 5})
	if err == nil {
		t.Fatal("a store failure was reported as a successful result")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("the caller waited for its deadline instead of being told")
	}
}

// The eight lines of glue that join net/http to the vertical, exercised. There
// is no adapter in the library for this on purpose: the status codes and the
// encoding are the two decisions that matter, and an adapter would hide them.
func TestOverHTTP(t *testing.T) {
	store := newStore(orders.Line{OrderID: 42, Index: 7, SaleQty: 1, Price: 500})
	svc := orders.New(store, 8, 2*time.Millisecond)
	defer func() { _ = svc.Close() }()

	handler := func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(r.URL.Query().Get("order"), 10, 64)
		if err != nil {
			http.Error(w, "bad order id", http.StatusBadRequest)
			return
		}
		idx, _ := strconv.Atoi(r.URL.Query().Get("line"))
		qty, _ := strconv.ParseInt(r.URL.Query().Get("qty"), 10, 64)

		res, err := svc.Apply(r.Context(), orders.Amendment{OrderID: id, Index: idx, SaleQty: qty})
		if err != nil {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if !res.Applied {
			// The refusal carries its own explanation and its remedies, which
			// is what makes 422 actionable rather than merely correct.
			w.WriteHeader(http.StatusUnprocessableEntity)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"applied":     res.Applied,
			"saleQty":     res.Line.SaleQty,
			"problems":    renderDiagnostics(res.Diagnostics),
			"remedies":    renderAffordances(res.Affordances),
			"contentType": "application/json",
		})
	}

	srv := httptest.NewServer(http.HandlerFunc(handler))
	defer srv.Close()

	t.Run("applied", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "?order=42&line=7&qty=9")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("status = %d, want 200", resp.StatusCode)
		}
		var body map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["applied"] != true || body["saleQty"] != float64(9) {
			t.Errorf("body = %v", body)
		}
	})

	t.Run("refused with a remedy", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "?order=42&line=7&qty=0")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusUnprocessableEntity {
			t.Errorf("status = %d, want 422", resp.StatusCode)
		}
		var body map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		problems, _ := body["problems"].([]any)
		remedies, _ := body["remedies"].([]any)
		if len(problems) != 1 || len(remedies) != 1 {
			t.Fatalf("problems %v, remedies %v", problems, remedies)
		}
		// The client can pair them because they carry the same path.
		p := problems[0].(map[string]any)
		r := remedies[0].(map[string]any)
		if p["path"] != r["path"] {
			t.Errorf("the problem is at %v and the remedy at %v", p["path"], r["path"])
		}
	})
}

func renderDiagnostics(ds []diagnostics.Diagnostic) []map[string]any {
	out := make([]map[string]any, 0, len(ds))
	for _, d := range ds {
		out = append(out, map[string]any{
			"severity":  d.Severity.String(),
			"code":      d.Code,
			"messageId": d.MessageID,
			"path":      d.Path.JSONPointer(),
		})
	}
	return out
}

func renderAffordances(as []diagnostics.Affordance) []map[string]any {
	out := make([]map[string]any, 0, len(as))
	for _, a := range as {
		inputs := make([]map[string]any, 0, len(a.Inputs))
		for _, in := range a.Inputs {
			inputs = append(inputs, map[string]any{
				"name": in.Name, "kind": in.Kind,
				"required": in.Required, "pattern": in.Pattern,
			})
		}
		out = append(out, map[string]any{
			"action": a.Action, "method": a.Method, "target": a.Target,
			"path": a.Path.JSONPointer(), "inputs": inputs,
		})
	}
	return out
}

// The seam this example exists to demonstrate, checked by the compiler: a
// store backed by the connector satisfies the interface the gateway's pipeline
// consumes.
//
// Compiling it is the cheap half. The other half now exists in
// integration_test.go, where the same seam runs against a real PostgreSQL —
// concurrent callers, one gateway batch, one round trip, and the database as
// the assertion. This stays because it costs nothing and fails without a
// server, which is where most readers will be.
var _ orders.Store = orders.NewPGStore(nil)

// stuckStore never answers on its own: only the context it is handed ends a
// call, which is what makes the service's store timeout observable.
type stuckStore struct{}

func (stuckStore) LoadLines(ctx context.Context, _ []orders.LineKey) (map[orders.LineKey]orders.Line, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (stuckStore) SaveLines(ctx context.Context, _ []orders.Line) error {
	<-ctx.Done()
	return ctx.Err()
}

// The store calls run detached from the callers' contexts, so the service's
// own timeout is all that bounds them — and it is the caller's to choose, not
// a five seconds fixed in the pipeline.
func TestStoreTimeoutIsTheCallers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		svc := orders.NewWithStoreTimeout(stuckStore{}, 1, time.Millisecond, 200*time.Millisecond)
		defer func() { _ = svc.Close() }()

		start := time.Now()
		_, err := svc.Apply(context.Background(), orders.Amendment{OrderID: 1, Index: 0, SaleQty: 1})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Apply = %v, want the store call's deadline", err)
		}
		if took := time.Since(start); took >= orders.DefaultStoreTimeout {
			t.Errorf("Apply took %v: the chosen timeout was not the one applied", took)
		}
	})
}
