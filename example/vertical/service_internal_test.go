package vertical

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/diagnostics"
	"github.com/mlagarrigue/sluice/gateway"
	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/web"
)

func TestAmend(t *testing.T) {
	current := Line{OrderID: 7, Index: 2, SaleQty: 5, Price: 1000}
	tests := []struct {
		name     string
		current  Line
		qty      int64
		wantQty  int64
		wantCode string
		severity diagnostics.Severity
		remedy   bool
	}{
		{"an ordinary change is applied", current, 8, 8, "", 0, false},
		{"zero is refused", current, 0, 5, "Sales.Order.QtyMustBePositive", diagnostics.Error, true},
		{"a negative is refused", current, -3, 5, "Sales.Order.QtyMustBePositive", diagnostics.Error, true},
		{"ten times is still ordinary", current, 50, 50, "", 0, false},
		{"over ten times is applied and flagged", current, 51, 51, "Sales.Order.QtyFarAbovePrevious", diagnostics.Warning, false},
		{"from zero nothing is far above", Line{OrderID: 7, Index: 2}, 1000, 1000, "", 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			line, diags, remedies := Amend(tc.current, Amendment{OrderID: 7, Index: 2, SaleQty: tc.qty})
			if line.SaleQty != tc.wantQty {
				t.Errorf("saleQty = %d, want %d", line.SaleQty, tc.wantQty)
			}
			if line.Price != tc.current.Price || line.OrderID != 7 || line.Index != 2 {
				t.Errorf("the rule touched more than the quantity: %+v", line)
			}
			if tc.wantCode == "" {
				if len(diags) != 0 || len(remedies) != 0 {
					t.Errorf("diagnostics %v, remedies %v; want none", diags, remedies)
				}
				return
			}
			if len(diags) != 1 || diags[0].Code != tc.wantCode || diags[0].Severity != tc.severity {
				t.Fatalf("diagnostics = %+v, want one %s", diags, tc.wantCode)
			}
			if got := diags[0].Path.String(); got != "saleQty" {
				t.Errorf("path = %q, want saleQty", got)
			}
			if tc.remedy != (len(remedies) == 1) {
				t.Fatalf("remedies = %+v, want one: %v", remedies, tc.remedy)
			}
			if tc.remedy && remedies[0].Target != "/orders/7/lines/2" {
				t.Errorf("remedy target = %q", remedies[0].Target)
			}
		})
	}
}

// fixtureRows mirrors vertical_test.go's table: two tenants, two lines each.
func fixtureRows() []fakeRow {
	return []fakeRow{
		{tenant: "acme", line: Line{OrderID: 1, Index: 0, SaleQty: 5, Price: 1000}},
		{tenant: "acme", line: Line{OrderID: 1, Index: 1, SaleQty: 3, Price: 500}},
		{tenant: "globex", line: Line{OrderID: 2, Index: 0, SaleQty: 7, Price: 2000}},
		{tenant: "globex", line: Line{OrderID: 2, Index: 1, SaleQty: 1, Price: 250}},
	}
}

func fakeService(t *testing.T, pg *fakePG) *Service {
	t.Helper()
	s := NewService(pg.conn(t), 64, time.Millisecond)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func bounded(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// Apply, serve and serveGroup against a backend that enforces the tenant
// policy: a principal sees its own lines, the rule decides on them, and a
// line under another tenant is not found rather than refused.
func TestApplyThroughTheService(t *testing.T) {
	s := fakeService(t, &fakePG{rows: fixtureRows()})
	ctx := bounded(t)

	tests := []struct {
		name     string
		tenant   string
		want     Amendment
		accepted bool
		qty      int64
		code     string
	}{
		{"own line, accepted", "acme", Amendment{OrderID: 1, Index: 0, SaleQty: 6}, true, 6, ""},
		{"own line, refused by the rule", "acme", Amendment{OrderID: 1, Index: 1, SaleQty: 0}, false, 3, "Sales.Order.QtyMustBePositive"},
		{"own line, flagged", "globex", Amendment{OrderID: 2, Index: 1, SaleQty: 11}, true, 11, "Sales.Order.QtyFarAbovePrevious"},
		{"another tenant's line is absent", "acme", Amendment{OrderID: 2, Index: 0, SaleQty: 1}, false, 0, "Sales.Order.LineNotFound"},
		{"no such line", "globex", Amendment{OrderID: 9, Index: 0, SaleQty: 1}, false, 0, "Sales.Order.LineNotFound"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := s.Apply(ctx, tc.tenant, tc.want)
			if err != nil {
				t.Fatal(err)
			}
			if res.Accepted != tc.accepted || res.Line.SaleQty != tc.qty {
				t.Errorf("accepted %v qty %d, want %v %d", res.Accepted, res.Line.SaleQty, tc.accepted, tc.qty)
			}
			if tc.code == "" {
				if len(res.Diagnostics) != 0 {
					t.Errorf("diagnostics = %+v, want none", res.Diagnostics)
				}
			} else if len(res.Diagnostics) != 1 || res.Diagnostics[0].Code != tc.code {
				t.Errorf("diagnostics = %+v, want %s", res.Diagnostics, tc.code)
			}
		})
	}

	batches, trips := s.Stats()
	if batches == 0 || trips != batches*3+batches*2 {
		// One tenant per batch here: three for the frame, two for the tenant.
		t.Errorf("stats: %d batches, %d round trips; want 5 per single-tenant batch", batches, trips)
	}
}

// A failure anywhere in a batch's transaction answers its callers with that
// failure — never with a business answer it did not compute.
func TestServeFailures(t *testing.T) {
	tests := []struct {
		name string
		rows []fakeRow
		fail string
		want string
	}{
		{"BEGIN fails", fixtureRows(), "BEGIN", "simulated failure"},
		{"the query fails", fixtureRows(), "order_lines", "simulated failure"},
		{
			"a row will not decode",
			[]fakeRow{{tenant: "acme", line: Line{OrderID: 1}, nullSale: true}},
			"", "decoding an order line",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pg := &fakePG{rows: tc.rows}
			if tc.fail != "" {
				pg.fail(tc.fail)
			}
			s := fakeService(t, pg)
			ctx := bounded(t)
			_, err := s.Apply(ctx, "acme", Amendment{OrderID: 1, Index: 0, SaleQty: 2})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want one mentioning %q", err, tc.want)
			}
			// The connection survived, and the next batch is served.
			if tc.fail != "" {
				res, err := s.Apply(ctx, "acme", Amendment{OrderID: 1, Index: 0, SaleQty: 2})
				if err != nil || !res.Accepted {
					t.Errorf("after the failure: %+v, %v", res, err)
				}
			}
		})
	}
}

// fakeService over a gateway with no database, for the transport half: the
// handler's routing, refusals and rendering are what is under test.
func gatewayService(t *testing.T, run func(request) (Result, error)) *Service {
	t.Helper()
	s := &Service{}
	s.gw = gateway.New(gateway.Config{Size: 64, Within: time.Millisecond},
		func(calls sluice.Stream[*gateway.Call[request, Result]]) {
			calls(func(b sluice.Batch[*gateway.Call[request, Result]]) bool {
				for _, c := range b.Items {
					out, err := run(c.In)
					if err != nil {
						c.Fail(err)
						continue
					}
					c.Reply(out)
				}
				return true
			})
		})
	t.Cleanup(func() { _ = s.gw.Close() })
	return s
}

// tokens is a verifier that knows a fixed set of tokens.
type tokens map[string]web.Principal

func (v tokens) Verify(tok string) (web.Principal, error) {
	if p, ok := v[tok]; ok {
		return p, nil
	}
	return web.Principal{}, web.ErrTokenSignature
}

func TestStreamHandler(t *testing.T) {
	var seen []request
	s := gatewayService(t, func(r request) (Result, error) {
		seen = append(seen, r)
		if r.want.OrderID == 13 {
			return Result{}, errors.New("backend down")
		}
		line, diags, remedies := Amend(Line{OrderID: r.want.OrderID, Index: r.want.Index, SaleQty: 5}, r.want)
		return Result{Line: line, Accepted: len(diags) == 0 || diags[0].Severity < diagnostics.Error, Diagnostics: diags, Affordances: remedies}, nil
	})
	v := tokens{"acme": {Subject: "u", Tenant: "acme"}, "nobody": {Subject: "u"}}
	h := StreamHandler(s, v, web.ClaimAuthorizer{}, 5*time.Second)

	req := func(method, target, tok, body string) httpstream.Request {
		r := httpstream.Request{Method: []byte(method), Target: []byte(target), Body: []byte(body)}
		r.Headers = append(r.Headers, httpstream.Header{Name: []byte("content-type"), Value: []byte("application/json")})
		if tok != "" {
			r.Headers = append(r.Headers, httpstream.Header{Name: []byte("authorization"), Value: []byte("Bearer " + tok)})
		}
		return r
	}
	tests := []struct {
		name   string
		req    httpstream.Request
		status int
		want   string // a substring of the body
	}{
		{"accepted", req("PATCH", "/orders/1/lines/0", "acme", `{"saleQty":6}`), 200, `"accepted":true`},
		{"refused by the rule", req("PATCH", "/orders/1/lines/0", "acme", `{"saleQty":0}`), 422, "Sales.Order.QtyMustBePositive"},
		{"no route", req("PATCH", "/nowhere", "acme", `{}`), 404, "NoRoute"},
		{"wrong method", req("GET", "/orders/1/lines/0", "acme", ``), 405, "MethodNotAllowed"},
		{"no credentials", req("PATCH", "/orders/1/lines/0", "", `{"saleQty":1}`), 401, web.CodeUnauthenticated},
		{"forged credentials", req("PATCH", "/orders/1/lines/0", "forged", `{"saleQty":1}`), 401, web.CodeUnauthenticated},
		{"no tenant", req("PATCH", "/orders/1/lines/0", "nobody", `{"saleQty":1}`), 403, ""},
		{"order is not a number", req("PATCH", "/orders/x/lines/0", "acme", `{"saleQty":1}`), 400, "order"},
		{"line is not a number", req("PATCH", "/orders/1/lines/x", "acme", `{"saleQty":1}`), 400, "line"},
		{"body is not JSON", req("PATCH", "/orders/1/lines/0", "acme", `{`), 400, ""},
		{"the gateway fails", req("PATCH", "/orders/13/lines/0", "acme", `{"saleQty":1}`), 503, web.CodeInternal},
	}
	batch := make([]httpstream.Request, len(tests))
	for i, tc := range tests {
		batch[i] = tc.req
	}
	// One transport batch: refusals keep their place among the served.
	out := h(sluice.Batch[httpstream.Request]{Items: batch})
	if out.Len() != len(tests) {
		t.Fatalf("%d responses for %d requests", out.Len(), len(tests))
	}
	for i, tc := range tests {
		r := out.Items[i]
		if r.Status != tc.status {
			t.Errorf("%s: status %d, want %d; body %s", tc.name, r.Status, tc.status, r.Body)
		}
		if !json.Valid(r.Body) {
			t.Errorf("%s: body is not JSON: %q", tc.name, r.Body)
		}
		if !strings.Contains(string(r.Body), tc.want) {
			t.Errorf("%s: body %s does not mention %q", tc.name, r.Body, tc.want)
		}
	}
	// Only the three requests that passed every stage reached the gateway,
	// with the tenant from the verified principal.
	if len(seen) != 3 {
		t.Fatalf("the gateway saw %d calls, want 3: %+v", len(seen), seen)
	}
	for _, r := range seen {
		if r.tenant != "acme" {
			t.Errorf("call carried tenant %q", r.tenant)
		}
	}
}
