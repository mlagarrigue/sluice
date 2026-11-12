package web_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/mlagarrigue/sluice/diagnostics"
	"github.com/mlagarrigue/sluice/web"
)

// A body over its limit, or not JSON, is a diagnostic the caller answers;
// a body that fits is decoded into dst.
func ExampleDecodeJSON() {
	var order struct {
		Qty int64 `json:"qty"`
	}
	r := httptest.NewRequest("POST", "/orders", strings.NewReader(`{"qty":3}`))
	r.Header.Set("Content-Type", "application/json")
	if d, ok := web.DecodeJSON(r, 1<<10, &order); !ok {
		fmt.Println(web.TransportStatus(d), d.Code)
		return
	}
	fmt.Println(order.Qty)

	r = httptest.NewRequest("POST", "/orders", strings.NewReader(`{"qty":`))
	r.Header.Set("Content-Type", "application/json")
	_, ok := web.DecodeJSON(r, 1<<10, &order)
	fmt.Println(ok)
	// Output:
	// 3
	// false
}

// A path wildcard read as an integer, or refused with the diagnostic a 400
// carries.
func ExamplePathInt64() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders/{order}", func(w http.ResponseWriter, r *http.Request) {
		order, d, ok := web.PathInt64(r, "order")
		if !ok {
			_ = web.WriteJSON(w, web.TransportStatus(d), web.ReportOf(d))
			return
		}
		_ = web.WriteJSON(w, http.StatusOK, map[string]int64{"order": order})
	})
	for _, target := range []string{"/orders/42", "/orders/forty-two"} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("GET", target, nil))
		fmt.Println(target, rec.Code)
	}
	// Output:
	// /orders/42 200
	// /orders/forty-two 400
}

// The status is chosen on the line that writes; the body is encoded as JSON
// with its content type.
func ExampleWriteJSON() {
	rec := httptest.NewRecorder()
	_ = web.WriteJSON(rec, http.StatusCreated, map[string]string{"id": "o-7"})
	fmt.Println(rec.Code, rec.Header().Get("Content-Type"))
	fmt.Print(rec.Body.String())
	// Output:
	// 201 application/json
	// {"id":"o-7"}
}

// One or more diagnostics, in the envelope a rejection travels in.
func ExampleReportOf() {
	d := diagnostics.NewDiagnostic(diagnostics.Error, "Sales.Order.InvalidQty", diagnostics.Field("qty"))
	rep := web.ReportOf(d)
	fmt.Println(len(rep.Problems), rep.Problems[0].Code, rep.Problems[0].Path)
	// Output: 1 Sales.Order.InvalidQty /qty
}

// Diagnostics in the shape a client reads them: the severity by name, the
// path as a JSON pointer (RFC 6901).
func ExampleProblems() {
	ps := web.Problems([]diagnostics.Diagnostic{
		diagnostics.NewDiagnostic(diagnostics.Warning, "Sales.Order.UnusualQty", diagnostics.Field("lines").Index(2).Field("qty")),
	})
	fmt.Println(ps[0].Severity, ps[0].Code, ps[0].Path)
	// Output: Warning Sales.Order.UnusualQty /lines/2/qty
}

// Affordances in the shape a client reads them, on the same path as the
// problem they fix.
func ExampleRemedies() {
	rs := web.Remedies([]diagnostics.Affordance{{
		Path: diagnostics.Field("qty"), Action: "Sales.Order.CorrectQty",
		Method: "PATCH", Target: "/orders/7",
	}})
	fmt.Println(rs[0].Action, rs[0].Method, rs[0].Target, rs[0].Path)
	// Output: Sales.Order.CorrectQty PATCH /orders/7 /qty
}

// The status a transport diagnostic answers with: a body that is not JSON
// is 415, one over its limit 413.
func ExampleTransportStatus() {
	r := httptest.NewRequest("POST", "/orders", strings.NewReader(`qty=3`))
	d, _ := web.DecodeJSON(r, 1<<10, &struct{}{})
	fmt.Println(web.TransportStatus(d))

	r = httptest.NewRequest("POST", "/orders", strings.NewReader(`{"qty":12345}`))
	r.Header.Set("Content-Type", "application/json")
	d, _ = web.DecodeJSON(r, 4, &struct{}{})
	fmt.Println(web.TransportStatus(d))
	// Output:
	// 415
	// 413
}

// A main function's whole serving loop: Serve blocks while the service is
// up and returns once it has drained, here as soon as ctx ends.
func ExampleServe() {
	srv := &http.Server{
		Addr:              "127.0.0.1:0",
		Handler:           http.NewServeMux(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // in a main: signal.NotifyContext(ctx, os.Interrupt)
	err := web.Serve(ctx, srv, 10*time.Second)
	fmt.Println(err)
	// Output: <nil>
}

// What the principal may do, decided by the caller's rule: here its own
// tenant, and nothing for a principal without one.
func ExampleAuthorize() {
	for _, p := range []web.Principal{{Subject: "ada", Tenant: "acme"}, {Subject: "bob"}} {
		g, _, err := web.Authorize(p, tenantRule{})
		if err != nil {
			fmt.Println(p.Subject, "refused")
			continue
		}
		fmt.Println(p.Subject, "may act for", g.Tenant)
	}
	// Output:
	// ada may act for acme
	// bob refused
}

// The WWW-Authenticate value a 401 carries (RFC 6750 §3): the realm, and the
// error the diagnostic of a refused token names.
func ExampleBearerChallenge() {
	verifier, err := web.NewHMACVerifier(web.HMACConfig{Secret: []byte("a 32-byte secret drawn from crypto/rand!")})
	if err != nil {
		fmt.Println(err)
		return
	}
	r := httptest.NewRequest("GET", "/orders", nil)
	r.Header.Set("Authorization", "Bearer not.a.token")
	_, d, _ := web.Authenticate(r, verifier)
	fmt.Println(web.BearerChallenge("orders", d))
	// Output: Bearer realm="orders", error="invalid_token"
}

// A verifier is built once, from a secret of at least 32 bytes; a shorter
// one is refused at construction rather than at the first request.
func ExampleNewHMACVerifier() {
	_, err := web.NewHMACVerifier(web.HMACConfig{Secret: []byte("short")})
	fmt.Println(err != nil)
	v, err := web.NewHMACVerifier(web.HMACConfig{
		Secret:      []byte("a 32-byte secret drawn from crypto/rand!"),
		TenantClaim: "tenant",
	})
	fmt.Println(v != nil, err)
	// Output:
	// true
	// true <nil>
}
