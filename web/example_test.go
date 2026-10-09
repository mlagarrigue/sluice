package web_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/mlagarrigue/sluice/diagnostics"
	"github.com/mlagarrigue/sluice/web"
)

// The whole request path, in the shape this package is meant to be used: a
// ServeMux route, a bounded decode, one call into whatever batches behind it,
// and a response whose status is chosen on a line the reader can see.
//
// Nothing here is a framework calling the handler. Every decision that matters
// — the byte limit, the status code, the envelope — is a visible argument.
func Example() {
	mux := http.NewServeMux()
	mux.HandleFunc("PATCH /orders/{order}/lines/{line}", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			SaleQty int64 `json:"saleQty"`
		}
		if d, ok := web.DecodeJSON(r, 1<<16, &body); !ok {
			// A transport problem is the caller's to map: the request never
			// reached a business rule, so 400 is the honest answer.
			_ = web.WriteJSON(w, http.StatusBadRequest, web.ReportOf(d))
			return
		}
		order, d, ok := web.PathInt64(r, "order")
		if !ok {
			_ = web.WriteJSON(w, http.StatusBadRequest, web.ReportOf(d))
			return
		}

		// Where a gateway call would go. It answers with the entity and the
		// diagnostics that travelled beside it — a rejection is answered, never
		// dropped.
		diags, remedies := amend(order, body.SaleQty)

		_ = web.WriteJSON(w, web.Status(diags), web.Report{
			Problems: web.Problems(diags),
			Remedies: web.Remedies(remedies),
		})
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPatch, srv.URL+"/orders/42/lines/7",
		strings.NewReader(`{"saleQty":0}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	fmt.Println("status:", resp.StatusCode)
	fmt.Println("content-type:", resp.Header.Get("Content-Type"))

	// Output:
	// status: 422
	// content-type: application/json
}

// amend stands in for the business rule. A quantity of zero is refused, and
// the refusal carries the action that fixes it at the same path — which is
// what makes a 422 actionable rather than merely correct.
func amend(order, qty int64) ([]diagnostics.Diagnostic, []diagnostics.Affordance) {
	at := diagnostics.Path{}.Field("saleQty")
	if qty > 0 {
		return nil, nil
	}
	refused := diagnostics.NewDiagnostic(diagnostics.Error, "Sales.Order.InvalidQty", at).
		WithMessage("QtyMustBeOverZero", map[string]any{"min": 1})
	correct := diagnostics.Affordance{
		Path: at, Action: "Sales.Order.CorrectQty", Method: "PATCH",
		Target: fmt.Sprintf("/orders/%d/lines/{line}", order),
		Inputs: []diagnostics.Input{{Name: "saleQty", Kind: "integer", Required: true}},
	}
	return []diagnostics.Diagnostic{refused}, []diagnostics.Affordance{correct}
}

// A warning is not a failure: the element was processed and kept moving, so
// the status stays 200 and the warning travels in the body.
func ExampleStatus_warning() {
	diags := []diagnostics.Diagnostic{
		diagnostics.NewDiagnostic(diagnostics.Warning, "Sales.Order.UnusualQty", diagnostics.Path{}.Field("saleQty")).
			WithMessage("QtyFarFromHistoricalRange", nil),
	}
	fmt.Println(web.Status(diags))
	// Output: 200
}
