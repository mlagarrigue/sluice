package stream

import (
	"encoding/json"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/diagnostics"
	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/web"
)

func TestRenderAnswersPositionally(t *testing.T) {
	ex := []Exchange{
		exchangeFor("GET", "/a"),
		exchangeFor("GET", "/b"),
		exchangeFor("GET", "/c"),
	}
	ex[1].Refuse(404, diagnostics.NewDiagnostic(diagnostics.SeverityError, CodeNoRoute, diagnostics.Path{}).
		WithMessage("NoRoute", nil))

	served := 0
	out := Render(ex, func(i int) (int, any) {
		served++
		return 200, map[string]any{"index": i}
	})
	if out.Len() != len(ex) {
		t.Fatalf("%d responses for %d exchanges", out.Len(), len(ex))
	}
	if served != 2 {
		t.Errorf("serve ran %d times; a refused exchange must not reach it", served)
	}

	if out.Items[1].Status != 404 {
		t.Errorf("response 1 status = %d, want the refusal's 404", out.Items[1].Status)
	}
	var report struct {
		Problems []struct{ Code string } `json:"problems"`
	}
	if err := json.Unmarshal(out.Items[1].Body, &report); err != nil {
		t.Fatalf("the refusal's body is not JSON: %v", err)
	}
	if len(report.Problems) != 1 || report.Problems[0].Code != CodeNoRoute {
		t.Errorf("problems = %+v", report.Problems)
	}

	for _, i := range []int{0, 2} {
		var body struct {
			Index int `json:"index"`
		}
		if err := json.Unmarshal(out.Items[i].Body, &body); err != nil {
			t.Fatalf("response %d body: %v", i, err)
		}
		if body.Index != i {
			t.Errorf("response %d carries the answer for %d", i, body.Index)
		}
	}
}

func TestRenderSetsContentTypeAndKeepsExchangeHeaders(t *testing.T) {
	ex := []Exchange{exchangeFor("GET", "/a")}
	ex[0].Headers = append(ex[0].Headers, httpstream.Header{
		Name: []byte("cache-control"), Value: []byte("no-store"),
	})
	out := Render(ex, func(int) (int, any) { return 200, map[string]any{} })

	got := map[string]string{}
	for _, h := range out.Items[0].Headers {
		got[string(h.Name)] = string(h.Value)
	}
	if got["content-type"] != "application/json" {
		t.Errorf("content-type = %q", got["content-type"])
	}
	if got["cache-control"] != "no-store" {
		t.Errorf("the exchange's own header was dropped: %v", got)
	}
}

// The web.WriteJSON bargain, batch-shaped: a body that will not marshal keeps
// its status and loses its body, because a right status with no document
// beats a wrong status with one.
func TestRenderKeepsTheStatusWhenTheBodyWillNotMarshal(t *testing.T) {
	ex := []Exchange{exchangeFor("GET", "/a")}
	out := Render(ex, func(int) (int, any) { return 201, map[string]any{"ch": make(chan int)} })
	if out.Items[0].Status != 201 {
		t.Errorf("status = %d, want the intended 201", out.Items[0].Status)
	}
	if len(out.Items[0].Body) != 0 {
		t.Errorf("body = %q, want empty", out.Items[0].Body)
	}
	if ct := headerOf(out.Items[0], "content-type"); ct != "" {
		t.Errorf("content-type = %q on an empty body, want none", ct)
	}
}

// A 204, 205 or 304 has no content: no body, whatever serve returned, and no
// Content-Type describing one. The exchange's own headers still go out, and
// the response is one the transport writes: a 205 with a body is refused
// there, and became a 500.
func TestRenderSendsNoContentFor204205And304(t *testing.T) {
	for _, status := range []int{204, 205, 304} {
		ex := []Exchange{exchangeFor("GET", "/a")}
		ex[0].Headers = append(ex[0].Headers, httpstream.Header{
			Name: []byte("etag"), Value: []byte(`"v1"`),
		})
		out := Render(ex, func(int) (int, any) { return status, map[string]any{"ignored": true} })
		res := out.Items[0]
		if res.Status != status {
			t.Errorf("status = %d, want %d", res.Status, status)
		}
		if len(res.Body) != 0 {
			t.Errorf("%d: body = %q, want none", status, res.Body)
		}
		if ct := headerOf(res, "content-type"); ct != "" {
			t.Errorf("%d: content-type = %q, want none", status, ct)
		}
		if etag := headerOf(res, "etag"); etag != `"v1"` {
			t.Errorf("%d: etag = %q, the exchange's own header was dropped", status, etag)
		}
		if _, err := httpstream.WriteBatch(nil, httpstream.Config{}, nil, out.Items); err != nil {
			t.Errorf("%d: the transport refuses the rendered response: %v", status, err)
		}
	}
}

func headerOf(res httpstream.Response, name string) string {
	for _, h := range res.Headers {
		if string(h.Name) == name {
			return string(h.Value)
		}
	}
	return ""
}

// The stages composed: one batch through route, authn, authz and render, with
// a healthy element, an unroutable one and a forged token — three answers, in
// their positions.
func TestPipelineEndToEnd(t *testing.T) {
	rt := table(t)

	reqs := sluice.Batch[httpstream.Request]{Items: []httpstream.Request{
		{Method: []byte("GET"), Target: []byte("/health"), Headers: []httpstream.Header{
			{Name: []byte("authorization"), Value: []byte("Bearer good")},
		}},
		{Method: []byte("GET"), Target: []byte("/nothing")},
		{Method: []byte("GET"), Target: []byte("/health"), Headers: []httpstream.Header{
			{Name: []byte("authorization"), Value: []byte("Bearer forged")},
		}},
	}}

	ex := Exchanges(nil, reqs)
	rt.Route(ex)
	Authenticate(ex, acceptToken{}, "orders")
	Authorize(ex, grantTenant{})
	out := Render(ex, func(i int) (int, any) {
		return 200, map[string]any{"ok": true, "tenant": ex[i].Grant.Tenant}
	})

	if out.Len() != 3 {
		t.Fatalf("%d responses for 3 requests", out.Len())
	}
	for i, want := range []int{200, 404, 401} {
		if out.Items[i].Status != want {
			t.Errorf("response %d status = %d, want %d", i, out.Items[i].Status, want)
		}
	}
}

// RenderOpen numbers the exchanges it serves in batch order, skipping the
// refused ones, so a handler that submitted one call per open exchange reads
// its outcome by that rank without keeping a mapping of its own.
func TestRenderOpenRanksTheServedExchanges(t *testing.T) {
	ex := []Exchange{
		exchangeFor("GET", "/a"),
		exchangeFor("GET", "/b"),
		exchangeFor("GET", "/c"),
		exchangeFor("GET", "/d"),
	}
	refuse := func(e *Exchange) {
		e.Refuse(404, diagnostics.NewDiagnostic(diagnostics.SeverityError, CodeNoRoute, diagnostics.Path{}).
			WithMessage("NoRoute", nil))
	}
	refuse(&ex[0])
	refuse(&ex[2])

	outcomes := []string{"for b", "for d"} // one per open exchange, in order
	got := map[int]int{}
	out := RenderOpen(ex, func(i, open int) (int, any) {
		got[i] = open
		return 200, outcomes[open]
	})
	if want := map[int]int{1: 0, 3: 1}; len(got) != len(want) || got[1] != 0 || got[3] != 1 {
		t.Errorf("ranks = %v, want %v", got, want)
	}
	for i, want := range map[int]string{1: `"for b"`, 3: `"for d"`} {
		if body := string(out.Items[i].Body); body != want+"\n" {
			t.Errorf("response %d = %q, want %s", i, body, want)
		}
	}
}

// Render sends WriteJSON's defensive pair: nosniff on every document, no-store
// on an error unless the exchange carries its own cache directive — on a
// refusal and on a served error alike.
func TestRenderSendsTheDefensiveHeaders(t *testing.T) {
	refused := exchangeFor("GET", "/a")
	refused.Refuse(401, diagnostics.NewDiagnostic(diagnostics.SeverityError, "X.Unauthenticated", diagnostics.Path{}))
	cached := exchangeFor("GET", "/b")
	cached.Headers = append(cached.Headers, httpstream.Header{Name: []byte("Cache-Control"), Value: []byte("max-age=60")})
	ex := []Exchange{refused, exchangeFor("GET", "/c"), cached, exchangeFor("GET", "/d"), exchangeFor("GET", "/e")}
	status := []int{0, 200, 404, 500, 400} // 400: the first error status
	out := Render(ex, func(i int) (int, any) { return status[i], map[string]any{} })

	want := []string{"no-store", "", "max-age=60", "no-store", "no-store"}
	for i, r := range out.Items {
		nosniff, cache := 0, []string(nil)
		for _, h := range r.Headers {
			switch {
			case foldEqual(h.Name, "x-content-type-options") && string(h.Value) == "nosniff":
				nosniff++
			case foldEqual(h.Name, "cache-control"):
				cache = append(cache, string(h.Value))
			}
		}
		if nosniff != 1 {
			t.Errorf("response %d (status %d): %d nosniff fields, want 1", i, r.Status, nosniff)
		}
		// One slice sized for them all, so no append regrew it.
		if cap(r.Headers) != len(r.Headers) {
			t.Errorf("response %d (status %d): %d fields in a slice of %d", i, r.Status, len(r.Headers), cap(r.Headers))
		}
		if (want[i] == "" && cache != nil) || (want[i] != "" && (len(cache) != 1 || cache[0] != want[i])) {
			t.Errorf("response %d (status %d): Cache-Control %q, want %q", i, r.Status, cache, want[i])
		}
	}
}

// A Renderer answers a refusal in the caller's document, with the status the
// caller picks, and leaves everything else as Render does: the served
// exchanges, the headers a stage put on the refused one, the defensive pair.
// A refusal the caller does not map keeps the package's document through
// Exchange.Report.
func TestRendererAnswersARefusalInTheCallersDocument(t *testing.T) {
	rt := NewRouter(Route{Method: "GET", Pattern: "/orders/{id}"})
	ex := []Exchange{
		exchangeFor("GET", "/orders/7"),
		exchangeFor("GET", "/orders/seven"),
		exchangeFor("DELETE", "/orders/7"),
	}
	rt.Route(ex)
	ids := make([]int64, len(ex))
	for i := range ex {
		ids[i], _ = PathInt64(&ex[i], 0, "id")
	}

	type apiError struct {
		Code  string `json:"code"`
		Param string `json:"param"`
	}
	var refused []int
	render := Renderer{Refused: func(e *Exchange) (int, any) {
		refused = append(refused, e.RouteIndex)
		if e.RouteIndex == RouteNone {
			return e.Status, e.Report()
		}
		return 422, apiError{Code: e.Diagnostics[0].Code, Param: e.Diagnostics[0].Path.String()}
	}}
	served := 0
	out := render.Render(ex, func(i int) (int, any) {
		served++
		return 200, map[string]int64{"id": ids[i]}
	})

	if served != 1 || len(refused) != 2 || refused[0] != 0 || refused[1] != RouteNone {
		t.Fatalf("serve ran %d times, Refused saw routes %v; want 1 and [0 %d]", served, refused, RouteNone)
	}
	if body := string(out.Items[0].Body); out.Items[0].Status != 200 || body != `{"id":7}`+"\n" {
		t.Errorf("served exchange = %d %q", out.Items[0].Status, body)
	}

	if out.Items[1].Status != 422 {
		t.Errorf("parameter refusal status = %d, want the caller's 422", out.Items[1].Status)
	}
	var doc apiError
	if err := json.Unmarshal(out.Items[1].Body, &doc); err != nil {
		t.Fatalf("parameter refusal body %q: %v", out.Items[1].Body, err)
	}
	if want := (apiError{Code: web.CodeInvalidParam, Param: "id"}); doc != want {
		t.Errorf("parameter refusal = %+v, want %+v", doc, want)
	}
	if got := headerOf(out.Items[1], "cache-control"); got != "no-store" {
		t.Errorf("parameter refusal Cache-Control = %q, want no-store", got)
	}

	if out.Items[2].Status != 405 || headerOf(out.Items[2], "allow") == "" {
		t.Errorf("router refusal = %d with Allow %q, want 405 with the router's Allow",
			out.Items[2].Status, headerOf(out.Items[2], "allow"))
	}
	var report struct {
		Problems []struct{ Code string } `json:"problems"`
	}
	if err := json.Unmarshal(out.Items[2].Body, &report); err != nil || len(report.Problems) != 1 {
		t.Fatalf("router refusal body %q (%v), want the package's report", out.Items[2].Body, err)
	}
}

// The zero Renderer is the package's Render: the same responses, byte for
// byte, refusals included.
func TestTheZeroRendererIsRender(t *testing.T) {
	batch := func() []Exchange {
		ex := []Exchange{exchangeFor("GET", "/a"), exchangeFor("GET", "/b")}
		ex[1].Refuse(404, diagnostics.NewDiagnostic(diagnostics.SeverityError, CodeNoRoute, diagnostics.Path{}).
			WithMessage("NoRoute", nil))
		return ex
	}
	serve := func(int) (int, any) { return 200, "ok" }
	want, got := Render(batch(), serve), Renderer{}.Render(batch(), serve)
	for i := range want.Items {
		w, g := want.Items[i], got.Items[i]
		if w.Status != g.Status || string(w.Body) != string(g.Body) || len(w.Headers) != len(g.Headers) {
			t.Errorf("response %d: Renderer{} = %d %q, Render = %d %q", i, g.Status, g.Body, w.Status, w.Body)
		}
	}
}
