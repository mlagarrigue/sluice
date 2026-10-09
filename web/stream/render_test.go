package stream

import (
	"encoding/json"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/diagnostics"
	"github.com/mlagarrigue/sluice/net/httpstream"
)

func TestRenderAnswersPositionally(t *testing.T) {
	ex := []Exchange{
		exchangeFor("GET", "/a"),
		exchangeFor("GET", "/b"),
		exchangeFor("GET", "/c"),
	}
	ex[1].Refuse(404, diagnostics.NewDiagnostic(diagnostics.Error, CodeNoRoute, diagnostics.Path{}).
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

// A 204 or 304 has no content: no body, whatever serve returned, and no
// Content-Type describing one. The exchange's own headers still go out.
func TestRenderSendsNoContentFor204And304(t *testing.T) {
	for _, status := range []int{204, 304} {
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
	Authenticate(acceptToken{}, ex)
	Authorize(grantTenant{}, ex)
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
		e.Refuse(404, diagnostics.NewDiagnostic(diagnostics.Error, CodeNoRoute, diagnostics.Path{}).
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
