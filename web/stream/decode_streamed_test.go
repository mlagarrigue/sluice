package stream_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/httpstream"
	webstream "github.com/mlagarrigue/sluice/web/stream"
)

// A streamed body cannot be fabricated from outside httpstream — only a
// served connection produces one — so these tests serve a handler built the
// way a web/stream deployment builds one: Exchanges, Route, DecodeJSON,
// Render, over the zero Config.
//
// What they hold: the zero Config streams a request body
// ([httpstream.BodyStreamed]), so Request.Body is nil for a body that did not
// ride along with the headers, and DecodeJSON assembles it rather than
// refusing an empty document. Before the fix, a valid request was refused 400
// "RequestBodyMalformed" whenever the body arrived in a read of its own —
// which is every body a client writes separately, and most bodies past a
// packet. Nothing in the repository called this stage, which is why the
// default's inversion did not show.

type patch struct {
	SaleQty int64 `json:"saleQty"`
}

// serveDecode serves one route that decodes a JSON body of at most limit
// bytes and answers what it decoded, under cfg.
func serveDecode(t *testing.T, cfg httpstream.Config, limit int64) net.Addr {
	t.Helper()
	router := webstream.NewRouter(webstream.Route{Method: "PATCH", Pattern: "/orders/{order}"})
	handler := func(_ context.Context, b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
		ex := webstream.Exchanges(nil, b)
		router.Route(ex)
		decoded := make([]patch, len(ex))
		for i := range ex {
			webstream.DecodeJSON(&ex[i], limit, &decoded[i])
		}
		return webstream.Render(ex, func(i int) (int, any) { return 200, decoded[i] })
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &httpstream.Server{Config: cfg, Handler: handler}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			t.Errorf("shutting down: %v", err)
		}
		<-done
	})
	return ln.Addr()
}

// patchInTwoWrites sends the head, waits for the server to read it, then
// sends the body: the body then arrives in a read of its own.
func patchInTwoWrites(t *testing.T, addr net.Addr, body string) *http.Response {
	t.Helper()
	c, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	head := fmt.Sprintf("PATCH /orders/1 HTTP/1.1\r\nHost: %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n",
		addr.String(), len(body))
	if _, err := c.Write([]byte(head)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if _, err := c.Write([]byte(body)); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(c), &http.Request{Method: "PATCH"})
	if err != nil {
		t.Fatalf("reading the response: %v", err)
	}
	return resp
}

func TestDecodeJSONAssemblesAStreamedBody(t *testing.T) {
	addr := serveDecode(t, httpstream.Config{}, 1<<20)
	resp := patchInTwoWrites(t, addr, `{"saleQty": 4}`)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200: %s", resp.StatusCode, readBody(t, resp))
	}
	var got patch
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decoding the answer: %v", err)
	}
	if got.SaleQty != 4 {
		t.Errorf("saleQty = %d, want 4", got.SaleQty)
	}
}

// A body larger than any single read is the case the whole-body assumption
// could never serve, whatever the client's write pattern.
func TestDecodeJSONAssemblesABodyPastOneRead(t *testing.T) {
	addr := serveDecode(t, httpstream.Config{}, 1<<20)
	body := fmt.Sprintf(`{"saleQty": 4, "_pad": %q}`, strings.Repeat("x", 128<<10))
	resp := patchInTwoWrites(t, addr, body)
	// The padding is an unknown field, refused by the stage's own rules —
	// which is the point: the refusal is about the document, not about a
	// body the stage never saw.
	if resp.StatusCode != 400 {
		t.Fatalf("status = %d, want 400: %s", resp.StatusCode, readBody(t, resp))
	}
	p := problem(t, resp)
	if p.Code != "Transport.Request.Malformed" {
		t.Errorf("code = %q, want the malformed-document refusal", p.Code)
	}
	// The named field is what proves the whole 128 KiB was read: a stage
	// that saw nothing cannot quote the padding it refuses.
	if p.Args["field"] != "_pad" {
		t.Errorf("args = %v, want the refused field named", p.Args)
	}
}

// Past the route's own ceiling, a streamed body is 413 like a whole one, not
// 400: the two need different fixes.
func TestDecodeJSONRefusesAStreamedBodyPastTheLimit(t *testing.T) {
	addr := serveDecode(t, httpstream.Config{}, 16)
	resp := patchInTwoWrites(t, addr, `{"saleQty": 400000000000000000}`)
	if resp.StatusCode != 413 {
		t.Fatalf("status = %d, want 413: %s", resp.StatusCode, readBody(t, resp))
	}
	if code := problemCode(t, resp); code != "Transport.Request.TooLarge" {
		t.Errorf("code = %q, want the too-large refusal", code)
	}
}

// Under BodyWhole the stage sees Request.Body, as it always did.
func TestDecodeJSONUnderBodyWhole(t *testing.T) {
	addr := serveDecode(t, httpstream.Config{BodyDelivery: httpstream.BodyWhole}, 1<<20)
	resp := patchInTwoWrites(t, addr, `{"saleQty": 7}`)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200: %s", resp.StatusCode, readBody(t, resp))
	}
	var got patch
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decoding the answer: %v", err)
	}
	if got.SaleQty != 7 {
		t.Errorf("saleQty = %d, want 7", got.SaleQty)
	}
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading the body: %v", err)
	}
	return string(b)
}

// reported is one problem of a refusal's report, as a client reads it.
type reported struct {
	Code string         `json:"code"`
	Args map[string]any `json:"args"`
}

func problem(t *testing.T, resp *http.Response) reported {
	t.Helper()
	var report struct {
		Problems []reported `json:"problems"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&report); err != nil {
		t.Fatalf("the refusal is not a report: %v", err)
	}
	if len(report.Problems) != 1 {
		t.Fatalf("%d problems, want one", len(report.Problems))
	}
	return report.Problems[0]
}

func problemCode(t *testing.T, resp *http.Response) string {
	t.Helper()
	return problem(t, resp).Code
}
