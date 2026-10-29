package interop

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/web"
	"github.com/mlagarrigue/sluice/web/stream"
)

// BenchmarkWebStream is the measurement web/stream left its experimental
// label on, one of two conditions (docs/guide/httpstream.md): a third-party client
// — net/http over HTTP/1.1 and HTTP/2, quic-go over HTTP/3 — requests one
// order through a handler built from web/stream's stages (routing,
// authentication, authorization, a path parameter, a JSON render), and
// through a bare httpstream handler that answers the same bytes by hand.
// The gap between the two is what the stages cost on a real round trip;
// everything else — TLS, framing, the client — is common to both.
//
//	cd interop && go test -run '^$' -bench 'WebStream' -count 6
//
// The verifier compares the token with a constant: the stages are measured,
// not a signature check, which a real verifier adds to both sides alike.
func BenchmarkWebStream(b *testing.B) {
	handlers := []struct {
		name   string
		handle httpstream.Handler
	}{{"bare", bareOrderHandle}, {"stages", stagesOrderHandle}}
	header := http.Header{"Authorization": {"Bearer " + orderToken}}
	for _, p := range []struct {
		name   string
		proto  int
		client func(testing.TB) *http.Client
		start  func(testing.TB, httpstream.Handler) string
	}{
		{"h1", 1, h1Client, func(tb testing.TB, h httpstream.Handler) string {
			tb.Helper()
			return startTLSServer(tb, "http/1.1", h)
		}},
		{"h2", 2, h2Client, func(tb testing.TB, h httpstream.Handler) string {
			tb.Helper()
			return startTLSServer(tb, "h2", h)
		}},
		{"h3", 3, h3Client, func(tb testing.TB, h httpstream.Handler) string {
			tb.Helper()
			base, _, _ := startH3Server(tb, h)
			return base
		}},
	} {
		// Bare and stages run back to back on each protocol, so a machine
		// drifting during the run moves both figures of a pair together.
		for _, h := range handlers {
			b.Run(p.name+"/"+h.name, func(b *testing.B) {
				url := p.start(b, h.handle) + orderPath
				client := p.client(b)
				checkOrder(b, client, url, header, p.proto)
				benchGet(b, client, url, header, p.proto, len(orderBody))
			})
		}
	}
}

// The one request both handlers answer, and the answer they both give.
const (
	orderPath  = "/orders/42"
	orderToken = "bench-token"
	orderBody  = `{"id":42,"tenant":"t1"}` + "\n"
)

// checkOrder fetches the order once and compares status, content type and
// body byte for byte: benchGet checks only the length, and a benchmark whose
// two handlers answer differently compares nothing.
func checkOrder(tb testing.TB, client *http.Client, url string, header http.Header, proto int) {
	tb.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		tb.Fatal(err)
	}
	req.Header = header
	resp, err := client.Do(req)
	if err != nil {
		tb.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		tb.Fatalf("GET %s: reading the body: %v", url, err)
	}
	if resp.StatusCode != http.StatusOK || resp.ProtoMajor != proto ||
		resp.Header.Get("Content-Type") != "application/json" || string(body) != orderBody {
		tb.Fatalf("GET %s: %s over %s, Content-Type %q, body %q, want 200 over HTTP/%d, application/json, %q",
			url, resp.Status, resp.Proto, resp.Header.Get("Content-Type"), body, proto, orderBody)
	}
}

var orderRouter = stream.NewRouter(stream.Route{Method: "GET", Pattern: "/orders/{order}"})

type order struct {
	ID     int    `json:"id"`
	Tenant string `json:"tenant"`
}

// stagesOrderHandle is the handler web/stream's package documentation
// sketches, filled in: every stage runs over the whole batch, a refusal
// stays in place, and Render answers positionally.
func stagesOrderHandle(_ context.Context, b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
	ex := stream.Exchanges(nil, b)
	orderRouter.Route(ex)
	stream.Authenticate(ex, orderVerifier{}, "orders")
	stream.Authorize(ex, orderAuthorizer{})
	ids := make([]int, len(ex))
	for i := range ex {
		if !ex[i].Refused() {
			ids[i], _ = stream.PathInt(&ex[i], 0, "order")
		}
	}
	return stream.Render(ex, func(i int) (int, any) {
		return http.StatusOK, order{ID: ids[i], Tenant: ex[i].Grant.Tenant}
	})
}

var jsonType = []httpstream.Header{{Name: []byte("Content-Type"), Value: []byte("application/json")}}

// bareOrderHandle answers the same request with the same bytes and none of
// the stages: a prefix cut for the route, a byte comparison for the token,
// the body appended by hand.
func bareOrderHandle(_ context.Context, b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
	out := make([]httpstream.Response, len(b.Items))
	for i := range b.Items {
		r := &b.Items[i]
		id, ok := strings.CutPrefix(string(r.Target), "/orders/")
		n, err := strconv.Atoi(id)
		if !ok || err != nil || string(r.Get("authorization")) != "Bearer "+orderToken {
			out[i] = httpstream.Response{Status: http.StatusNotFound}
			continue
		}
		body := strconv.AppendInt([]byte(`{"id":`), int64(n), 10)
		body = append(body, `,"tenant":"t1"}`+"\n"...)
		out[i] = httpstream.Response{Status: http.StatusOK, Headers: jsonType, Body: body}
	}
	return sluice.Batch[httpstream.Response]{Items: out}
}

type orderVerifier struct{}

func (orderVerifier) Verify(token string) (web.Principal, error) {
	if token != orderToken {
		return web.Principal{}, errors.New("unknown token")
	}
	return web.Principal{Subject: "bench", Tenant: "t1"}, nil
}

type orderAuthorizer struct{}

func (orderAuthorizer) Authorize(p web.Principal) (web.Grant, error) {
	return web.Grant{Tenant: p.Tenant}, nil
}

// BenchmarkWebStreamHandler calls both handlers in process on a batch of
// one request, no socket in the way: the round trip of BenchmarkWebStream
// is two orders of magnitude larger than the stages, and its spread hides
// them, so the cost of the stages is read here and the round trip says
// what share of a request it is.
func BenchmarkWebStreamHandler(b *testing.B) {
	batch := sluice.Batch[httpstream.Request]{Items: []httpstream.Request{{
		Method:  []byte("GET"),
		Target:  []byte(orderPath),
		Headers: []httpstream.Header{{Name: []byte("authorization"), Value: []byte("Bearer " + orderToken)}},
	}}}
	for _, h := range []struct {
		name   string
		handle httpstream.Handler
	}{{"bare", bareOrderHandle}, {"stages", stagesOrderHandle}} {
		b.Run(h.name, func(b *testing.B) {
			if got := h.handle(context.Background(), batch).Items[0]; got.Status != http.StatusOK || string(got.Body) != orderBody {
				b.Fatalf("answered %d %q, want 200 %q", got.Status, got.Body, orderBody)
			}
			b.ReportAllocs()
			for b.Loop() {
				h.handle(context.Background(), batch)
			}
		})
	}
}
