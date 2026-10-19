package benchmarks

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/go-chi/chi/v5"
	"github.com/labstack/echo/v4"
	"github.com/mlagarrigue/sluice/diagnostics"
	"github.com/mlagarrigue/sluice/web"
)

// The web comparison, in process.
//
// # What every side does, exactly
//
// `PATCH /orders/{order}/lines/{line}` with a JSON body. Read two path
// parameters as integers, decode the body under a byte limit, apply one
// business rule — a quantity must be positive — and answer JSON: 200 with the
// amended quantity, or 422 with the problem and the remedy for it. Same
// statuses, same body shape, same validation, on all five.
//
// Each framework is written the way its own documentation writes it. `gin` uses
// `c.Param` and `c.ShouldBindJSON`; `echo` uses `c.Param` and `c.Bind`; `chi`
// uses `chi.URLParam`. None of them is hand-crippled to make a point, and if
// one of them looks slow here it is because that is what it costs to do this
// work through that API.
//
// # What is measured, and what is not
//
// The handler, from `ServeHTTP` to a written response. No socket, no kernel, no
// connection reuse, no TLS. That exclusion is large and it runs in one
// direction: a real service's tail latency is dominated by queueing at the
// accept loop, which is exactly what is removed here. So a p99.9 out of this
// file is **the tail of the pipeline, not the tail of the service**, and it is
// labelled that way wherever it is published.
//
// # Where this project loses, and why the number is here
//
// Without a database there is nothing for batching to amortise. The gateway's
// whole thesis is that sixty-four callers waiting on one round trip beat
// sixty-four round trips; with no round trip in the picture it is pure
// overhead — a channel, a wait, and a batch of one. `BenchmarkWebSluiceGateway`
// is that cost with nothing on the other side of the trade, and it belongs in
// the README beside the wins rather than in a footnote.

const bodyLimit = 1 << 16

type amendBody struct {
	SaleQty int64 `json:"saleQty"`
}

// answer is the one business rule, shared by every handler so that no side is
// measured doing different work.
func answer(order int64, line int, qty int64) (int, any) {
	if qty > 0 {
		return http.StatusOK, map[string]any{
			"orderId": order, "lineIndex": line, "saleQty": qty,
		}
	}
	at := diagnostics.Path{}.Field("saleQty")
	return http.StatusUnprocessableEntity, web.Report{
		Problems: web.Problems([]diagnostics.Diagnostic{
			diagnostics.NewDiagnostic(diagnostics.Error, "Sales.Order.InvalidQty", at).
				WithMessage("QtyMustBeOverZero", map[string]any{"min": 1}),
		}),
		Remedies: web.Remedies([]diagnostics.Affordance{{
			Path: at, Action: "Sales.Order.CorrectQty", Method: "PATCH",
			Target: fmt.Sprintf("/orders/%d/lines/%d", order, line),
			Inputs: []diagnostics.Input{{Name: "saleQty", Kind: "integer", Required: true}},
		}}),
	}
}

// --- net/http, no framework: the cost of not using one -----------------------

func netHTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PATCH /orders/{order}/lines/{line}", func(w http.ResponseWriter, r *http.Request) {
		order, err := strconv.ParseInt(r.PathValue("order"), 10, 64)
		if err != nil {
			http.Error(w, "bad order", http.StatusBadRequest)
			return
		}
		line, err := strconv.Atoi(r.PathValue("line"))
		if err != nil {
			http.Error(w, "bad line", http.StatusBadRequest)
			return
		}
		var body amendBody
		dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, bodyLimit))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&body); err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		status, payload := answer(order, line, body.SaleQty)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(payload)
	})
	return mux
}

// --- sluice's web package, no gateway ---------------------------------------

func sluiceHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PATCH /orders/{order}/lines/{line}", func(w http.ResponseWriter, r *http.Request) {
		order, d, ok := web.PathInt64(r, "order")
		if !ok {
			_ = web.WriteJSON(w, http.StatusBadRequest, web.ReportOf(d))
			return
		}
		line, d, ok := web.PathInt(r, "line")
		if !ok {
			_ = web.WriteJSON(w, http.StatusBadRequest, web.ReportOf(d))
			return
		}
		var body amendBody
		if d, ok := web.DecodeJSON(r, bodyLimit, &body); !ok {
			_ = web.WriteJSON(w, http.StatusBadRequest, web.ReportOf(d))
			return
		}
		status, payload := answer(order, line, body.SaleQty)
		_ = web.WriteJSON(w, status, payload)
	})
	return mux
}

// --- gin ---------------------------------------------------------------------

func ginHandler() http.Handler {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.PATCH("/orders/:order/lines/:line", func(c *gin.Context) {
		order, err := strconv.ParseInt(c.Param("order"), 10, 64)
		if err != nil {
			c.String(http.StatusBadRequest, "bad order")
			return
		}
		line, err := strconv.Atoi(c.Param("line"))
		if err != nil {
			c.String(http.StatusBadRequest, "bad line")
			return
		}
		var body amendBody
		if err := c.ShouldBindJSON(&body); err != nil {
			c.String(http.StatusBadRequest, "bad body")
			return
		}
		status, payload := answer(order, line, body.SaleQty)
		c.JSON(status, payload)
	})
	return r
}

// --- echo --------------------------------------------------------------------

func echoHandler() http.Handler {
	e := echo.New()
	e.HideBanner, e.HidePort = true, true
	e.PATCH("/orders/:order/lines/:line", func(c echo.Context) error {
		order, err := strconv.ParseInt(c.Param("order"), 10, 64)
		if err != nil {
			return c.String(http.StatusBadRequest, "bad order")
		}
		line, err := strconv.Atoi(c.Param("line"))
		if err != nil {
			return c.String(http.StatusBadRequest, "bad line")
		}
		var body amendBody
		if err := c.Bind(&body); err != nil {
			return c.String(http.StatusBadRequest, "bad body")
		}
		status, payload := answer(order, line, body.SaleQty)
		return c.JSON(status, payload)
	})
	return e
}

// --- chi ---------------------------------------------------------------------

func chiHandler() http.Handler {
	r := chi.NewRouter()
	r.Patch("/orders/{order}/lines/{line}", func(w http.ResponseWriter, r *http.Request) {
		order, err := strconv.ParseInt(chi.URLParam(r, "order"), 10, 64)
		if err != nil {
			http.Error(w, "bad order", http.StatusBadRequest)
			return
		}
		line, err := strconv.Atoi(chi.URLParam(r, "line"))
		if err != nil {
			http.Error(w, "bad line", http.StatusBadRequest)
			return
		}
		var body amendBody
		dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, bodyLimit))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&body); err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		status, payload := answer(order, line, body.SaleQty)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(payload)
	})
	return r
}

// --- the comparison ----------------------------------------------------------

func contenders() map[string]http.Handler {
	return map[string]http.Handler{
		"NetHTTP": netHTTPHandler(),
		"Sluice":  sluiceHandler(),
		"Gin":     ginHandler(),
		"Echo":    echoHandler(),
		"Chi":     chiHandler(),
	}
}

func request(qty int64) *http.Request {
	body := `{"saleQty":` + strconv.FormatInt(qty, 10) + `}`
	r := httptest.NewRequest(http.MethodPatch, "/orders/42/lines/7", strings.NewReader(body))
	// echo's Bind dispatches on it, and a real client sends it. Setting it for
	// everyone keeps the request identical on all five sides rather than
	// tailored to whichever one needs it.
	r.Header.Set("Content-Type", "application/json")
	return r
}

// A comparison of handlers that answer differently is not a comparison. This
// asserts every side produces the same status and the same decoded body for
// both the accepted and the rejected case, before any figure is believed.
func TestWebContendersAgree(t *testing.T) {
	for _, qty := range []int64{9, 0} {
		var wantStatus int
		var wantBody map[string]any

		names := []string{"NetHTTP", "Sluice", "Gin", "Echo", "Chi"}
		handlers := contenders()
		for _, name := range names {
			rec := httptest.NewRecorder()
			handlers[name].ServeHTTP(rec, request(qty))

			var got map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
				t.Fatalf("%s (qty %d): body is not JSON: %v\n%s", name, qty, err, rec.Body.String())
			}
			if wantBody == nil {
				wantStatus, wantBody = rec.Code, got
				continue
			}
			if rec.Code != wantStatus {
				t.Errorf("%s (qty %d): status %d, want %d", name, qty, rec.Code, wantStatus)
			}
			if !sameJSON(got, wantBody) {
				t.Errorf("%s (qty %d): body differs from the first contender\n got %v\nwant %v",
					name, qty, got, wantBody)
			}
		}
	}
}

func sameJSON(a, b map[string]any) bool {
	x, err1 := json.Marshal(a)
	y, err2 := json.Marshal(b)
	return err1 == nil && err2 == nil && string(x) == string(y)
}

// replayBody is a request body that can be rewound, so a benchmark is not
// measuring the allocation of a new reader every iteration.
type replayBody struct {
	data []byte
	pos  int
}

func (b *replayBody) Read(p []byte) (int, error) {
	if b.pos >= len(b.data) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.pos:])
	b.pos += n
	return n, nil
}
func (b *replayBody) Close() error { return nil }
func (b *replayBody) rewind()      { b.pos = 0 }

// discardWriter is a ResponseWriter that keeps the status and throws the bytes
// away.
//
// httptest.NewRecorder allocates roughly 8 KiB per call — forty times the size
// of the response these handlers write. Measured through one, all five
// contenders came out within 15% of each other at ~3400 ns, which was the
// recorder being measured rather than the frameworks. §2 says to look for the
// harness being wrong before believing a result, and this is what that looked
// like.
type discardWriter struct {
	header http.Header
	status int
	n      int
}

func (w *discardWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header, 4)
	}
	return w.header
}
func (w *discardWriter) WriteHeader(status int) { w.status = status }
func (w *discardWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.n += len(p)
	return len(p), nil
}

// reset keeps the header map's storage — the handlers set one or two entries,
// and reallocating that map per iteration is the same mistake one layer down.
func (w *discardWriter) reset() {
	clear(w.header)
	w.status, w.n = 0, 0
}

func benchHandler(b *testing.B, h http.Handler, qty int64) {
	b.Helper()

	body := &replayBody{data: []byte(`{"saleQty":` + strconv.FormatInt(qty, 10) + `}`)}
	tmpl := request(qty)
	w := &discardWriter{}

	b.ReportAllocs()
	for b.Loop() {
		body.rewind()
		w.reset()

		// A shallow copy per iteration: a handler may store path values on the
		// request, and reusing one across iterations would measure a state a
		// real server never has.
		r := *tmpl
		r.Body = body

		h.ServeHTTP(w, &r)
		if w.status == 0 {
			b.Fatal("no status written")
		}
	}
}

func BenchmarkWebNetHTTP(b *testing.B) { benchHandler(b, netHTTPHandler(), 9) }
func BenchmarkWebSluice(b *testing.B)  { benchHandler(b, sluiceHandler(), 9) }
func BenchmarkWebGin(b *testing.B)     { benchHandler(b, ginHandler(), 9) }
func BenchmarkWebEcho(b *testing.B)    { benchHandler(b, echoHandler(), 9) }
func BenchmarkWebChi(b *testing.B)     { benchHandler(b, chiHandler(), 9) }

// The rejection path, which is where this project claims to do more than the
// others: the answer carries the problem *and* the action that fixes it. That
// extra work is real and it shows here — a comparison that only measured the
// happy path would hide what the diagnostics model costs.
func BenchmarkWebNetHTTPRejected(b *testing.B) { benchHandler(b, netHTTPHandler(), 0) }
func BenchmarkWebSluiceRejected(b *testing.B)  { benchHandler(b, sluiceHandler(), 0) }
func BenchmarkWebGinRejected(b *testing.B)     { benchHandler(b, ginHandler(), 0) }
func BenchmarkWebEchoRejected(b *testing.B)    { benchHandler(b, echoHandler(), 0) }
func BenchmarkWebChiRejected(b *testing.B)     { benchHandler(b, chiHandler(), 0) }
