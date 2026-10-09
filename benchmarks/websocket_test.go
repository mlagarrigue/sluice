package benchmarks

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
)

// The same comparison over a real socket, which is the only way fiber can be in
// it at all.
//
// # Why this file exists separately
//
// fiber does not implement [http.Handler]. It runs on `fasthttp`, its own HTTP
// implementation, so there is no `ServeHTTP` to call and the in-process table
// cannot contain it. Measuring it through `app.Test` instead would compare
// fasthttp's test harness against a direct method call — two different things
// wearing the same number.
//
// So everything here is served over a listener and driven by a real client.
// That makes fiber comparable, and it also produces the socket-level figure the
// in-process table says it does not have: a tail that includes the accept loop
// and the kernel rather than only the handler.
//
// # fiber is on its own row and stays there
//
// Its parser is not `net/http`. A row comparing this pipeline to fiber measures
// **fasthttp against net/http** as much as it measures either pipeline, and no
// amount of care in the handler changes that. It is reported, and it is
// reported as a different thing — which is also why it is the row this project
// is most likely to lose.

func listenAndServe(b testing.TB, h http.Handler) string {
	b.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	b.Cleanup(func() { _ = srv.Close() })
	return "http://" + ln.Addr().String()
}

// fiberURL starts a fiber app on its own listener. Written the way fiber's own
// documentation writes it.
func fiberURL(b testing.TB) string {
	b.Helper()
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Patch("/orders/:order/lines/:line", func(c *fiber.Ctx) error {
		order, err := strconv.ParseInt(c.Params("order"), 10, 64)
		if err != nil {
			return c.Status(http.StatusBadRequest).SendString("bad order")
		}
		line, err := strconv.Atoi(c.Params("line"))
		if err != nil {
			return c.Status(http.StatusBadRequest).SendString("bad line")
		}
		var body amendBody
		if err := c.BodyParser(&body); err != nil {
			return c.Status(http.StatusBadRequest).SendString("bad body")
		}
		status, payload := answer(order, line, body.SaleQty)
		return c.Status(status).JSON(payload)
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	go func() { _ = app.Listener(ln) }()
	b.Cleanup(func() { _ = app.Shutdown() })
	return "http://" + ln.Addr().String()
}

// client keeps connections alive, so what is measured is request handling
// rather than connection setup.
func client() *http.Client {
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 128,
			DisableCompression:  true,
		},
	}
}

// fire reports failures with b.Error rather than b.Fatal: the concurrent
// benchmarks call it from worker goroutines, and FailNow must run on the
// goroutine running the benchmark function — called off it, it only exits
// the worker while the benchmark keeps going, which is the silent half-stop
// webdb_test's serveConcurrently avoids the same way.
func fire(b *testing.B, c *http.Client, url string, qty int64) {
	b.Helper()
	body := `{"saleQty":` + strconv.FormatInt(qty, 10) + `}`
	req, err := http.NewRequestWithContext(b.Context(), http.MethodPatch,
		url+"/orders/42/lines/7", strings.NewReader(body))
	if err != nil {
		b.Error(err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.Do(req)
	if err != nil {
		b.Error(err)
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b.Errorf("status %d", resp.StatusCode)
	}
}

func benchOverSocket(b *testing.B, url string) {
	b.Helper()
	c := client()
	// Warm the connection pool, so the first iteration does not pay for a
	// handshake the others do not.
	for range 20 {
		fire(b, c, url, 9)
	}
	b.ReportAllocs()
	for b.Loop() {
		fire(b, c, url, 9)
	}
}

func BenchmarkSocketNetHTTP(b *testing.B) { benchOverSocket(b, listenAndServe(b, netHTTPHandler())) }
func BenchmarkSocketSluice(b *testing.B)  { benchOverSocket(b, listenAndServe(b, sluiceHandler())) }
func BenchmarkSocketGin(b *testing.B)     { benchOverSocket(b, listenAndServe(b, ginHandler())) }
func BenchmarkSocketEcho(b *testing.B)    { benchOverSocket(b, listenAndServe(b, echoHandler())) }
func BenchmarkSocketChi(b *testing.B)     { benchOverSocket(b, listenAndServe(b, chiHandler())) }

// Different protocol parser. Read this row beside the others, not against them.
func BenchmarkSocketFiberFasthttp(b *testing.B) { benchOverSocket(b, fiberURL(b)) }

// Concurrent, which is the shape a server actually sees.
func benchOverSocketConcurrent(b *testing.B, url string, n int) {
	b.Helper()
	c := client()
	for range 20 {
		fire(b, c, url, 9)
	}
	b.ReportAllocs()
	for b.Loop() {
		var wg sync.WaitGroup
		for range n {
			wg.Go(func() {
				fire(b, c, url, 9)
			})
		}
		wg.Wait()
	}
	b.StopTimer()
	b.ReportMetric(float64(n), "req/op")
}

func BenchmarkSocketConcurrentNetHTTP(b *testing.B) {
	benchOverSocketConcurrent(b, listenAndServe(b, netHTTPHandler()), 32)
}

func BenchmarkSocketConcurrentSluice(b *testing.B) {
	benchOverSocketConcurrent(b, listenAndServe(b, sluiceHandler()), 32)
}

func BenchmarkSocketConcurrentFiberFasthttp(b *testing.B) {
	benchOverSocketConcurrent(b, fiberURL(b), 32)
}

// fiber must answer the same thing as the others, or its row means nothing even
// as a labelled one.
func TestFiberAgreesWithTheRest(t *testing.T) {
	url := fiberURL(t)
	c := client()

	for _, qty := range []int64{9, 0} {
		body := `{"saleQty":` + strconv.FormatInt(qty, 10) + `}`
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPatch,
			url+"/orders/42/lines/7", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		var got map[string]any
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatalf("qty %d: fiber's body is not JSON: %v\n%s", qty, err, raw)
		}

		// The in-process contenders, asserted equal to each other already.
		rec := recordOnce(t, netHTTPHandler(), qty)
		var want map[string]any
		if err := json.Unmarshal(rec, &want); err != nil {
			t.Fatal(err)
		}

		wantStatus := http.StatusOK
		if qty == 0 {
			wantStatus = http.StatusUnprocessableEntity
		}
		if resp.StatusCode != wantStatus {
			t.Errorf("qty %d: fiber answered %d, want %d", qty, resp.StatusCode, wantStatus)
		}
		if !sameJSON(got, want) {
			t.Errorf("qty %d: fiber answered %v, the others answered %v", qty, got, want)
		}
	}
}

// recordOnce runs an in-process handler and returns its body.
func recordOnce(t *testing.T, h http.Handler, qty int64) []byte {
	t.Helper()
	w := &captureWriter{}
	r := *request(qty)
	r.Body = &replayBody{data: []byte(`{"saleQty":` + strconv.FormatInt(qty, 10) + `}`)}
	h.ServeHTTP(w, &r)
	return w.body
}

type captureWriter struct {
	header http.Header
	status int
	body   []byte
}

func (w *captureWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header, 4)
	}
	return w.header
}
func (w *captureWriter) WriteHeader(status int) { w.status = status }
func (w *captureWriter) Write(p []byte) (int, error) {
	w.body = append(w.body, p...)
	return len(p), nil
}

// A sanity check on the socket harness itself: it must be measuring the server
// rather than the client. If a request costs more than a millisecond on
// loopback, something in the harness is wrong — a fresh connection per call, a
// Nagle interaction, a timeout.
func TestSocketHarnessIsNotTheBottleneck(t *testing.T) {
	// Gated like the timing assertions in internal/bench: a wall-clock bound
	// on a shared CI runner, or under the race detector's instrumentation,
	// measures the machine rather than the harness.
	if testing.Short() {
		t.Skip("timing-sensitive: skipped under -short")
	}
	if raceEnabled {
		t.Skip("the race detector instruments every memory access: timings measure it, not the code")
	}
	url := listenAndServe(t, netHTTPHandler())
	c := client()

	body := `{"saleQty":9}`
	warm := func() {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodPatch,
			url+"/orders/42/lines/7", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	for range 50 {
		warm()
	}

	const n = 200
	start := time.Now()
	for range n {
		warm()
	}
	per := time.Since(start) / n
	t.Logf("%v per request over loopback", per)
	if per > time.Millisecond {
		t.Errorf("%v per request; the harness is measuring itself, not the server", per)
	}
}
