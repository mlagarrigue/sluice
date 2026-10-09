package web_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/web"
)

// net/http already survives a panic; what it does not do is answer. A client
// that gets a closed connection cannot tell a bug from a network failure, and
// retries something that will panic again.
func TestRecoverAnswersInsteadOfDroppingTheConnection(t *testing.T) {
	var seen atomic.Value
	h := web.Recover(func(v any) { seen.Store(v) }, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("a nil map somewhere")
	}))

	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("the connection was dropped instead of answered: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q", got)
	}
	if seen.Load() != "a nil map somewhere" {
		t.Errorf("the observer saw %v", seen.Load())
	}

	// And the panic value must not be in the body: it is internal state, and a
	// nil dereference names a field.
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), "nil map") {
		t.Errorf("the panic value reached the client: %s", body)
	}
	var report web.Report
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatalf("the body is not a report: %v", err)
	}
	if len(report.Problems) != 1 || report.Problems[0].Code != web.CodeInternal {
		t.Errorf("report = %+v", report)
	}
}

// A boundary that turns crashes into 500s and says nothing has converted a bug
// into a metric nobody reads.
func TestRecoverRequiresAnObserver(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Recover accepted a nil observer")
		}
	}()
	web.Recover(nil, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
}

// http.ErrAbortHandler is net/http's own "drop this connection in silence".
// Catching it would answer a request the standard library was told to abandon.
func TestRecoverReRaisesErrAbortHandler(t *testing.T) {
	var observed atomic.Bool
	h := web.Recover(func(any) { observed.Store(true) }, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler)
	}))

	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL) //nolint:bodyclose // the point is that there is no response
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("the connection was answered; ErrAbortHandler was swallowed")
	}
	if observed.Load() {
		t.Error("ErrAbortHandler was reported as a bug; it is not one")
	}
}

// Once bytes are on the wire the status is spent. Writing a second header
// would append a body to a 200 and log a superfluous-WriteHeader warning.
// Ending the body normally would be worse: the client would accept the
// truncated document as complete. The connection must be aborted instead.
func TestRecoverDoesNotOverwriteAResponseAlreadyBegun(t *testing.T) {
	var observed atomic.Bool
	h := web.Recover(func(any) { observed.Store(true) }, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"partial":true}`))
		// Put the status and the partial body on the wire, so the client
		// sees a response that has begun rather than a dropped connection.
		_ = http.NewResponseController(w).Flush()
		panic("after the response started")
	}))

	status, body, readErr := getTruncated(t, h)
	if status != http.StatusOK {
		t.Errorf("status = %d, want the 200 the handler already sent", status)
	}
	if readErr == nil {
		t.Errorf("the truncated body %q was delivered as complete; want a read error", body)
	}
	if strings.Contains(body, web.CodeInternal) {
		t.Errorf("a 500 report was appended to a started response: %s", body)
	}
	if !observed.Load() {
		t.Error("the panic was not reported")
	}
}

// An implicit 200 — writing with no explicit status — must count as a started
// response too, and the response it started must be aborted, not finished.
func TestRecoverTreatsAnImplicitWriteAsStarted(t *testing.T) {
	h := web.Recover(func(any) {}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("partial"))
		_ = http.NewResponseController(w).Flush()
		panic("after an implicit 200")
	}))

	status, body, readErr := getTruncated(t, h)
	if status != http.StatusOK {
		t.Errorf("status = %d, want 200", status)
	}
	if readErr == nil {
		t.Errorf("the truncated body %q was delivered as complete; want a read error", body)
	}
	if strings.Contains(body, web.CodeInternal) {
		t.Errorf("a report was appended after an implicit 200: %s", body)
	}
}

// A sentinel raised after the response began cannot be answered either, and
// TryHandler returning normally would let net/http end the body cleanly — a
// truncated response passed off as a complete one.
func TestTryHandlerAbortsAResponseAlreadyBegun(t *testing.T) {
	var observed atomic.Bool
	var answered atomic.Bool
	h := web.TryHandler(
		func(error) { observed.Store(true) },
		func(http.ResponseWriter, *http.Request, error) { answered.Store(true) },
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`[{"id":1},`))
			_ = http.NewResponseController(w).Flush()
			panic(sluice.ErrOverflow)
		}))

	status, body, readErr := getTruncated(t, web.Recover(func(v any) {
		t.Errorf("Recover observed %v: the abort sentinel must pass through it", v)
	}, h))
	if status != http.StatusOK {
		t.Errorf("status = %d, want 200", status)
	}
	if readErr == nil {
		t.Errorf("the truncated body %q was delivered as complete; want a read error", body)
	}
	if answered.Load() {
		t.Error("answer ran after the response had begun")
	}
	if !observed.Load() {
		t.Error("the sentinel was not observed")
	}
}

// getTruncated serves h over a real connection and reports the status, the
// body read so far and the error that ended the read.
func getTruncated(t *testing.T, h http.Handler) (int, string, error) {
	t.Helper()
	srv := httptest.NewUnstartedServer(h)
	// net/http logs the aborted handler; keep the test output quiet.
	srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	srv.Start()
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, readErr := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body), readErr
}

// A handler reaching for Flush through http.NewResponseController must still
// get there: a wrapper that hides the optional interfaces breaks streaming
// silently.
func TestRecoverDoesNotHideTheResponseController(t *testing.T) {
	var flushed atomic.Bool
	h := web.Recover(func(any) {}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		rc := http.NewResponseController(w)
		_, _ = w.Write([]byte("chunk"))
		if err := rc.Flush(); err != nil {
			t.Errorf("Flush through the controller: %v", err)
			return
		}
		flushed.Store(true)
	}))

	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.ReadAll(resp.Body)

	if !flushed.Load() {
		t.Error("the handler could not reach Flush through the wrapper")
	}
}

// A sentinel is the library refusing data under the configured policy; a nil
// dereference is a defect. TryHandler catches the first and lets the second
// keep unwinding, so the two stay distinguishable.
func TestTryHandlerConvertsSentinelsAndNotBugs(t *testing.T) {
	t.Run("a sentinel is answered", func(t *testing.T) {
		// The handlers run on the server's goroutine; everything they record
		// is read back here, so it goes through a lock.
		var mu sync.Mutex
		var answered, observed error
		h := web.TryHandler(
			func(err error) { mu.Lock(); observed = err; mu.Unlock() },
			func(w http.ResponseWriter, _ *http.Request, err error) {
				mu.Lock()
				answered = err
				mu.Unlock()
				_ = web.WriteJSON(w, http.StatusInternalServerError, web.InternalReport())
			},
			http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				panic(sluice.ErrOverflow)
			}))

		srv := httptest.NewServer(h)
		defer srv.Close()

		resp, err := http.Get(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusInternalServerError {
			t.Errorf("status = %d", resp.StatusCode)
		}
		mu.Lock()
		defer mu.Unlock()
		if !errors.Is(answered, sluice.ErrOverflow) {
			t.Errorf("answered with %v, want ErrOverflow", answered)
		}
		if !errors.Is(observed, sluice.ErrOverflow) {
			t.Errorf("observed %v, want every caught sentinel to be observed", observed)
		}
	})

	t.Run("a bug keeps unwinding into Recover", func(t *testing.T) {
		var answeredAsSentinel atomic.Bool
		var observed atomic.Value
		h := web.Recover(
			func(v any) { observed.Store(v) },
			web.TryHandler(
				func(error) { answeredAsSentinel.Store(true) },
				func(http.ResponseWriter, *http.Request, error) { answeredAsSentinel.Store(true) },
				http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
					panic("a real bug")
				})))

		srv := httptest.NewServer(h)
		defer srv.Close()

		resp, err := http.Get(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()

		if answeredAsSentinel.Load() {
			t.Error("a plain panic was reported as a library sentinel")
		}
		if got := observed.Load(); got != "a real bug" {
			t.Errorf("Recover saw %v", got)
		}
		if resp.StatusCode != http.StatusInternalServerError {
			t.Errorf("status = %d", resp.StatusCode)
		}
	})
}

// closerFunc records when it was closed, so ordering can be asserted.
type closerFunc struct {
	mu     sync.Mutex
	closed bool
	at     time.Time
	err    error
}

func (c *closerFunc) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed, c.at = true, time.Now()
	return c.err
}

func (c *closerFunc) state() (bool, time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed, c.at
}

// The invariant this function exists for: a gateway closed before the server
// has drained refuses calls that in-flight requests are about to make. Those
// requests answer 500 during what was supposed to be a clean stop.
func TestShutdownDrainsTheServerBeforeClosingAnything(t *testing.T) {
	gw := &closerFunc{}

	started := make(chan struct{})
	release := make(chan struct{})
	// atomic.Value, not a bare time.Time: a drain that is broken — the very
	// defect under test — would make this read race with the handler's write.
	var handlerFinished atomic.Value
	var callFailed atomic.Bool

	srv := &http.Server{
		ReadHeaderTimeout: time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			close(started)
			<-release
			// The in-flight request uses the gateway *after* Shutdown began.
			if closed, _ := gw.state(); closed {
				callFailed.Store(true)
			}
			handlerFinished.Store(time.Now())
			w.WriteHeader(http.StatusOK)
		}),
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()

	respCh := make(chan *http.Response, 1)
	go func() {
		resp, err := http.Get("http://" + ln.Addr().String())
		if err == nil {
			respCh <- resp
		} else {
			respCh <- nil
		}
	}()
	<-started

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- web.Shutdown(context.Background(), srv, gw) }()

	// Give Shutdown a moment to begin, then let the handler finish.
	time.Sleep(20 * time.Millisecond)
	if closed, _ := gw.state(); closed {
		t.Fatal("the gateway was closed while a request was still running")
	}
	close(release)

	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown returned %v", err)
	}
	resp := <-respCh
	if resp == nil {
		t.Fatal("the in-flight request was not answered")
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("the in-flight request answered %d during a clean stop", resp.StatusCode)
	}
	if callFailed.Load() {
		t.Error("the handler found the gateway already closed")
	}
	closed, closedAt := gw.state()
	if !closed {
		t.Fatal("the gateway was never closed")
	}
	finished, ok := handlerFinished.Load().(time.Time)
	if !ok {
		t.Fatal("the handler never recorded when it finished")
	}
	if closedAt.Before(finished) {
		t.Errorf("the gateway closed at %v, before the handler finished at %v", closedAt, finished)
	}
}

// A drain that times out must still run the closers: a gateway left open
// because one handler hung is a goroutine and a connection leaked for the life
// of the process.
func TestShutdownClosesEvenWhenTheDrainTimesOut(t *testing.T) {
	gw := &closerFunc{}
	release := make(chan struct{})
	defer close(release)

	started := make(chan struct{})
	srv := &http.Server{
		ReadHeaderTimeout: time.Second,
		Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			close(started)
			<-release
		}),
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	go func() {
		resp, err := http.Get("http://" + ln.Addr().String())
		if err == nil {
			_ = resp.Body.Close()
		}
	}()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err = web.Shutdown(ctx, srv, gw)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want it to carry the drain's deadline", err)
	}
	if closed, _ := gw.state(); !closed {
		t.Error("the closers were abandoned because the drain timed out")
	}
}

// Every closer runs, and every failure survives into the result.
func TestShutdownRunsEveryCloserAndJoinsFailures(t *testing.T) {
	boom1, boom2 := errors.New("gateway"), errors.New("connection")
	a := &closerFunc{err: boom1}
	b := &closerFunc{}
	c := &closerFunc{err: boom2}

	srv := &http.Server{ReadHeaderTimeout: time.Second}
	err := web.Shutdown(context.Background(), srv, a, nil, b, c)

	for i, cl := range []*closerFunc{a, b, c} {
		if closed, _ := cl.state(); !closed {
			t.Errorf("closer %d never ran", i)
		}
	}
	if !errors.Is(err, boom1) || !errors.Is(err, boom2) {
		t.Errorf("err = %v, want both failures joined", err)
	}
}

func TestShutdownWithoutAServer(t *testing.T) {
	if err := web.Shutdown(context.Background(), nil); err == nil {
		t.Error("Shutdown accepted a nil server")
	}
	if err := web.Serve(context.Background(), nil, 0); err == nil {
		t.Error("Serve accepted a nil server")
	}
}

// ErrServerClosed is what ListenAndServe returns once Shutdown was called,
// which is the successful path and must not be reported as a failure.
func TestServeStopsCleanlyOnContextCancellation(t *testing.T) {
	gw := &closerFunc{}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // Serve does its own listening; we only wanted a free port

	srv := &http.Server{
		Addr:              addr,
		ReadHeaderTimeout: time.Second,
		Handler:           http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- web.Serve(ctx, srv, time.Second, gw) }()

	// Wait for it to be listening.
	deadline := time.Now().Add(2 * time.Second)
	for {
		resp, err := http.Get("http://" + addr)
		if err == nil {
			_ = resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("the server never came up: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("Serve returned %v, want nil: ErrServerClosed is the ordinary end", err)
	}
	if closed, _ := gw.state(); !closed {
		t.Error("the closers did not run")
	}
}

// A listener that cannot start must report why, and must still run the
// closers: whatever was opened before the listen attempt is open now.
func TestServeReportsAListenFailureAndStillCloses(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	gw := &closerFunc{}
	srv := &http.Server{Addr: ln.Addr().String(), ReadHeaderTimeout: time.Second}

	err = web.Serve(context.Background(), srv, time.Second, gw)
	if err == nil {
		t.Fatal("a port already in use was reported as success")
	}
	if closed, _ := gw.state(); !closed {
		t.Error("the closers did not run after a listen failure")
	}
}

// A flush is the one way to commit a response without passing through
// WriteHeader or Write, and the boundaries decide whether to answer by asking
// whether the response has begun.
//
// The case is narrow and real: the flush has to be the *first* thing the
// handler does, since a WriteHeader or a Write before it already marks the
// response as begun. A handler opening a stream does exactly that — set the
// content type, flush to send the headers, then produce events — and if it
// then fails, the boundary used to believe nothing had been written and
// append its own document to a stream already committed as something else.
//
// The flush put the stream's headers on the wire, so the client sees them,
// then an aborted body: no answer appended, and no clean end either.
func TestTryHandlerDoesNotAnswerAfterABareFlush(t *testing.T) {
	var mu sync.Mutex
	var observed error
	h := web.TryHandler(
		func(err error) { mu.Lock(); observed = err; mu.Unlock() },
		func(w http.ResponseWriter, _ *http.Request, err error) {
			_ = web.WriteJSON(w, http.StatusUnprocessableEntity, web.InternalReport())
		},
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			// No WriteHeader and no Write: this flush is what commits the 200.
			if err := http.NewResponseController(w).Flush(); err != nil {
				t.Errorf("Flush: %v", err)
			}
			panic(sluice.ErrOverflow)
		}))
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	// The stream was committed and then failed: it is aborted, so the read
	// ends in an error rather than a clean end of stream.
	body, err := io.ReadAll(resp.Body)
	if err == nil {
		t.Error("the failed stream ended cleanly; want it aborted")
	}

	if got := resp.Header.Get("Content-Type"); got != "text/event-stream" {
		t.Errorf("Content-Type = %q, want the stream's own: the answer overwrote it", got)
	}
	if len(body) != 0 {
		t.Errorf("a report was appended to a committed event stream: %q", body)
	}
	// The response could not be amended, so the observer is the sentinel's
	// only trace: dropped unobserved, a truncated stream is undiagnosable.
	mu.Lock()
	defer mu.Unlock()
	if !errors.Is(observed, sluice.ErrOverflow) {
		t.Errorf("observed %v, want the sentinel the boundary could not answer", observed)
	}
}

// A flush the writer underneath cannot perform commits nothing, so it must not
// count as a response having begun — otherwise a handler that flushed into a
// middleware without a Flusher and then panicked gets no answer at all, and
// net/http sends an empty 200 for a request that crashed.
func TestRecoverStillAnswersWhenTheFlushWasNotSupported(t *testing.T) {
	h := web.Recover(func(any) {}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Ignored exactly as a streaming handler would: it asked to flush and
		// the stack said no.
		_ = http.NewResponseController(w).Flush()
		panic("nothing was ever written")
	}))

	w := &unflushableWriter{header: http.Header{}}
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))

	if w.status != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500: a flush that committed nothing was taken for a started response", w.status)
	}
	if !strings.Contains(w.body.String(), web.CodeInternal) {
		t.Errorf("the body carries no %s report: %q", web.CodeInternal, w.body.String())
	}
}

// unflushableWriter is a ResponseWriter and nothing else — no Flusher, no
// Unwrap — which is what a middleware wrapper written without care looks like
// to http.NewResponseController.
type unflushableWriter struct {
	header http.Header
	status int
	body   strings.Builder
}

func (w *unflushableWriter) Header() http.Header { return w.header }

func (w *unflushableWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *unflushableWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(b)
}

// selfSigned mints a certificate for 127.0.0.1, so a test can serve TLS
// without a fixture on disk.
func selfSigned(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// The supported path speaks HTTP/2, and this is what says so.
//
// There is nothing in this package to turn on: net/http offers "h2" through
// ALPN as soon as it serves TLS, and everything here works over it unchanged.
// The test exists because "it should work" is not the same as "it does", and
// because the one thing that would break it — a TLSConfig whose NextProtos
// omits h2 — is silent when it happens.
func TestServeTLSNegotiatesHTTP2(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /x", func(w http.ResponseWriter, r *http.Request) {
		// Written through this package's own helper, so what is exercised is
		// the supported path rather than a bare handler.
		_ = web.WriteJSON(w, http.StatusOK, web.Report{
			Problems: []web.Problem{{Code: "X.Proto", MessageID: r.Proto}},
		})
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // ServeTLS opens its own; this was only to claim a port

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		TLSConfig:         &tls.Config{Certificates: []tls.Certificate{selfSigned(t)}, MinVersion: tls.VersionTLS12},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- web.ServeTLS(ctx, srv, 5*time.Second, "", "") }()

	transport := &http.Transport{
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}, //nolint:gosec // G402: a self-signed certificate minted by this test
		ForceAttemptHTTP2: true,
	}
	client := &http.Client{Transport: transport}
	// An HTTP/2 connection is long-lived by design, so a drain waits for it:
	// the client releases it before the server is asked to stop, which is
	// what a real deployment does by closing its own clients first.
	defer func() {
		transport.CloseIdleConnections()
		cancel()
		if err := <-done; err != nil {
			t.Errorf("ServeTLS returned %v", err)
		}
	}()
	var resp *http.Response
	for range 50 { // the listener is opened on another goroutine
		resp, err = client.Get("https://" + addr + "/x")
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.Proto != "HTTP/2.0" {
		t.Errorf("the connection spoke %s, want HTTP/2.0", resp.Proto)
	}
	var report struct {
		Problems []struct{ MessageID string } `json:"problems"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&report); err != nil {
		t.Fatalf("decoding the report: %v", err)
	}
	if len(report.Problems) != 1 || report.Problems[0].MessageID != "HTTP/2.0" {
		t.Errorf("the handler saw %+v, want it served over HTTP/2.0", report.Problems)
	}
}

// A server that bounds no read leaves header reception unbounded — the
// slowloris shape. Serve gives it a header deadline; a server that already
// bounds its reads, or opted out with a negative value, is left as it was.
func TestServeBoundsHeaderReadsByDefault(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	for _, tc := range []struct {
		name               string
		srv                *http.Server
		wantHeader, wantRd time.Duration
	}{
		{"unbounded", &http.Server{}, web.DefaultReadHeaderTimeout, 0}, //nolint:gosec // the unbounded server is what is under test
		{"own header bound", &http.Server{ReadHeaderTimeout: time.Second}, time.Second, 0},
		{"read bound only", &http.Server{ReadTimeout: 3 * time.Second}, 0, 3 * time.Second},
		{"explicit opt-out", &http.Server{ReadHeaderTimeout: -1}, -1, 0},
	} {
		// The port is taken, so the listen fails at once: what is under test
		// is what Serve did to the server before it tried.
		tc.srv.Addr = ln.Addr().String()
		if err := web.Serve(context.Background(), tc.srv, time.Second); err == nil {
			t.Fatalf("%s: listening on a taken port succeeded", tc.name)
		}
		if tc.srv.ReadHeaderTimeout != tc.wantHeader || tc.srv.ReadTimeout != tc.wantRd {
			t.Errorf("%s: ReadHeaderTimeout=%v ReadTimeout=%v, want %v and %v",
				tc.name, tc.srv.ReadHeaderTimeout, tc.srv.ReadTimeout, tc.wantHeader, tc.wantRd)
		}
	}
}
