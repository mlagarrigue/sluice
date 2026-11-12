package web_test

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"time"

	"github.com/mlagarrigue/sluice/web"
)

// A proxy forwards a request it received in TLS early data with
// Early-Data: 1. Reads are accepted, since a replay does them no harm; the
// rest is answered 425 Too Early, and the client sends it again after its
// handshake.
func ExampleEarlyData() {
	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	h := web.EarlyData(func(r *http.Request) bool { return r.Method == http.MethodGet }, ok)

	for _, method := range []string{"GET", "POST"} {
		r := httptest.NewRequest(method, "/orders", nil)
		r.Header.Set("Early-Data", "1")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		fmt.Println(method, w.Code)
	}
	// Output:
	// GET 204
	// POST 425
}

// The body EarlyData answers with, for a handler that refuses early data on
// its own terms: one problem, and nothing about the request.
func ExampleTooEarlyReport() {
	rep := web.TooEarlyReport()
	fmt.Println(len(rep.Problems), rep.Problems[0].Code == web.CodeTooEarly)
	// Output: 1 true
}

// Serve over TLS, which is how the supported path speaks HTTP/2: the same
// timeouts and the same clean stop as Serve. Compiled, not run: it needs a
// certificate and a port.
func ExampleServeTLS() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	context.AfterFunc(ctx, stop) // a second Ctrl-C, during the drain, ends the process
	// ReadHeaderTimeout left at zero would get ServeTLS's ten seconds too.
	srv := &http.Server{Addr: ":8443", Handler: http.NewServeMux(), ReadHeaderTimeout: 10 * time.Second}
	if err := web.ServeTLS(ctx, srv, 10*time.Second, "cert.pem", "key.pem"); err != nil {
		log.Println(err)
	}
}
