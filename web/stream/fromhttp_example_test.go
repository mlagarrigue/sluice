package stream_test

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/net/quic"
	"github.com/mlagarrigue/sluice/web"
	"github.com/mlagarrigue/sluice/web/stream"
)

// The ServeMux a program answers net/http with, as a batch handler of the
// native transport: each request at its position, with the answer the mux
// gives it.
func ExampleFromHTTP() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, _, ok := web.PathInt64(r, "id")
		if !ok {
			_ = web.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "bad key"})
			return
		}
		_ = web.WriteJSON(w, http.StatusOK, map[string]int64{"id": id})
	})
	handle := stream.FromHTTP(mux)

	out := handle(context.Background(), sluice.Batch[httpstream.Request]{Items: []httpstream.Request{
		{Method: []byte("GET"), Target: []byte("/orders/7")},
		{Method: []byte("GET"), Target: []byte("/orders/x")},
	}})
	for _, r := range out.Items {
		fmt.Printf("%d %s", r.Status, r.Body)
	}
	// Output:
	// 200 {"id":7}
	// 400 {"error":"bad key"}
}

// HTTP/3 for the http.Server a program already has, until an interrupt,
// then the drain; a gateway passed after the certificate files would be
// closed once the drain is done.
func ExampleServeH3() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		_ = web.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	srv := &http.Server{Addr: ":8443", Handler: mux, ReadHeaderTimeout: web.DefaultReadHeaderTimeout}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	err := stream.ServeH3(ctx, srv, 10*time.Second, "cert.pem", "key.pem")
	stop()
	if err != nil {
		log.Fatal(err)
	}
}

// One call serves the same routes over HTTP/1.1 and HTTP/2 in TCP and over
// HTTP/3 in UDP, on one address; the TCP answers announce HTTP/3 by
// Alt-Svc, and one Ctrl-C drains everything.
func ExampleServeTLSAndH3() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		_ = web.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	srv := &http.Server{Addr: ":8443", Handler: mux, ReadHeaderTimeout: web.DefaultReadHeaderTimeout}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	err := stream.ServeTLSAndH3(ctx, srv, 10*time.Second, "cert.pem", "key.pem")
	stop()
	if err != nil {
		log.Fatal(err)
	}
}

// On a public UDP socket the QUIC listener's bounds are set through H3:
// every connection and every address held to a share.
func ExampleH3() {
	srv := &http.Server{Addr: ":8443", Handler: http.NotFoundHandler(), ReadHeaderTimeout: web.DefaultReadHeaderTimeout}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	h3 := stream.H3{Listener: quic.ListenerConfig{MaxConns: 10_000, MaxConnsPerIP: 64}}
	err := h3.ServeTLSAndH3(ctx, srv, 10*time.Second, "cert.pem", "key.pem")
	stop()
	if err != nil {
		log.Fatal(err)
	}
}
