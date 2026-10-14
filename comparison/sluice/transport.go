package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/web"
)

const maxBody = 1 << 16

// serve runs the app on ln with the transport the options name, until ctx is
// cancelled. It takes a listener rather than an address so that the test can
// bind port 0 and read the port back.
func serve(ctx context.Context, o options, a *app, ln net.Listener) error {
	if o.tls {
		cfg, err := tlsConfig(o)
		if err != nil {
			return err
		}
		ln = tls.NewListener(ln, cfg)
	}
	if o.httpstream() {
		return httpstream.Serve(ctx, ln, httpstream.Config{
			MaxBodyBytes:        maxBody,
			MaxRequestsPerBatch: o.maxRequestsPerBatch,
			MaxRequestsPerConn:  o.perConn(),
		}, a.handler())
	}
	return serveNetHTTP(ctx, o, a, ln)
}

// serveNetHTTP is R's transport: net/http, srv.Serve on the (TLS) listener,
// web.Shutdown at the end — web.ServeTLS without the Listen it does itself.
func serveNetHTTP(ctx context.Context, o options, a *app, ln net.Listener) error {
	mux := http.NewServeMux()
	mux.Handle("/", a)
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: web.DefaultReadHeaderTimeout}
	if o.transport == transportNetHTTPH1 {
		// An empty map tells net/http not to install its h2 handler.
		srv.TLSNextProto = map[string]func(*http.Server, *tls.Conn, http.Handler){}
	}
	done := make(chan struct{})
	var err error
	go func() {
		defer close(done)
		err = srv.Serve(ln)
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := web.Shutdown(shutdownCtx, srv); err != nil {
			return err
		}
		<-done
		return nil
	case <-done:
		return err
	}
}

// ServeHTTP is the net/http adapter: one request, one answer, written by
// web.WriteJSON as the supported path does.
func (a *app) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body []byte
	if r.Body != nil {
		b, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
		if err != nil || len(b) > maxBody {
			_ = web.WriteJSON(w, http.StatusBadRequest, errDoc{Error: "bad body"})
			return
		}
		body = b
	}
	ans := a.route(r.Context(), r.Method, r.URL.RequestURI(), body)
	if !a.codec.accepts(r.Header.Get("Accept-Encoding")) {
		_ = web.WriteJSON(w, ans.status, ans.body)
		return
	}
	// Axis F: the same document, encoded. Vary is set whatever the outcome
	// of the negotiation, so a cache keys on the header that decided it.
	b, err := json.Marshal(ans.body)
	if err != nil {
		_ = web.WriteJSON(w, http.StatusInternalServerError, errDoc{Error: "encode"})
		return
	}
	enc, err := a.codec.encode(b)
	if err != nil {
		_ = web.WriteJSON(w, http.StatusInternalServerError, errDoc{Error: "compress"})
		return
	}
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Content-Encoding", a.codec.token)
	h.Set("Vary", "Accept-Encoding")
	w.WriteHeader(ans.status)
	_, _ = w.Write(enc)
}

var (
	jsonHeader = []httpstream.Header{{Name: []byte("content-type"), Value: []byte("application/json")}}
	varyHeader = httpstream.Header{Name: []byte("vary"), Value: []byte("Accept-Encoding")}
)

// handler is the httpstream adapter: a batch of requests, a batch of answers.
// The unit reads of one batch are submitted to the gateway together
// (DoBatch), which is what the transport's batch is for; the other requests
// are answered one by one on this goroutine.
func (a *app) handler() httpstream.Handler {
	return func(batch sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
		out := make([]httpstream.Response, batch.Len())
		var unitIdx []int
		var unitKeys []int64
		for i, req := range batch.Items {
			if key, ok := unitTarget(req); ok {
				unitIdx = append(unitIdx, i)
				unitKeys = append(unitKeys, key)
				continue
			}
			ans := a.route(context.Background(), string(req.Method), string(req.Target), req.Body)
			out[i] = a.encode(ans, req)
		}
		if len(unitIdx) > 0 {
			for j, oc := range a.gw.DoBatch(context.Background(), unitKeys) {
				out[unitIdx[j]] = a.encode(unitAnswer(unitKeys[j], oc.Out, oc.Err), batch.Items[unitIdx[j]])
			}
		}
		return sluice.Batch[httpstream.Response]{Items: out}
	}
}

// unitTarget recognises GET /orders/{id} with a well-formed key.
func unitTarget(req httpstream.Request) (int64, bool) {
	const prefix = "/orders/"
	if string(req.Method) != "GET" || len(req.Target) <= len(prefix) || string(req.Target[:len(prefix)]) != prefix {
		return 0, false
	}
	var key int64
	for _, c := range req.Target[len(prefix):] {
		if c < '0' || c > '9' || key > (1<<62)/10 {
			return 0, false
		}
		key = key*10 + int64(c-'0')
	}
	return key, key > 0
}

// encode marshals the answer and, under axis F, compresses it when the
// request accepts the codec. The headers of a compressed answer are built
// per response: the slice is read during the write and not retained.
func (a *app) encode(ans answer, req httpstream.Request) httpstream.Response {
	b, err := json.Marshal(ans.body)
	if err != nil {
		return httpstream.Response{Status: 500, Headers: jsonHeader, Body: []byte(`{"error":"encode"}`)}
	}
	if !a.codec.accepts(string(req.Get("accept-encoding"))) {
		return httpstream.Response{Status: ans.status, Headers: jsonHeader, Body: b}
	}
	enc, err := a.codec.encode(b)
	if err != nil {
		return httpstream.Response{Status: 500, Headers: jsonHeader, Body: []byte(`{"error":"compress"}`)}
	}
	headers := []httpstream.Header{
		jsonHeader[0],
		{Name: []byte("content-encoding"), Value: []byte(a.codec.token)},
		varyHeader,
	}
	return httpstream.Response{Status: ans.status, Headers: headers, Body: enc}
}

// tlsConfig is the certificate the options name, or a self-signed ECDSA P-256
// one for a local run (§4 fixes P-256 for the campaign). ALPN narrows the
// protocol to the one the transport flag names.
func tlsConfig(o options) (*tls.Config, error) {
	var cert tls.Certificate
	var err error
	if o.cert != "" {
		cert, err = tls.LoadX509KeyPair(o.cert, o.key)
	} else {
		cert, err = selfSigned()
	}
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}
	switch o.transport {
	case transportNetHTTPH1, transportHTTPStreamH1:
		cfg.NextProtos = []string{"http/1.1"}
	case transportNetHTTPH2, transportHTTPStreamH2:
		cfg.NextProtos = []string{"h2"}
	default:
		return nil, errors.New("tlsConfig: transport without a TLS protocol")
	}
	return cfg, nil
}

func selfSigned() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "sluice-study"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, nil
}
