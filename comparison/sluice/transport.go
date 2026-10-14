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
	"strconv"
	"sync"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/net/quic"
	"github.com/mlagarrigue/sluice/web"
)

const maxBody = 1 << 16

// serve runs the app on ln with the transport the options name, until ctx is
// cancelled. It takes a listener rather than an address so that the test can
// bind port 0 and read the port back. httpstream-h3 runs on a packet socket,
// not a stream listener: see serveH3.
func serve(ctx context.Context, o options, a *app, ln net.Listener) error {
	if o.tls {
		cfg, err := tlsConfig(o)
		if err != nil {
			return err
		}
		ln = tls.NewListener(ln, cfg)
	}
	if o.httpstream() {
		return httpstream.Serve(ctx, ln, httpstreamConfig(o), a.handler())
	}
	return serveNetHTTP(ctx, o, a, ln)
}

// httpstreamConfig is axis D's budget, the same under every httpstream
// transport.
func httpstreamConfig(o options) httpstream.Config {
	return httpstream.Config{
		MaxBodyBytes:        maxBody,
		MaxRequestsPerBatch: o.maxRequestsPerBatch,
		MaxRequestsPerConn:  o.perConn(),
	}
}

// serveH3 is axis A's UDP transport: one quic.Listener on pc, one
// httpstream.ServeH3 per accepted connection, until ctx is cancelled. The
// TLS config is a parameter rather than built here so that the test can hand
// over the certificate its client trusts; run passes tlsConfig(o).
func serveH3(ctx context.Context, o options, a *app, pc net.PacketConn, tlsCfg *tls.Config) error {
	cfg := httpstreamConfig(o)
	ln, err := quic.NewListener(pc, tlsCfg, cfg.TransportParameters(), quic.ListenerConfig{})
	if err != nil {
		return err
	}
	handle := a.handler()
	var wg sync.WaitGroup
	accepted := make(chan error, 1)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				accepted <- err
				return
			}
			wg.Go(func() { _ = httpstream.ServeH3(ctx, c, cfg, handle) })
		}
	}()
	select {
	case <-ctx.Done():
		err = ln.Close()
		<-accepted
		wg.Wait()
		return errors.Join(ctx.Err(), err)
	case err := <-accepted:
		_ = ln.Close()
		wg.Wait()
		return err
	}
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
	accepts := a.codec.accepts(r.Header.Get("Accept-Encoding"))
	if ans.stream != nil {
		a.writeStream(w, ans, accepts)
		return
	}
	b := ans.raw
	switch {
	case b != nil && !accepts:
		writeRaw(w, ans.status, b)
		return
	case b == nil && !accepts:
		_ = web.WriteJSON(w, ans.status, ans.body)
		return
	case b == nil:
		var err error
		if b, err = json.Marshal(ans.body); err != nil {
			_ = web.WriteJSON(w, http.StatusInternalServerError, errDoc{Error: "encode"})
			return
		}
	}
	// Axis F: the same document, encoded. Vary is set whatever the outcome
	// of the negotiation, so a cache keys on the header that decided it.
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

// writeRaw writes an answer already encoded the way web.WriteJSON writes
// a value: Content-Type, Content-Length counting the newline, the newline
// as its own write.
func writeRaw(w http.ResponseWriter, status int, b []byte) {
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Content-Length", strconv.Itoa(len(b)+1))
	w.WriteHeader(status)
	if _, err := w.Write(b); err != nil {
		return
	}
	_, _ = io.WriteString(w, "\n")
}

// writeStream is axis E on net/http: no Content-Length, so net/http frames
// the body as the protocol streams it (chunked under HTTP/1.1, DATA frames
// under h2), and each batch of fragments is flushed to the socket as soon as
// it is written. The document is the one WriteJSON would have written, its
// trailing newline included; under axis F the fragments go through the
// codec, flushed per batch as well, and the newline is dropped as the
// buffered compressed path drops it.
func (a *app) writeStream(w http.ResponseWriter, ans answer, compress bool) {
	h := w.Header()
	h.Set("Content-Type", "application/json")
	var out io.Writer = w
	var zw resetWriter
	if compress {
		h.Set("Content-Encoding", a.codec.token)
		h.Set("Vary", "Accept-Encoding")
		zw = a.codec.get(w)
		defer a.codec.put(zw)
		out = zw
	}
	w.WriteHeader(ans.status)
	flusher, _ := w.(http.Flusher)
	ans.stream(func(batch sluice.Batch[[]byte]) bool {
		for _, frag := range batch.Items {
			if _, err := out.Write(frag); err != nil {
				return false
			}
		}
		if zw != nil {
			if err := zw.Flush(); err != nil {
				return false
			}
		}
		if flusher != nil {
			flusher.Flush()
		}
		return true
	})
	if zw != nil {
		_ = zw.Close()
		return
	}
	_, _ = io.WriteString(w, "\n")
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
	if ans.stream != nil {
		// Axis E: Response.Stream, the transport pulls the fragments and
		// frames them itself; under axis F the codec is flushed per batch.
		if !a.codec.accepts(string(req.Get("accept-encoding"))) {
			return httpstream.Response{Status: ans.status, Headers: jsonHeader, Stream: ans.stream}
		}
		return httpstream.Response{Status: ans.status, Headers: a.encodedHeaders(), Stream: a.codec.encodeStream(ans.stream)}
	}
	b := ans.raw
	if b == nil {
		var err error
		if b, err = json.Marshal(ans.body); err != nil {
			return httpstream.Response{Status: 500, Headers: jsonHeader, Body: []byte(`{"error":"encode"}`)}
		}
	}
	if !a.codec.accepts(string(req.Get("accept-encoding"))) {
		return httpstream.Response{Status: ans.status, Headers: jsonHeader, Body: b}
	}
	enc, err := a.codec.encode(b)
	if err != nil {
		return httpstream.Response{Status: 500, Headers: jsonHeader, Body: []byte(`{"error":"compress"}`)}
	}
	return httpstream.Response{Status: ans.status, Headers: a.encodedHeaders(), Body: enc}
}

// encodedHeaders are the headers of a compressed answer, built per response:
// the slice is read during the write and not retained.
func (a *app) encodedHeaders() []httpstream.Header {
	return []httpstream.Header{
		jsonHeader[0],
		{Name: []byte("content-encoding"), Value: []byte(a.codec.token)},
		varyHeader,
	}
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
	case transportHTTPStreamH3:
		cfg.NextProtos = []string{"h3"}
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
