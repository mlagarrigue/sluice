package stream

import (
	"cmp"
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/net/quic"
	"github.com/mlagarrigue/sluice/web"
)

// ServeH3 is [web.ServeTLS] for HTTP/3: srv.Handler, through [FromHTTP],
// over QUIC on srv.Addr in UDP (":https" when empty) until ctx is
// cancelled, then the drain of at most grace and the closers stopped, in
// [web.Shutdown]'s order.
//
// It serves HTTP/3 alone; [ServeTLSAndH3] serves the three protocols from
// one call. Calling ServeH3 beside ServeTLS on the same srv is not the way:
// each call stops the closers when its own drain ends, a gateway among them
// under the other server still using it, and ServeTLS fills the zero
// timeouts of srv while this reads them.
//
// The TLS is srv.TLSConfig's, on a copy whose ALPN names "h3" and whose
// minimum is TLS 1.3, QUIC's one version; certFile and keyFile load the
// certificate as ServeTLS loads it. Session tickets follow
// SessionTicketsDisabled, as they do over TCP, and QUIC sends one ticket per
// connection. The timeouts are srv's with ServeTLS's defaults
// ([web.DefaultReadTimeout], [web.DefaultWriteTimeout],
// [web.DefaultIdleTimeout]) and MaxHeaderBytes when set; a negative
// timeout, net/http's "no bound", takes the native transport's default,
// since every bound there is mandatory. The rest of [httpstream.Config] is
// its default, and the transport logs to [slog.Default].
//
// grace is ServeTLS's: zero means [web.DefaultShutdownGrace], a negative
// value no bound.
//
// Experimental: it runs on net/quic, which is experimental, and stays outside
// the v1 promise the rest of the package makes (ADR 0020, ADR 0021): it may
// change with net/quic, announced with a `!`.
func ServeH3(ctx context.Context, srv *http.Server, grace time.Duration, certFile, keyFile string, closers ...web.Drainer) error {
	return H3{}.ServeH3(ctx, srv, grace, certFile, keyFile, closers...)
}

// H3 is [ServeH3] and [ServeTLSAndH3] with the transport's settings within
// reach: the QUIC listener's — MaxConns and MaxConnsPerIP, which are no
// bound at zero and which a public UDP socket wants set, the keys, the
// pending table — and the HTTP/3 server's. Its zero value is the package's
// ServeH3 and ServeTLSAndH3.
//
//	err := stream.H3{Listener: quic.ListenerConfig{MaxConns: 10_000, MaxConnsPerIP: 64}}.
//	    ServeTLSAndH3(ctx, srv, 10*time.Second, "", "", gw)
//
// srv's timeouts and MaxHeaderBytes fill the Transport fields left at zero,
// as they fill the defaults under ServeH3; the session tickets follow
// srv.TLSConfig, as over TCP, whatever Listener.SessionTickets says.
//
// Experimental: it runs on net/quic, as ServeH3 does, and stays outside the
// v1 promise the rest of the package makes (ADR 0020, ADR 0021).
type H3 struct {
	Listener  quic.ListenerConfig
	Transport httpstream.Config
}

// ServeH3 is the package's [ServeH3] with h's settings.
func (h H3) ServeH3(ctx context.Context, srv *http.Server, grace time.Duration, certFile, keyFile string, closers ...web.Drainer) error {
	if srv == nil {
		return errors.New("stream: ServeH3 requires a server")
	}
	h3, ln, err := h.listen(srv, certFile, keyFile)
	if err != nil {
		return failedBefore(err, grace, h3, closers)
	}
	served := make(chan error, 1)
	go func() { served <- h3.ServeQUIC(ln) }()
	select {
	case err := <-served:
		_ = ln.Close() // left open when ServeQUIC refused before reading it
		return failedBefore(err, grace, h3, closers)
	case <-ctx.Done():
	}
	dctx, cancel := drainContext(grace)
	defer cancel()
	return errors.Join(web.Shutdown(dctx, h3, closers...), <-served)
}

// ServeTLSAndH3 serves srv over HTTP/1.1, HTTP/2 and HTTP/3 from one call:
// [web.ServeTLS] on srv.Addr in TCP and [ServeH3] on the same address in
// UDP, until ctx is cancelled, then both drained together within grace and
// the closers stopped once, after the last of them, in [web.Shutdown]'s
// order.
//
// Every response over TCP announces HTTP/3 on the UDP port, Alt-Svc
// h3=":port" (RFC 7838 §3, RFC 9114 §3.1.1), unless the handler set an
// Alt-Svc of its own: a browser then moves to HTTP/3 for the next requests.
// Over HTTP/2, RFC 7838 §3 asks for an ALTSVC frame rather than the field,
// which net/http does not send; the field MAY occur in any response, and
// clients read it there. To do so srv.Handler is wrapped, as ServeTLS
// already fills srv's zero timeouts: srv is this call's once it starts.
//
// The timeouts are read for HTTP/3 before ServeTLS fills them, so the two
// listeners apply the same defaults without one reading what the other
// writes. A listener that fails — a port taken, a certificate refused —
// stops the other, and the closers still run. grace, certFile and keyFile
// are ServeTLS's and ServeH3's.
//
// Experimental: it runs on net/quic, as ServeH3 does, and stays outside the
// v1 promise the rest of the package makes (ADR 0020, ADR 0021).
func ServeTLSAndH3(ctx context.Context, srv *http.Server, grace time.Duration, certFile, keyFile string, closers ...web.Drainer) error {
	return H3{}.ServeTLSAndH3(ctx, srv, grace, certFile, keyFile, closers...)
}

// ServeTLSAndH3 is the package's [ServeTLSAndH3] with h's settings for
// HTTP/3; the TCP side is [web.ServeTLS]'s, as it is there.
func (h H3) ServeTLSAndH3(ctx context.Context, srv *http.Server, grace time.Duration, certFile, keyFile string, closers ...web.Drainer) error {
	if srv == nil {
		return errors.New("stream: ServeTLSAndH3 requires a server")
	}
	h3, ln, err := h.listen(srv, certFile, keyFile)
	if err != nil {
		return failedBefore(err, grace, h3, closers)
	}
	srv.Handler = altSvc(srv.Handler, ln.Addr())

	tcpCtx, stopTCP := context.WithCancel(ctx)
	defer stopTCP()
	tcp := make(chan error, 1)
	go func() { tcp <- web.ServeTLS(tcpCtx, srv, grace, certFile, keyFile) }()
	served := make(chan error, 1)
	go func() { served <- h3.ServeQUIC(ln) }()

	var tcpErr, h3Err error
	tcpDone, h3Done := false, false
	select {
	case tcpErr = <-tcp:
		tcpDone = true
	case h3Err = <-served:
		h3Done = true
		_ = ln.Close() // left open when ServeQUIC refused before reading it
	case <-ctx.Done():
	}
	// One drain for both, from here: ServeTLS drains srv once its context
	// ends, bounded by the same grace, while HTTP/3 drains beside it; the
	// closers wait for both.
	stopTCP()
	dctx, cancel := drainContext(grace)
	defer cancel()
	errs := []error{tcpErr, h3Err, web.Shutdown(dctx, h3)}
	if !h3Done {
		errs = append(errs, <-served)
	}
	if !tcpDone {
		errs = append(errs, <-tcp)
	}
	return errors.Join(append(errs, web.Shutdown(dctx, web.Servers(), closers...))...)
}

// altSvc adds the Alt-Svc that announces HTTP/3 on addr's port to every
// response h writes, unless h sets its own.
func altSvc(h http.Handler, addr net.Addr) http.Handler {
	if h == nil {
		h = http.DefaultServeMux
	}
	_, port, _ := net.SplitHostPort(addr.String())
	value := `h3=":` + port + `"`
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if w.Header().Get("Alt-Svc") == "" {
			w.Header().Set("Alt-Svc", value)
		}
		h.ServeHTTP(w, r)
	})
}

// listen builds the HTTP/3 server for srv and binds its QUIC listener on
// srv.Addr in UDP, with h's settings. The server is returned even when the
// listener is not, for the drain of a call that failed before serving.
func (h H3) listen(srv *http.Server, certFile, keyFile string) (*httpstream.Server, *quic.Listener, error) {
	cfg := h3Config(srv, h.Transport)
	h3 := &httpstream.Server{Config: cfg, Handler: FromHTTP(srv.Handler)}
	tlsCfg, err := h3TLS(srv.TLSConfig, certFile, keyFile)
	if err != nil {
		return h3, nil, err
	}
	params, err := cfg.QUICConfig()
	if err != nil {
		return h3, nil, err
	}
	pc, err := net.ListenPacket("udp", cmp.Or(srv.Addr, ":https"))
	if err != nil {
		return h3, nil, err
	}
	lcfg := h.Listener
	lcfg.SessionTickets = !tlsCfg.SessionTicketsDisabled
	ln, err := quic.NewListener(pc, tlsCfg, params, lcfg)
	if err != nil {
		_ = pc.Close()
		return h3, nil, err
	}
	return h3, ln, nil
}

// drainContext is grace as ServeTLS reads it: zero is
// [web.DefaultShutdownGrace], a negative value no bound.
func drainContext(grace time.Duration) (context.Context, context.CancelFunc) {
	switch {
	case grace == 0:
		return context.WithTimeout(context.Background(), web.DefaultShutdownGrace)
	case grace > 0:
		return context.WithTimeout(context.Background(), grace)
	}
	return context.Background(), func() {}
}

// failedBefore answers a call whose listener failed before anything was
// served: the closers still run, since whatever was opened before is open
// now.
func failedBefore(err error, grace time.Duration, h3 *httpstream.Server, closers []web.Drainer) error {
	dctx, cancel := drainContext(grace)
	defer cancel()
	return errors.Join(err, web.Shutdown(dctx, h3, closers...))
}

// h3TLS is srv's TLS for QUIC: a copy, ALPN "h3", and the certificate
// loaded as [http.Server.ServeTLS] loads it.
func h3TLS(base *tls.Config, certFile, keyFile string) (*tls.Config, error) {
	cfg := &tls.Config{}
	if base != nil {
		cfg = base.Clone()
	}
	cfg.NextProtos = []string{"h3"}
	cfg.MinVersion = max(cfg.MinVersion, tls.VersionTLS13) // QUIC's one version (RFC 9001 §4.2)
	hasCert := len(cfg.Certificates) > 0 || cfg.GetCertificate != nil || cfg.GetConfigForClient != nil
	if !hasCert || certFile != "" || keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// h3Config is srv's bounds in the native transport's terms, with
// ServeTLS's defaults, over base: a field base sets is kept.
func h3Config(srv *http.Server, base httpstream.Config) httpstream.Config {
	read, write, idle := srv.ReadTimeout, srv.WriteTimeout, srv.IdleTimeout
	if idle == 0 && read <= 0 {
		idle = web.DefaultIdleTimeout
	}
	if read == 0 {
		read = web.DefaultReadTimeout
	}
	if write == 0 {
		write = web.DefaultWriteTimeout
	}
	cfg := base
	if read > 0 && cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = read
	}
	if write > 0 && cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = write
	}
	if idle > 0 && cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = idle
	}
	if srv.MaxHeaderBytes > 0 && cfg.MaxHeaderBytes == 0 {
		cfg.MaxHeaderBytes = srv.MaxHeaderBytes
	}
	return cfg
}
