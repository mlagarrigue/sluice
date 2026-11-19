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
// It serves HTTP/3 alone. Calling it beside ServeTLS on the same srv is not
// the way to serve the three protocols together: each call stops the
// closers when its own drain ends, a gateway among them under the other
// server still using it, and ServeTLS fills the zero timeouts of srv while
// this reads them.
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
	if srv == nil {
		return errors.New("stream: ServeH3 requires a server")
	}
	tlsCfg, err := h3TLS(srv.TLSConfig, certFile, keyFile)
	if err != nil {
		return err
	}
	cfg := h3Config(srv)
	params, err := cfg.QUICConfig()
	if err != nil {
		return err
	}
	h3 := &httpstream.Server{Config: cfg, Handler: FromHTTP(srv.Handler)}
	drain := func() (context.Context, context.CancelFunc) {
		switch {
		case grace == 0:
			return context.WithTimeout(context.Background(), web.DefaultShutdownGrace)
		case grace > 0:
			return context.WithTimeout(context.Background(), grace)
		}
		return context.Background(), func() {}
	}
	// The listener failed before anything was served: the closers still
	// run, since whatever was opened before is open now.
	failed := func(err error) error {
		dctx, cancel := drain()
		defer cancel()
		return errors.Join(err, web.Shutdown(dctx, h3, closers...))
	}
	pc, err := net.ListenPacket("udp", cmp.Or(srv.Addr, ":https"))
	if err != nil {
		return failed(err)
	}
	ln, err := quic.NewListener(pc, tlsCfg, params, quic.ListenerConfig{SessionTickets: !tlsCfg.SessionTicketsDisabled})
	if err != nil {
		_ = pc.Close()
		return failed(err)
	}
	served := make(chan error, 1)
	go func() { served <- h3.ServeQUIC(ln) }()
	select {
	case err := <-served:
		_ = ln.Close() // left open when ServeQUIC refused before reading it
		return failed(err)
	case <-ctx.Done():
	}
	dctx, cancel := drain()
	defer cancel()
	return errors.Join(web.Shutdown(dctx, h3, closers...), <-served)
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
// ServeTLS's defaults.
func h3Config(srv *http.Server) httpstream.Config {
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
	var cfg httpstream.Config
	if read > 0 {
		cfg.ReadTimeout = read
	}
	if write > 0 {
		cfg.WriteTimeout = write
	}
	if idle > 0 {
		cfg.IdleTimeout = idle
	}
	if srv.MaxHeaderBytes > 0 {
		cfg.MaxHeaderBytes = srv.MaxHeaderBytes
	}
	return cfg
}
