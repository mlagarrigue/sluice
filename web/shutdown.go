package web

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// A Server is anything that stops by draining: [http.Server] is one, and so
// is every Serve(ctx) once [Start] has run it. [Shutdown] takes one; several
// become one through [Servers].
//
// Shutdown stops admitting work and returns once the work already admitted
// has finished, or once ctx is done, whichever comes first. Returning early
// does not cancel the drain — the server keeps finishing on its own — it only
// stops waiting for it, and says so with ctx's error.
//
// A server that can also stop by force — a Close() error method, as
// [http.Server] and httpstream's Server have — is closed by [Shutdown] when
// ctx ends before its drain did.
type Server interface {
	Shutdown(context.Context) error
}

// Shutdown stops a server and then the things it was serving from, in that
// order.
//
// # The order is the whole function
//
// A gateway closed before the server has stopped serving is a gateway that
// refuses calls the in-flight requests are about to make. Those requests were
// accepted, they are inside a handler, and they now fail for a reason that has
// nothing to do with them — the server is not even shutting down from their
// point of view, it is broken. Every one of them answers 500 during what was
// supposed to be a clean stop.
//
// So: [Server.Shutdown] first, which stops accepting and waits for the
// handlers already running. Only once they have all returned is there nothing
// left that could call a gateway, and only then is it safe to close one.
//
//	err := web.Shutdown(ctx, srv, ordersGateway, conn)
//
// This is three lines of code and it is here because the order is not
// discoverable: both spellings compile, both look symmetrical, and the wrong
// one fails only under load during a deploy.
//
// A process that serves on more than one front — an [http.Server] for HTTP/1.1
// and HTTP/2, an httpstream Server for HTTP/3 — drains them together, since
// none of them calls the other, and closes the gateway after the last one:
//
//	h3 := &httpstream.Server{Config: cfg, Handler: handle}
//	go h3.ServeQUIC(quicLn)
//	err := web.Shutdown(ctx, web.Servers(srv, h3), ordersGateway, conn)
//
// # What ctx bounds
//
// The server's drain, and that is the stop in two steps of ADR 0017: the
// normal stop first, the forced one when ctx ends before it finished. A
// deadline that expires means requests are still running; a server with a
// Close method is then closed, which cuts its connections and, for an
// httpstream Server, cancels the context of every handler still running.
// [http.ErrServerClosed] is not what comes back then, [context.DeadlineExceeded]
// is, and the closers still run. That is deliberate: a gateway left open
// because a handler hung is a goroutine and a connection leaked for the life
// of the process, which is worse than closing under a request that was not
// going to finish anyway.
//
// The closers are **always** run, and their failures are joined with the
// server's. A shutdown that abandons half its cleanup because the first step
// timed out is the shape of bug this function exists to remove.
func Shutdown(ctx context.Context, srv Server, closers ...io.Closer) error {
	if srv == nil {
		return errors.New("web: Shutdown requires a server")
	}

	var errs []error
	if err := srv.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("draining the server: %w", err))
		// The drain gave up because ctx ended, not because it failed: what
		// it left running is cut now rather than left to outlive the closers.
		if c, ok := srv.(io.Closer); ok && ctx.Err() != nil {
			if err := c.Close(); err != nil {
				errs = append(errs, fmt.Errorf("closing the server: %w", err))
			}
		}
	}
	for i, c := range closers {
		if c == nil {
			continue
		}
		if err := c.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing %d: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

// Servers is several servers as one, for [Shutdown] to stop together.
//
// They drain at the same time, not one after the other: the front of a
// process is one layer whatever the number of listeners, and nothing on it
// calls anything else on it, so a serial drain would only add the graces up.
// Shutdown returns once every one of them has, with their failures joined;
// a nil entry is skipped. Close closes every one of them that has a Close
// method, which is what [Shutdown] calls when its ctx ends first.
func Servers(srvs ...Server) Server {
	return servers(srvs)
}

type servers []Server

func (ss servers) Shutdown(ctx context.Context) error {
	errs := make([]error, len(ss))
	var wg sync.WaitGroup
	for i, s := range ss {
		if s == nil {
			continue
		}
		wg.Go(func() { errs[i] = s.Shutdown(ctx) })
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (ss servers) Close() error {
	var errs []error
	for _, s := range ss {
		if c, ok := s.(io.Closer); ok {
			errs = append(errs, c.Close())
		}
	}
	return errors.Join(errs...)
}

// Start runs serve on its own goroutine and returns the handle that stops it.
//
// It is for a server whose only stop signal is its context — httpstream's
// Serve and ServeH3, or any Serve(ctx) written in that shape — so that
// [Shutdown] can stop it next to an [http.Server] and in the same order. The
// handle's Shutdown cancels the context serve was given and waits for serve
// to return, which is when that server's own drain, bounded by its own grace,
// is over. Nothing is added to the server: it still has one signal, and the
// handle is what composes it.
//
//	hs := web.Start(ctx, func(ctx context.Context) error {
//	    return httpstream.Serve(ctx, ln, cfg, handle)
//	})
//	select {
//	case <-ctx.Done():
//	case <-hs.Done():
//	}
//	err := web.Shutdown(drain, web.Servers(srv, hs), gw)
//
// ctx is the parent of what serve gets: cancelling it stops the server too,
// which is the signal.NotifyContext shape. A serve that returns on its own —
// a listener failed, a port was in use — is reported by [Started.Done], and
// its error comes back from the next Shutdown, where a main function reads it.
//
// A Started has no Close: its one signal is the context, already cancelled
// by Shutdown, and the forced stop is serve's own — httpstream's Serve
// forces at its ShutdownGrace. A server with a forced stop of its own to
// offer [Shutdown] is handed to it directly, as httpstream's Server is.
func Start(ctx context.Context, serve func(context.Context) error) *Started {
	if serve == nil {
		panic("web: Start requires a serve function")
	}
	ctx, cancel := context.WithCancel(ctx)
	s := &Started{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		s.err = serve(ctx)
	}()
	return s
}

// A Started is a Serve(ctx) that [Start] is running. It is a [Server].
type Started struct {
	cancel context.CancelFunc
	done   chan struct{}
	err    error // written before done is closed, read after
}

// Done is closed when serve has returned, whether it was asked to or not.
func (s *Started) Done() <-chan struct{} { return s.done }

// Shutdown cancels serve's context and waits for it to return, or for ctx
// to be done, whichever comes first. It returns what serve returned — nil
// for a server that stopped because it was asked to — or ctx's error when
// the wait ended first. Calling it again waits again.
func (s *Started) Shutdown(ctx context.Context) error {
	s.cancel()
	select {
	case <-s.done:
		return s.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// DefaultReadHeaderTimeout is the header deadline [Serve] and [ServeTLS] give
// a server that set no read deadline at all. Ten seconds is generous for a
// real client on a slow link and short enough that an idle trickle of bytes
// cannot pin connections for the life of the process.
const DefaultReadHeaderTimeout = 10 * time.Second

// DefaultIdleTimeout is how long [Serve] and [ServeTLS] let a keep-alive
// connection wait for its next request when the server set no idle or read
// deadline. Without it, a client that made one request and went quiet holds
// its connection, and a file descriptor, for the life of the process. Two
// minutes outlasts the idle timeout of the common proxies in front (60
// seconds on most load balancers and on nginx's upstream pool), so the
// server is not the side that closes a connection the proxy is about to
// reuse; a proxy that keeps its connections longer wants a longer IdleTimeout.
const DefaultIdleTimeout = 2 * time.Minute

// Serve runs a server until ctx is cancelled, then shuts it down.
//
// It is the shape a main function wants: one call that blocks while the
// service is up and returns when it is properly down, with the ordering of
// [Shutdown] applied.
//
//	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
//	defer stop()
//	if err := web.Serve(ctx, srv, 10*time.Second, gw); err != nil {
//	    log.Fatal(err)
//	}
//
// grace bounds the drain, counted from the moment ctx is cancelled — not from
// the process start, and not from the first signal handler. A zero or negative
// grace means no bound, which is a choice rather than a default: a deploy that
// waits forever for one stuck request is a deploy that does not finish.
//
// [http.ErrServerClosed] is not an error here. It is what ListenAndServe
// returns when Shutdown was called, which is precisely the successful path.
//
// A server with neither ReadHeaderTimeout nor ReadTimeout set has no bound on
// how long a client may take to send its headers, which is the slowloris
// shape: a connection held open a byte at a time, for free, until the
// process runs out of them. Serve sets ReadHeaderTimeout to
// [DefaultReadHeaderTimeout] on such a server before listening — the same
// stance [DecodeJSON] takes by refusing an unbounded body. A negative
// ReadHeaderTimeout is an explicit "no bound" and is left alone, as is any
// server that already bounds its reads.
//
// The same holds between requests: a keep-alive connection waiting for its
// next request has no deadline unless IdleTimeout or ReadTimeout gives it one.
// Serve sets IdleTimeout to [DefaultIdleTimeout] on a server with neither; a
// negative IdleTimeout is left alone.
//
// What Serve runs is cleartext ListenAndServe. That is kept for
// interoperability — a TLS-terminating proxy in front, a health check, local
// tooling — and is not the recommended shape for anything that crosses a
// network boundary: prefer [ServeTLS], which is also how the supported path
// speaks HTTP/2.
func Serve(ctx context.Context, srv *http.Server, grace time.Duration, closers ...io.Closer) error {
	if srv == nil {
		return errors.New("web: Serve requires a server")
	}
	return serve(ctx, srv, grace, srv.ListenAndServe, closers...)
}

// ServeTLS is [Serve] over TLS, and it is how the supported path speaks
// HTTP/2.
//
// The protocol is not a setting here and there is nothing to turn on: net/http
// offers "h2" through ALPN whenever it serves TLS, and a client that asks for
// it gets it. Everything in this package works unchanged over it — a batch of
// one is still a batch of one, since HTTP/2 delivers a request to a handler
// exactly as HTTP/1.1 does. What changes is on the wire rather than in the
// pipeline: header compression, and many requests multiplexed over one
// connection instead of queued behind each other.
//
// That last part is the one worth knowing, because it changes what a gateway
// in front of this is worth: requests multiplexed on one HTTP/2 connection do
// reach the server concurrently, where the same requests pipelined over
// HTTP/1.1 do not — see [gateway]'s note on the pipelining client. HTTP/2
// removes that trap rather than the library doing it.
//
// srv gets the same ReadHeaderTimeout and IdleTimeout defaults as under
// [Serve].
//
// certFile and keyFile are as [http.Server.ListenAndServeTLS] takes them, and
// may be empty when srv.TLSConfig already carries the certificates.
func ServeTLS(ctx context.Context, srv *http.Server, grace time.Duration, certFile, keyFile string, closers ...io.Closer) error {
	if srv == nil {
		return errors.New("web: ServeTLS requires a server")
	}
	return serve(ctx, srv, grace, func() error { return srv.ListenAndServeTLS(certFile, keyFile) }, closers...)
}

// serve is the lifecycle both share: listen on a goroutine, drain on ctx, and
// join the two errors rather than let one hide the other.
func serve(ctx context.Context, srv *http.Server, grace time.Duration, listen func() error, closers ...io.Closer) error {
	// net/http falls back to ReadTimeout when ReadHeaderTimeout is zero, so
	// only a server bounding neither is unbounded.
	if srv.ReadHeaderTimeout == 0 && srv.ReadTimeout <= 0 {
		srv.ReadHeaderTimeout = DefaultReadHeaderTimeout
	}
	// The same fallback holds for IdleTimeout, and a zero there leaves a
	// keep-alive connection waiting for its next request forever.
	if srv.IdleTimeout == 0 && srv.ReadTimeout <= 0 {
		srv.IdleTimeout = DefaultIdleTimeout
	}
	listenErr := make(chan error, 1)
	go func() {
		err := listen()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil // Shutdown was called; this is the ordinary end
		}
		listenErr <- err
	}()

	select {
	case err := <-listenErr:
		// The listener failed before anything asked it to stop — a port in
		// use, a permission refused. The closers still run: whatever was
		// opened before the listen attempt is open now.
		return errors.Join(err, Shutdown(context.Background(), srv, closers...))
	case <-ctx.Done():
	}

	drain := context.Background()
	if grace > 0 {
		var cancel context.CancelFunc
		drain, cancel = context.WithTimeout(drain, grace)
		defer cancel()
	}
	// The shutdown error and the listener's, joined: ListenAndServe has
	// already returned or is about to, and losing its reason because the drain
	// also had one is how a port-in-use turns into a mysterious timeout.
	return errors.Join(Shutdown(drain, srv, closers...), <-listenErr)
}
