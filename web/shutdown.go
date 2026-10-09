package web

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

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
// So: [http.Server.Shutdown] first, which stops accepting and waits for the
// handlers already running. Only once they have all returned is there nothing
// left that could call a gateway, and only then is it safe to close one.
//
//	err := web.Shutdown(ctx, srv, ordersGateway, conn)
//
// This is three lines of code and it is here because the order is not
// discoverable: both spellings compile, both look symmetrical, and the wrong
// one fails only under load during a deploy.
//
// # What ctx bounds
//
// The server's drain. A deadline that expires means requests are still running
// and the server gives up waiting; [http.ErrServerClosed] is not what comes
// back then, [context.DeadlineExceeded] is, and the closers still run. That is
// deliberate: a gateway left open because a handler hung is a goroutine and a
// connection leaked for the life of the process, which is worse than closing
// under a request that was not going to finish anyway.
//
// The closers are **always** run, and their failures are joined with the
// server's. A shutdown that abandons half its cleanup because the first step
// timed out is the shape of bug this function exists to remove.
func Shutdown(ctx context.Context, srv *http.Server, closers ...io.Closer) error {
	if srv == nil {
		return errors.New("web: Shutdown requires a server")
	}

	var errs []error
	if err := srv.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("draining the server: %w", err))
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

// DefaultReadHeaderTimeout is the header deadline [Serve] and [ServeTLS] give
// a server that set no read deadline at all. Ten seconds is generous for a
// real client on a slow link and short enough that an idle trickle of bytes
// cannot pin connections for the life of the process.
const DefaultReadHeaderTimeout = 10 * time.Second

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
// srv gets the same ReadHeaderTimeout default as under [Serve].
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
