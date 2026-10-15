package httpstream

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mlagarrigue/sluice/net/quic"
)

// ErrShutdownForced is what [Serve], [ServeH2] and [ServeH3] return when
// [Config.ShutdownGrace] passed before the requests in flight were answered,
// and the connections still carrying them were closed under their handlers.
// A drain that ends within the grace returns nil.
var ErrShutdownForced = errors.New("httpstream: the shutdown grace passed with requests in flight")

// Server serves HTTP/1.1 and HTTP/2 on the [net.Listener]s handed to
// [Server.Serve] and HTTP/3 on the [quic.Listener]s handed to
// [Server.ServeQUIC], and stops all of them two ways:
//
//   - [Server.Shutdown] is the normal stop: nothing new is accepted, the
//     connections with no request in flight are closed, and the requests in
//     flight are answered before their connections close.
//   - [Server.Close] is the forced stop: every connection is closed, and the
//     context of every handler still running is cancelled.
//
// A deployment typically calls Shutdown on the first signal and Close on the
// second, or when Shutdown's context ends first; [Serve] is that sequence
// with [Config.ShutdownGrace] between the two.
//
// Config and Handler are read on the first call of any method and not
// after. The zero Server is not usable: Handler is required. A Server is
// stopped for good once Shutdown or Close has been called; a listener handed
// to it afterwards is closed at once.
type Server struct {
	Config  Config
	Handler Handler

	once sync.Once
	cfg  Config // Config, resolved
	err  error  // what resolving Config refused
	// live is the HTTP/1.1 and HTTP/2 connections, and its draining context
	// is the normal stop of every protocol: HTTP/3 connections read it too.
	live *connSet
	// forced is cancelled by Close; it is every handler's context.
	forced context.Context
	force  context.CancelFunc

	mu        sync.Mutex
	stopping  bool // Shutdown or Close has begun
	listeners map[net.Listener]struct{}
	// quicLoops counts the ServeQUIC calls still running: each one waits for
	// its connections before it closes its listener, so they are the HTTP/3
	// half of what Shutdown waits for.
	quicLoops sync.WaitGroup
	h3Open    atomic.Int64 // HTTP/3 connections being served
}

// init resolves the configuration once, whichever method comes first; the
// state Shutdown and Close need is built even when it is refused.
func (s *Server) init() error {
	s.once.Do(func() {
		s.cfg, s.err = s.Config.resolve()
		s.live = newConnSet(max(s.cfg.MaxConns, 0))
		s.forced, s.force = context.WithCancel(context.Background())
		s.listeners = map[net.Listener]struct{}{}
	})
	return s.err
}

// Serve accepts connections on ln and serves each one, HTTP/1.1 or HTTP/2,
// until the server stops. It returns nil when Shutdown or Close closed ln,
// without waiting for the connections — Shutdown is what waits — and an
// error when ln fails, in which case what it accepted is still served. A
// configuration [Config] refuses is returned before ln is read.
//
// What Serve speaks, and what it does with a temporary Accept failure, is
// what the package function [Serve] says.
func (s *Server) Serve(ln net.Listener) error {
	if s.Handler == nil {
		panic("httpstream: Server.Serve requires a handler")
	}
	if err := s.init(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		_ = ln.Close()
		return nil
	}
	s.listeners[ln] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.listeners, ln)
		s.mu.Unlock()
		_ = ln.Close()
	}()

	stopped := s.live.draining.Done()
	var tempDelay time.Duration // how long to wait after a temporary Accept failure
	for {
		c, err := ln.Accept()
		if err != nil {
			select {
			case <-stopped:
				return nil // asked to stop: not a failure
			default:
			}
			if ne, ok := errors.AsType[net.Error](err); ok && ne.Temporary() { //nolint:staticcheck // SA1019: Temporary is what Accept still reports EMFILE and ECONNABORTED through, as net/http reads it
				// The same shape as the QUIC read loop's readBackoff and
				// deliberately not shared with it: four lines are cheaper
				// than a dependency between the two packages.
				tempDelay = min(max(tempDelay*2, 5*time.Millisecond), time.Second)
				t := time.NewTimer(tempDelay)
				select {
				case <-stopped:
					t.Stop()
					return nil
				case <-t.C:
				}
				continue
			}
			return fmt.Errorf("httpstream: accepting: %w", err)
		}
		tempDelay = 0
		st := s.live.add(c)
		if st == nil {
			// Over MaxConns, or the shutdown ran between Accept and here and
			// this connection would never be closed by it. Either way it is
			// closed with nothing read and nothing written: a response would
			// cost the request's read, which is what the bound refuses to
			// spend on a peer in excess (see Config.MaxConns).
			_ = c.Close()
			continue
		}
		go func() {
			defer func() { s.live.remove(c); _ = c.Close() }()
			serveAny(s.forced, c, st, s.cfg, s.Handler)
		}()
	}
}

// ServeQUIC accepts HTTP/3 connections on ln and serves each one as
// [ServeH3] does, until the server stops. Its stops are the server's: at
// Shutdown each connection drains as ServeH3's does at the cancellation of
// its context, at Close each is closed under its handler. A connection that
// completes its handshake once the server is stopping is closed at once.
//
// Unlike a TCP listener, a [quic.Listener] carries its connections — closing
// it ends them (see [quic.Listener.Close]) — so ServeQUIC closes ln only once
// its connections have drained or Close has been called, and returns then:
// nil when the server stopped, an error when ln failed. A configuration
// [Config] refuses is returned before ln is read, and ln is left open.
func (s *Server) ServeQUIC(ln *quic.Listener) error {
	if s.Handler == nil {
		panic("httpstream: Server.ServeQUIC requires a handler")
	}
	if err := s.init(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		_ = ln.Close()
		return nil
	}
	s.quicLoops.Add(1)
	s.mu.Unlock()
	defer s.quicLoops.Done()

	// Added to under s.mu while the server is not stopping, and waited for
	// once it is: no Add can race the Wait.
	var conns sync.WaitGroup
	accepted := make(chan error, 1)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				accepted <- err
				return
			}
			s.mu.Lock()
			if s.stopping {
				s.mu.Unlock()
				_ = c.CloseWithError(h3GracefulCode())
				continue
			}
			conns.Add(1)
			s.h3Open.Add(1)
			s.mu.Unlock()
			go func() {
				defer conns.Done()
				defer s.h3Open.Add(-1)
				_ = serveH3(s.live.draining, s.forced, c, s.cfg, s.Handler)
			}()
		}
	}()

	select {
	case err := <-accepted:
		_ = ln.Close()
		return fmt.Errorf("httpstream: accepting: %w", err)
	case <-s.live.draining.Done():
	}
	drained := make(chan struct{})
	go func() { conns.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-s.forced.Done():
	}
	_ = ln.Close()
	<-accepted
	return nil
}

// Shutdown stops the server normally (docs/design/adr/0017) and waits for
// the stop to finish or ctx to end, whichever comes first.
//
// Every listener stops accepting: a TCP one is closed at once, a QUIC one
// once its connections are done (see [Server.ServeQUIC]). A connection with
// no request in flight — parked between two keep-alive requests, or accepted
// and silent — is closed at once: nothing is owed to it, and waiting for it
// means stopping when the last idle client happens to time out. A connection
// with a request in flight is left to finish it: the handler runs to its
// end and its response goes out whole, after which the connection closes —
// with Connection: close on HTTP/1.1, a GOAWAY on HTTP/2 and HTTP/3, which
// tell the client which requests were not taken and may be sent again.
//
// Shutdown returns nil when every connection has finished, and ctx's error
// when ctx ends first; the connections still open are then left as they
// are, for [Server.Close] to cut or for a later Shutdown to wait for again.
func (s *Server) Shutdown(ctx context.Context) error {
	_ = s.init()
	s.stop()
	done := make(chan struct{})
	go func() {
		s.live.wg.Wait()
		s.quicLoops.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops the server by force: every listener and every connection is
// closed, the requests in flight with them, and the context of every handler
// still running is cancelled, so that what a handler waits on — a query, an
// upstream call — learns it too. Close does not wait for the handlers to
// return. When it cut connections that had requests in flight it says so
// to [Config.Logger], with their number. It returns nil.
func (s *Server) Close() error {
	_ = s.init()
	s.stop()
	cut := s.live.busy() + int(s.h3Open.Load())
	s.force()
	s.live.closeAll()
	if cut > 0 {
		s.cfg.logger().Warn("httpstream: server closed with requests in flight", "connections", cut)
	}
	return nil
}

// stop begins the normal stop, once. The drain is armed before the
// listeners close: a client that reads "refused" on a new dial can then rely
// on every live connection already seeing the shutdown. The other order let
// a handler finish between the two and answer without Connection: close on
// what was the connection's last response. Nothing is accepted in between
// either way: add refuses once the set is closed, which drain does first.
func (s *Server) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping {
		return
	}
	s.stopping = true
	s.live.drain()
	for ln := range s.listeners {
		_ = ln.Close()
	}
}
