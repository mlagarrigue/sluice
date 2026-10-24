package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"
)

// TestErrorsByNature: a 404, a 503 and a key never answered each land in
// their own class (§1.2), the unanswered one once its deadline 10 X has
// passed (no RTT on a loopback). They count against the SLO; the
// connections stay the generator's: under HTTP/1.1 the one under the
// abandoned request is closed by the generator and opened again, which is
// a reconnection and no closure by the server.
func TestErrorsByNature(t *testing.T) {
	protos(t, testErrorsByNature)
}

func testErrorsByNature(t *testing.T, proto string) { //nolint:thelper // a sub-test body
	c := testCampaign(t, proto, "https://unused", "-scenario", "unit", "-rate", "100", "-duration", "1s", "-conns", "4", "-slo", "10ms")
	draw := c.scenario.draw(c.seed, c.keyspace)
	srv := sleepServer(t, proto, 0, map[int64]fault{draw(20).key: silent, draw(42).key: notFound, draw(60).key: unavailable})
	c.url = srv.URL
	rep := runStage(t, c)
	t.Logf("%+v", rep)
	if rep.Status4xx != 1 || rep.Status5xx != 1 || rep.TimedOut != 1 || rep.Errors != 2 || rep.OK != rep.Sent-3 {
		t.Fatalf("4xx %d 5xx %d timed out %d errors %d ok %d of %d, want one each and two errors", rep.Status4xx, rep.Status5xx, rep.TimedOut, rep.Errors, rep.OK, rep.Sent)
	}
	if rep.BadKey+rep.Tester+rep.Refused+rep.Reset+rep.ConnectTimeout+rep.Other != 0 {
		t.Fatalf("bad_key %d tester %d refused %d reset %d connect_timeout %d other %d, want none", rep.BadKey, rep.Tester, rep.Refused, rep.Reset, rep.ConnectTimeout, rep.Other)
	}
	if rep.Deadline != 100 {
		t.Fatalf("deadline %v ms, want 10 X = 100", rep.Deadline)
	}
	if rep.Held || rep.Cause != causeSLO {
		t.Fatalf("held %v cause %q, want the SLO: 3 in %d over 0.1 %%", rep.Held, rep.Cause, rep.Sent)
	}
	reconnects := 0
	if proto == protoH1 {
		reconnects = 1
	}
	if rep.Closed != 0 || rep.Reconnects != reconnects {
		t.Fatalf("closed %d reconnects %d, want 0 and %d", rep.Closed, rep.Reconnects, reconnects)
	}
}

// TestServerClosures: a server that ends each connection after 10
// requests, by `Connection: close` under HTTP/1.1 and the GOAWAY net/http
// makes of it under HTTP/2. On one connection, 101 requests (the warm-up's
// and 100 measured) see 10 closures, all inside the measurement; each is
// counted once, and each costs the generator a reconnection and its
// handshake, with no request lost.
func TestServerClosures(t *testing.T) {
	for _, proto := range []string{protoH1, protoH2} {
		t.Run(proto, func(t *testing.T) {
			srv := sleepServer(t, proto, 0, nil)
			srv.closeEvery.Store(10)
			c := testCampaign(t, proto, srv.URL, "-scenario", "unit", "-rate", "100", "-duration", "1s", "-conns", "1")
			rep := runStage(t, c)
			t.Logf("sent %d closed %d reconnects %d handshakes %d, server closed %d", rep.Sent, rep.Closed, rep.Reconnects, rep.Handshakes, srv.closes.Load())
			if rep.Failed() {
				t.Fatalf("%+v", rep)
			}
			if got := srv.closes.Load(); got != 10 {
				t.Fatalf("the server closed %d connections, want 10", got)
			}
			if rep.Closed != 10 || rep.Reconnects != 10 || rep.Handshakes != 10 {
				t.Fatalf("closed %d reconnects %d handshakes %d, want 10 each", rep.Closed, rep.Reconnects, rep.Handshakes)
			}
			if got := srv.conns.Load(); got != 11 {
				t.Fatalf("the server accepted %d connections, want 11", got)
			}
		})
	}
}

// TestNothingListens: a fresh connection to a TCP port nothing listens on
// is refused; to a UDP one, QUIC hears nothing back and the deadline
// passes while the connection is opening.
func TestNothingListens(t *testing.T) {
	for _, tc := range []struct {
		proto string
		want  nature
	}{
		{protoH1, natureRefused},
		{protoH2, natureRefused},
		{protoH3, natureConnectTimeout},
	} {
		t.Run(tc.proto, func(t *testing.T) {
			c := testCampaign(t, tc.proto, "https://"+closedPort(t, tc.proto), "-scenario", "static", "-slo", "5ms")
			cl, err := c.newClient()
			if err != nil {
				t.Fatal(err)
			}
			defer cl.close()
			r := cl.send(context.Background(), 0, job{req: request{kind: static, method: "GET", path: "/static"}, due: time.Now(), fresh: true})
			if r.nature != tc.want {
				t.Fatalf("nature %d (%v), want %d", r.nature, r.err, tc.want)
			}
		})
	}
}

// closedPort is a loopback address that was just free: listened on, then
// closed.
func closedPort(t *testing.T, proto string) string {
	t.Helper()
	if proto == protoH3 {
		pc, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer pc.Close()
		return pc.LocalAddr().String()
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// TestClassify: the errors no loopback produces on demand, as the
// transports wrap them.
func TestClassify(t *testing.T) {
	sys := func(errno syscall.Errno) error {
		return fmt.Errorf("Get: %w", &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", errno)})
	}
	live := context.Background()
	past, cancel := context.WithDeadline(live, time.Now().Add(-time.Second))
	defer cancel()
	for _, tc := range []struct {
		name    string
		ctx     context.Context //nolint:containedctx // a case of the table
		err     error
		opening bool
		want    nature
	}{
		{"no local port", live, sys(syscall.EADDRNOTAVAIL), true, natureTester},
		{"no descriptor", live, sys(syscall.EMFILE), true, natureTester},
		{"refused", live, sys(syscall.ECONNREFUSED), true, natureRefused},
		{"reset", live, sys(syscall.ECONNRESET), false, natureReset},
		{"closed under the request", live, fmt.Errorf("Get: %w", io.EOF), false, natureReset},
		{"deadline while opening", past, fmt.Errorf("Get: %w", context.DeadlineExceeded), true, natureConnectTimeout},
		{"deadline on a connection", past, fmt.Errorf("Get: %w", context.DeadlineExceeded), false, natureTimedOut},
		{"deadline, whatever the error says", past, errors.New("net/http: request canceled"), false, natureTimedOut},
		{"a failed handshake", live, errors.New("tls: handshake failure"), true, natureOther},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := classify(tc.ctx, tc.err, tc.opening); got != tc.want {
				t.Fatalf("classify(%v) = %d, want %d", tc.err, got, tc.want)
			}
		})
	}
}

// TestVerdictAddsTheInjectedRTT: X is fixed net of the injected network
// (§3.2), so a p99 of 25 ms over a 20 ms link holds a 10 ms X, and the
// same p99 on a loopback does not.
func TestVerdictAddsTheInjectedRTT(t *testing.T) {
	for _, tc := range []struct {
		rtt  float64
		held bool
	}{{20, true}, {0, false}} {
		r := report{SLO: 10, RTT: tc.rtt, Sent: 1000, OK: 1000, TTLB: quantiles{P99: 25}}
		r.verdict()
		if r.Held != tc.held {
			t.Errorf("rtt %v ms: held %v cause %q, want held %v", tc.rtt, r.Held, r.Cause, tc.held)
		}
	}
}
