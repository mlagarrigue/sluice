package quic

import (
	"bytes"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mlagarrigue/sluice/internal/dgram"
)

// countingPC counts the datagrams written through it.
type countingPC struct {
	net.PacketConn
	writes atomic.Int64
}

func (p *countingPC) WriteTo(b []byte, to net.Addr) (int, error) {
	p.writes.Add(1)
	return p.PacketConn.WriteTo(b, to)
}

// delayedPC holds every datagram it writes for a fixed delay, in order: a
// path with a round trip to measure, where dgram.Pair in a bubble has none —
// every exchange there takes no time, and a zero sample is no sample, so the
// RTT estimator would stay at its initial guess.
type delayedPC struct {
	net.PacketConn
	delay time.Duration
	queue chan delayedDatagram
	done  chan struct{}
	once  sync.Once
}

type delayedDatagram struct {
	at time.Time
	b  []byte
	to net.Addr
}

func newDelayedPC(pc net.PacketConn, delay time.Duration) *delayedPC {
	p := &delayedPC{PacketConn: pc, delay: delay, queue: make(chan delayedDatagram, 64), done: make(chan struct{})}
	go func() {
		for {
			select {
			case d := <-p.queue:
				time.Sleep(time.Until(d.at))
				_, _ = p.PacketConn.WriteTo(d.b, d.to)
			case <-p.done:
				return
			}
		}
	}()
	return p
}

func (p *delayedPC) WriteTo(b []byte, to net.Addr) (int, error) {
	select {
	case p.queue <- delayedDatagram{time.Now().Add(p.delay), bytes.Clone(b), to}:
		return len(b), nil
	case <-p.done:
		return 0, net.ErrClosed
	}
}

func (p *delayedPC) Close() error {
	p.once.Do(func() { close(p.done) })
	return p.PacketConn.Close()
}

// A small request answered with a small response costs the server one
// datagram: the response, its FIN, the ACK of the request and nothing else.
// It cost three — the delayed ACK of the request left alone 25 ms after the
// response had gone without it (RFC 9000 §13.2.1 says an ACK SHOULD ride
// other frames), and the stream's slot came back as a MAX_STREAMS of its
// own the moment the stream retired.
//
// The exchange runs in a bubble over dgram.Pair, so no datagram is lost and
// time moves only when every goroutine waits, with the server's datagrams
// held pathDelay on the way: every round trip is exactly that long. The
// client acknowledges the response localMaxAckDelay after it arrives, so the
// ACK reaches the server pathDelay + localMaxAckDelay after the response
// left; the server's probe timeout is the smoothed RTT (pathDelay), four
// variances and the client's max_ack_delay (RFC 9002 §6.2.1) — later, by the
// variances, every run. A probe here is a timeout computed without the
// peer's max_ack_delay, not a late timer.
func TestASmallExchangeCostsTheServerOneDatagram(t *testing.T) {
	const pathDelay = 10 * time.Millisecond
	synctest.Test(t, func(t *testing.T) {
		left, right := dgram.Pair()
		serverPC := &countingPC{PacketConn: newDelayedPC(left, pathDelay)}
		l, err := NewListener(serverPC, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{})
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		accepted := make(chan *Conn, 1)
		go func() {
			c, err := l.Accept()
			if err != nil {
				t.Error(err)
			}
			accepted <- c
		}()
		client, err := Dial(right, serverPC.LocalAddr(), ClientTLSForTest(), DefaultParameters())
		if err != nil {
			t.Fatal(err)
		}
		server := <-accepted
		if server == nil {
			t.Fatal("no server connection")
		}

		// The handshake leaves a tail behind it — HANDSHAKE_DONE, connection
		// IDs, the path MTU probe and their acknowledgements. A second of
		// bubble time lets every timer it armed play out.
		time.Sleep(time.Second)
		synctest.Wait()
		server.mu.Lock()
		sampled := server.rtt.sampled
		server.mu.Unlock()
		if !sampled {
			t.Fatal("the server has no RTT sample: its probe timeout is the initial guess, and this test would not see one computed wrong")
		}

		timeouts := server.Stats().ProbeTimeouts
		before := serverPC.writes.Load()
		smallExchange(t, client, server)
		time.Sleep(4 * localMaxAckDelay) // whatever was delayed has left by now
		synctest.Wait()
		if n := serverPC.writes.Load() - before; n != 1 {
			t.Errorf("the server wrote %d datagrams for one small exchange, want 1", n)
		}
		if got := server.Stats().ProbeTimeouts; got != timeouts {
			t.Errorf("the server's probe timeout fired %d times during the exchange, want 0", got-timeouts)
		}

		// The closing period runs on the bubble's clock, which stops when
		// this function returns: wait it out here.
		_ = client.Close()
		_ = server.Close()
		<-server.gone
	})
}

// A packet's acknowledgement is owed before its frames reach the
// application, so an answer sent while they are being delivered carries it.
// It used to be owed only once every frame had been acted on: a handler that
// read the request and wrote the response in that window sent the response
// without the ACK, and the delayed ACK followed alone localMaxAckDelay later
// — two datagrams for the exchange above, a few runs in a thousand. The
// batch callback runs inside delivery, so it sees the window every time.
func TestAnAnswerSentDuringDeliveryCarriesTheACK(t *testing.T) {
	c := assembleConn(&fuzzSink{addr: "exchange:self"}, fakeFuzzAddr("exchange:peer"),
		[]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, false, DefaultParameters())
	defer c.Close()
	var pending bool
	c.OnStreamFrames(func([]Frame) error {
		c.mu.Lock()
		pending = c.spaces[spaceApplication].ack.pending()
		c.mu.Unlock()
		return nil
	})
	c.spaces[spaceApplication].ack.record(0, time.Now())
	if err := c.frames(spaceApplication, streamFrame(nil, 0, 0, "request", true), time.Now()); err != nil {
		t.Fatal(err)
	}
	if !pending {
		t.Error("the request's ACK was not pending while the request was delivered")
	}
}

// smallExchange runs one request and response on a new stream.
func smallExchange(t *testing.T, client, server *Conn) {
	t.Helper()
	served := make(chan error, 1)
	go func() {
		s, err := server.AcceptStream(t.Context())
		if err != nil {
			served <- err
			return
		}
		if _, err := io.ReadAll(s); err != nil {
			served <- err
			return
		}
		_, err = s.WriteClose([]byte("response"))
		served <- err
	}()
	s, err := client.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteClose([]byte("request")); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(s)
	if err != nil || !bytes.Equal(got, []byte("response")) {
		t.Fatalf("read %q, %v; want %q", got, err, "response")
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
}

// A retired peer stream's slot comes back an eighth of the initial limit at
// a time, never later: the peer is not left waiting on STREAMS_BLOCKED
// (RFC 9000 §4.6), and a limit under eight returns every slot at once.
func TestStreamSlotsComeBackAnEighthAtATime(t *testing.T) {
	for _, tc := range []struct {
		name          string
		initial, done uint64
		wantLimit     uint64
		wantGrants    int
	}{
		{"default limit", 100, 25, 124, 2},
		{"limit one", 1, 3, 4, 3},
		{"limit seven", 7, 3, 10, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Conn{}
			c.params.InitialMaxStreamsBidi = tc.initial
			c.localMaxStreamsBidi = tc.initial
			grants := 0
			for range tc.done {
				before := len(c.grants)
				c.returnStreamSlotLocked(false)
				if len(c.grants) > before {
					grants++
				}
			}
			if c.localMaxStreamsBidi != tc.wantLimit || grants != tc.wantGrants {
				t.Errorf("after %d retirements: limit %d in %d MAX_STREAMS, want %d in %d",
					tc.done, c.localMaxStreamsBidi, grants, tc.wantLimit, tc.wantGrants)
			}
		})
	}
}
