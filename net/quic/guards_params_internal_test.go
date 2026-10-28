package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// RFC 9000 §18.2: an ack_delay_exponent over 20 and a max_ack_delay of 2^14
// milliseconds or more are invalid values, and §7.4 makes them
// TRANSPORT_PARAMETER_ERROR. The guard keeps a peer from scaling every ACK
// delay this end reads by a shift it cannot represent, or from stretching
// its probe timeout without bound. Each ceiling is tried on both sides, so a
// guard slid one step either way fails as well as one removed.
func TestAckDelayParametersPastTheirCeilingAreAParameterError(t *testing.T) {
	tests := []struct {
		name    string
		id      uint64
		value   uint64
		refused bool
	}{
		{"ack_delay_exponent 20 is the largest legal", paramAckDelayExponent, 20, false},
		{"ack_delay_exponent 21 is refused", paramAckDelayExponent, 21, true},
		{"ack_delay_exponent of a whole varint is refused", paramAckDelayExponent, 1 << 30, true},
		{"max_ack_delay 2^14-1 ms is the largest legal", paramMaxAckDelay, 1<<14 - 1, false},
		{"max_ack_delay 2^14 ms is refused", paramMaxAckDelay, 1 << 14, true},
		{"max_ack_delay of a whole varint is refused", paramMaxAckDelay, 1 << 40, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := boundsConn(t, false)
			err := c.handlePeerParameters(clientParamsFor(appendParam(nil, tt.id, varint.Append(nil, tt.value))...))
			if !tt.refused {
				if err != nil {
					t.Fatalf("a legal value was refused: %v", err)
				}
				return
			}
			wantCode(t, err, transportParameterError, "the parameter block")
			if err != nil && !strings.Contains(err.Error(), "ack_delay_exponent") && !strings.Contains(err.Error(), "max_ack_delay") {
				t.Errorf("refused for another reason: %v", err)
			}
		})
	}
}

// RFC 9000 §18.2: retry_source_connection_id is one of the parameters only a
// server sends, and a server that receives one from a client closes with
// TRANSPORT_PARAMETER_ERROR (§7.3). The guard keeps a client from claiming a
// Retry it never saw. Present and zero-length counts as present: only the
// parameter's absence is legal.
func TestAClientSendingRetrySourceIDIsAParameterError(t *testing.T) {
	tests := []struct {
		name  string
		value []byte
	}{
		{"with an identifier", []byte{9, 9, 9, 9}},
		{"zero-length", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := boundsConn(t, false)
			err := c.handlePeerParameters(clientParamsFor(appendParam(nil, paramRetrySourceCID, tt.value)...))
			wantCode(t, err, transportParameterError, "a client's retry_source_connection_id")
			if err != nil && !strings.Contains(err.Error(), "retry_source_connection_id") {
				t.Errorf("refused for another reason: %v", err)
			}
		})
	}
	// The control: the same block without the parameter is a client's
	// ordinary announcement.
	if err := boundsConn(t, false).handlePeerParameters(clientParamsFor()); err != nil {
		t.Errorf("the control block was refused: %v", err)
	}
}

// RFC 9000 §7.3: after a Retry, the server's retry_source_connection_id must
// equal the Source Connection ID of the Retry packet, and a client that sees
// another value ends the connection with TRANSPORT_PARAMETER_ERROR or
// PROTOCOL_VIOLATION; this package picks the first, the code every parameter
// refusal shares. The guard keeps a Retry forged by someone on the path from
// being accepted under a handshake the server never redirected.
func TestARetrySourceIDTheServerDoesNotEchoIsAParameterError(t *testing.T) {
	dcid, scid := []byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}
	retrySCID := []byte{0xa, 0xb, 0xc, 0xd}
	client := func(t *testing.T) *Conn {
		t.Helper()
		c := assembleConn(&fuzzSink{addr: "rsc:self"}, fakeFuzzAddr("rsc:peer"), dcid, scid, true, DefaultParameters())
		t.Cleanup(func() { _ = c.Close() })
		c.initialDCID = dcid
		c.mu.Lock()
		c.retried, c.retrySCID = true, retrySCID
		c.mu.Unlock()
		return c
	}
	serverBlock := func(echo []byte) []byte {
		srv := DefaultParameters()
		srv.initialSourceCID = dcid // what the client checks it against here
		srv.originalDCID = dcid
		srv.retrySourceCID = echo
		return appendParameters(nil, srv)
	}

	err := client(t).handlePeerParameters(serverBlock([]byte{0xa, 0xb, 0xc, 0xe})) // one byte off
	wantCode(t, err, transportParameterError, "a retry_source_connection_id the Retry did not carry")
	if err != nil && !strings.Contains(err.Error(), "Retry") {
		t.Errorf("refused for another reason: %v", err)
	}
	err = client(t).handlePeerParameters(serverBlock(retrySCID[:3])) // a prefix of it
	wantCode(t, err, transportParameterError, "a retry_source_connection_id shorter than the Retry's")

	// The control: the exact identifier is the one block that passes.
	if err := client(t).handlePeerParameters(serverBlock(retrySCID)); err != nil {
		t.Errorf("the echoed identifier was refused: %v", err)
	}
}

// ticketLanded is a client session cache that says when a ticket lands, so
// the test waits for the server's NewSessionTicket rather than for a guess.
type ticketLanded struct {
	tls.ClientSessionCache
	stored chan struct{}
}

func (c *ticketLanded) Put(key string, cs *tls.ClientSessionState) {
	c.ClientSessionCache.Put(key, cs)
	if cs != nil {
		select {
		case c.stored <- struct{}{}:
		default:
		}
	}
}

// heldReads parks the client's reads until release is closed: whatever the
// server sends waits in the socket, so everything the test does right after
// DialEarly happens before the server's first answer, however fast the
// loopback is.
type heldReads struct {
	net.PacketConn
	release chan struct{}
}

func (h *heldReads) ReadFrom(b []byte) (int, net.Addr, error) {
	<-h.release
	return h.PacketConn.ReadFrom(b)
}

// RFC 9001 §4.6.2: a server always rejects 0-RTT if it sends a TLS
// HelloRetryRequest, since the second ClientHello no longer matches the
// first one the early data was keyed to. The client DialEarly resumed with
// then has to treat its 0-RTT as refused: the streams it opened fail with
// ErrEarlyDataRejected, and the handshake still completes and carries a new
// stream in 1-RTT. The client offers a key share for its first curve only,
// so a server that takes P-256 alone answers with a HelloRetryRequest
// exactly when that curve is X25519 — the negotiated curve is the witness;
// the control, P-256 first, resumes with no retry and has its 0-RTT read.
func TestADialEarlyClientHasItsZeroRTTRefusedByAHelloRetryRequest(t *testing.T) {
	tests := []struct {
		name         string
		firstCurve   tls.CurveID
		wantRejected bool
	}{
		{"a server forcing a HelloRetryRequest", tls.X25519, true},
		{"control without a HelloRetryRequest", tls.CurveP256, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serverPC, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
			if err != nil {
				t.Fatal(err)
			}
			serverCfg := ServerTLSForTest(t)
			serverCfg.CurvePreferences = []tls.CurveID{tls.CurveP256}
			l, err := NewListener(serverPC, serverCfg, DefaultParameters(), ListenerConfig{EarlyData: true})
			if err != nil {
				_ = serverPC.Close()
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = l.Close() })
			go echoEachStream(l)

			cache := &ticketLanded{ClientSessionCache: tls.NewLRUClientSessionCache(2), stored: make(chan struct{}, 1)}
			clientCfg := ClientTLSForTest()
			clientCfg.ClientSessionCache = cache
			clientCfg.CurvePreferences = []tls.CurveID{tls.CurveP256, tls.X25519}
			listen := func() net.PacketConn {
				pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = pc.Close() })
				return pc
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			// The first connection, with no retry, leaves the ticket.
			first, err := Dial(ctx, listen(), l.Addr(), clientCfg, DefaultParameters())
			if err != nil {
				t.Fatalf("first handshake: %v", err)
			}
			select {
			case <-cache.stored:
			case <-ctx.Done():
				t.Fatal("no session ticket reached the client")
			}
			_ = first.Close()

			// The second resumes it with DialEarly, its first answer held.
			clientCfg.CurvePreferences = []tls.CurveID{tt.firstCurve, tls.CurveP256}
			held := &heldReads{PacketConn: listen(), release: make(chan struct{})}
			tap := newTap(held)
			c, err := DialEarly(ctx, tap, l.Addr(), clientCfg, DefaultParameters())
			if err != nil {
				t.Fatalf("DialEarly: %v", err)
			}
			t.Cleanup(func() { _ = c.Close() })
			select {
			case <-c.HandshakeComplete():
				t.Fatal("DialEarly waited for the handshake: no 0-RTT was attempted, the test proves nothing")
			default:
			}
			early, err := c.OpenStream(ctx)
			if err != nil {
				t.Fatalf("opening a stream in 0-RTT: %v", err)
			}
			if _, err := early.WriteFinal([]byte("early")); err != nil {
				t.Fatalf("writing in 0-RTT: %v", err)
			}
			sentEarly := false
			for _, w := range tap.recorded() {
				sentEarly = sentEarly || hasTypes(packetTypes(t, w.data), packetZeroRTT)
			}
			if !sentEarly {
				t.Fatal("no 0-RTT packet left: the test proves nothing")
			}

			close(held.release)
			select {
			case <-c.HandshakeComplete():
			case <-c.Done():
				// crypto/tls (Go 1.26.9 and 1.27.3 alike) computes the PSK
				// binders of a second ClientHello before it drops
				// early_data from it, so a resuming client that offered 0-RTT
				// sends binders the server cannot verify and is closed with
				// decrypt_error (CRYPTO_ERROR 0x133). The connection cannot
				// survive that, and its error says why: the refused 0-RTT,
				// the HelloRetryRequest, and the server's code beneath. On a
				// toolchain with the order fixed the handshake completes and
				// the rest of the subtest runs.
				err := c.Err()
				if !tt.wantRejected {
					t.Fatalf("the connection ended before the handshake completed: %v", err)
				}
				var ce *ConnectionError
				if !errors.Is(err, ErrEarlyDataRejected) || !errors.Is(err, errEarlyHelloRetry) || !errors.As(err, &ce) {
					t.Fatalf("the connection ended with %v; want ErrEarlyDataRejected naming the HelloRetryRequest, over the server's ConnectionError", err)
				}
				return
			case <-ctx.Done():
				t.Fatal("the handshake did not complete")
			}
			if got := c.ConnectionState().CurveID; got != tls.CurveP256 {
				t.Fatalf("negotiated %v, want P-256: the server's choice was not forced", got)
			}
			if !c.ConnectionState().DidResume {
				t.Fatal("the session was not resumed")
			}

			c.mu.Lock()
			rejected := c.earlyRejected
			c.mu.Unlock()
			got, readErr := io.ReadAll(early)
			if tt.wantRejected {
				if !rejected {
					t.Error("a HelloRetryRequest did not make the client treat its 0-RTT as rejected")
				}
				if !errors.Is(readErr, ErrEarlyDataRejected) {
					t.Errorf("reading the 0-RTT stream = %q, %v; want ErrEarlyDataRejected", got, readErr)
				}
			} else {
				if rejected {
					t.Error("the control had its 0-RTT rejected: the test proves nothing")
				}
				if readErr != nil || string(got) != "early" {
					t.Errorf("control: the 0-RTT stream echoed %q, %v; want %q", got, readErr, "early")
				}
			}

			// Either way the connection works in 1-RTT afterwards.
			next, err := c.OpenStream(ctx)
			if err != nil {
				t.Fatalf("opening a stream after the handshake: %v", err)
			}
			if _, err := next.WriteFinal([]byte("after")); err != nil {
				t.Fatalf("writing in 1-RTT: %v", err)
			}
			if got, err := io.ReadAll(next); err != nil || string(got) != "after" {
				t.Errorf("the 1-RTT stream echoed %q, %v; want %q", got, err, "after")
			}
		})
	}
}

// echoEachStream answers every stream of every connection l accepts with
// what it read, FIN included, until l closes.
func echoEachStream(l *Listener) {
	for {
		c, err := l.Accept()
		if err != nil {
			return
		}
		go func() {
			for {
				s, err := c.AcceptStream(context.Background())
				if err != nil {
					return
				}
				go func() {
					b, _ := io.ReadAll(s)
					_, _ = s.WriteFinal(b)
				}()
			}
		}()
	}
}
