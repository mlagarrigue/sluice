package quic_test

import (
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/dgram"
	"github.com/mlagarrigue/sluice/net/quic"
)

// serverTLS and clientTLS are the internal test package's helpers, shared
// rather than restated.
var (
	serverTLS = quic.ServerTLSForTest
	clientTLS = quic.ClientTLSForTest
)

// A real TLS 1.3 handshake over UDP, carried in CRYPTO frames by this
// package, with the keys it hands back protecting the packets that follow.
//
// It is what turns "the pieces agree with each other" into "two endpoints
// that did not share memory agreed" — still not interoperability with another
// implementation, but no longer a round trip inside one process's own
// assumptions.
func TestHandshakeAndStream(t *testing.T) {
	// An in-memory pair rather than loopback UDP. There is no loss recovery
	// under this connection, so a datagram the kernel drops is dropped for
	// good — which made this test fail about one run in five under a loaded
	// -race suite, with a message about a handler that was never reached. The
	// handshake, the packet protection and the two endpoints are unchanged;
	// only the transport's right to lose a packet is gone.
	serverPC, clientPC := dgram.Pair()
	defer func() { _ = serverPC.Close() }()
	defer func() { _ = clientPC.Close() }()

	type result struct {
		got string
		err error
	}
	done := make(chan result, 1)

	go func() {
		buf := make([]byte, 2048)
		if err := serverPC.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			done <- result{err: err}
			return
		}
		n, peer, err := serverPC.ReadFrom(buf)
		if err != nil {
			done <- result{err: err}
			return
		}
		conn, err := quic.Accept(serverPC, peer, buf[:n], serverTLS(t), quic.DefaultParameters())
		if err != nil {
			done <- result{err: err}
			return
		}
		defer func() { _ = conn.Close() }()

		s, err := conn.AcceptStream()
		if err != nil {
			done <- result{err: err}
			return
		}
		body, err := io.ReadAll(s)
		if err != nil && !errors.Is(err, io.EOF) {
			done <- result{err: fmt.Errorf("read: %w (conn: %v)", err, conn.Err())} //nolint:errorlint // conn.Err() is context and may be nil
			return
		}
		done <- result{got: string(body)}
	}()

	conn, err := quic.Dial(clientPC, serverPC.LocalAddr(), clientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	s, err := conn.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("a request on a real stream")); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseWrite(); err != nil {
		t.Fatal(err)
	}

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("the server: %v", r.err)
		}
		if r.got != "a request on a real stream" {
			t.Errorf("the server read %q", r.got)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the exchange did not finish")
	}
}

// RESET_STREAM, both ways: a stream abandoned by one end reaches the other as
// an error rather than as an end of file.
//
// The distinction is the whole value of the frame. A reader that cannot tell
// "the message is complete" from "the sender gave up" hands a truncated body
// to whoever asked for it, and does so silently — which is the failure this
// package refuses everywhere else. It is also the per-stream error channel a
// multiplexed transport has: one request that cannot be served ends on its own
// stream and the connection carries on.
func TestResetStreamReachesTheReader(t *testing.T) {
	// An in-memory pair rather than loopback UDP. There is no loss recovery
	// under this connection, so a datagram the kernel drops is dropped for
	// good — which made this test fail about one run in five under a loaded
	// -race suite, with a message about a handler that was never reached. The
	// handshake, the packet protection and the two endpoints are unchanged;
	// only the transport's right to lose a packet is gone.
	serverPC, clientPC := dgram.Pair()
	defer func() { _ = serverPC.Close() }()
	defer func() { _ = clientPC.Close() }()

	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 2048)
		if err := serverPC.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			done <- err
			return
		}
		n, peer, err := serverPC.ReadFrom(buf)
		if err != nil {
			done <- err
			return
		}
		conn, err := quic.Accept(serverPC, peer, buf[:n], serverTLS(t), quic.DefaultParameters())
		if err != nil {
			done <- err
			return
		}
		defer func() { _ = conn.Close() }()

		s, err := conn.AcceptStream()
		if err != nil {
			done <- err
			return
		}
		_, err = io.ReadAll(s)
		switch {
		case err == nil, errors.Is(err, io.EOF):
			done <- fmt.Errorf("an abandoned stream read as a complete one (%v)", err) //nolint:errorlint // err may be nil here
		case !errors.Is(err, quic.ErrQUIC):
			done <- fmt.Errorf("the reset surfaced as %w, want a quic error", err)
		default:
			if _, reset := s.WasReset(); !reset {
				done <- errors.New("the stream does not report itself reset")
				return
			}
			done <- nil
		}
	}()

	conn, err := quic.Dial(clientPC, serverPC.LocalAddr(), clientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	s, err := conn.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("half a request")); err != nil {
		t.Fatal(err)
	}
	// 0x010c is H3_REQUEST_CANCELLED, which is what an HTTP/3 server sends
	// when it abandons a request rather than answering it.
	if err := s.Reset(0x010c); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the server: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the exchange did not finish")
	}
}

// Stream frames that arrive before anyone registers a callback are held, not
// dropped.
//
// The read loop starts inside Accept, so there is a window between a
// connection existing and its server having said what to do with the frames on
// it — and a client whose handshake finished a moment earlier writes straight
// into that window. Those frames used to be discarded in silence, and with no
// loss recovery underneath, discarded meant gone: the request never reached
// the handler and nothing in the logs said why. It failed about one HTTP/3
// test run in five, and a forced 50 ms window failed every one.
func TestStreamFramesBeforeTheCallbackAreHeld(t *testing.T) {
	serverPC, clientPC := dgram.Pair()
	defer func() { _ = serverPC.Close() }()
	defer func() { _ = clientPC.Close() }()

	type server struct {
		conn *quic.Conn
		err  error
	}
	up := make(chan server, 1)
	go func() {
		buf := make([]byte, 2048)
		if err := serverPC.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			up <- server{err: err}
			return
		}
		n, peer, err := serverPC.ReadFrom(buf)
		if err != nil {
			up <- server{err: err}
			return
		}
		conn, err := quic.Accept(serverPC, peer, buf[:n], serverTLS(t), quic.DefaultParameters())
		up <- server{conn: conn, err: err}
	}()

	conn, err := quic.Dial(clientPC, serverPC.LocalAddr(), clientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	s := <-up
	if s.err != nil {
		t.Fatalf("the server: %v", s.err)
	}
	defer func() { _ = s.conn.Close() }()

	stream, err := conn.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte("a request nobody is listening for yet")); err != nil {
		t.Fatal(err)
	}

	// Long enough that the frame has certainly been received and acted on
	// before anything asks for it. Waiting is the test: this is the window a
	// server races against, made wide on purpose.
	time.Sleep(100 * time.Millisecond)

	got := make(chan []byte, 4)
	s.conn.OnStreamFrames(func(frames []quic.Frame) error {
		for _, f := range frames {
			got <- append([]byte(nil), f.Data...)
		}
		return nil
	})

	select {
	case data := <-got:
		if string(data) != "a request nobody is listening for yet" {
			t.Errorf("the held frame carried %q", data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a frame that arrived before the callback was never delivered")
	}
}

// ALPN is mandatory in QUIC (RFC 9001 §8.1). A tls.Config with no NextProtos
// would fail against a conformant peer mid-handshake with a bare
// no_application_protocol alert; every constructor refuses it up front with
// an error that names the field instead.
func TestEmptyALPNIsRefusedAtConstruction(t *testing.T) {
	pc, peer := dgram.Pair()
	defer func() { _ = pc.Close() }()
	defer func() { _ = peer.Close() }()

	noALPN := clientTLS()
	noALPN.NextProtos = nil

	if _, err := quic.Dial(pc, peer.LocalAddr(), noALPN, quic.DefaultParameters()); err == nil {
		t.Error("Dial accepted a config with no ALPN")
	}
	if _, err := quic.Accept(pc, peer.LocalAddr(), []byte{0}, noALPN, quic.DefaultParameters()); err == nil {
		t.Error("Accept accepted a config with no ALPN")
	}
	if _, err := quic.NewListener(pc, noALPN, quic.DefaultParameters(), quic.ListenerConfig{}); err == nil {
		t.Error("NewListener accepted a config with no ALPN")
	}
	if _, err := quic.Dial(pc, peer.LocalAddr(), nil, quic.DefaultParameters()); err == nil {
		t.Error("Dial accepted a nil tls.Config")
	}
}

// PeerClosed is how a layer above learns the application code a peer closed
// with — HTTP/3's H3_NO_ERROR — and distinguishes it from a transport close,
// which it does not report.
func TestPeerClosedReportsTheApplicationCode(t *testing.T) {
	serverPC, clientPC := dgram.Pair()
	defer serverPC.Close()
	defer clientPC.Close()
	accepted := make(chan *quic.Conn, 1)
	go func() {
		buf := make([]byte, 2048)
		n, peer, err := serverPC.ReadFrom(buf)
		if err != nil {
			close(accepted)
			return
		}
		c, err := quic.Accept(serverPC, peer, buf[:n], serverTLS(t), quic.DefaultParameters())
		if err != nil {
			close(accepted)
			return
		}
		accepted <- c
	}()
	client, err := quic.Dial(clientPC, serverPC.LocalAddr(), clientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server, ok := <-accepted
	if !ok {
		t.Fatal("Accept failed")
	}
	if _, _, ok := client.PeerClosed(); ok {
		t.Fatal("PeerClosed reports a close before any happened")
	}
	if err := server.CloseWithError(0x100); err != nil {
		t.Fatal(err)
	}
	select {
	case <-client.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the client never saw the peer's close")
	}
	code, _, ok := client.PeerClosed()
	if !ok || code != 0x100 {
		t.Fatalf("PeerClosed = (%#x, %v), want (0x100, true)", code, ok)
	}
	if err := client.Err(); err != nil {
		t.Fatalf("an application close with code 0x100 is the peer's decision, not an error here: %v", err)
	}
}
