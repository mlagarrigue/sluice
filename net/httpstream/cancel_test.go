package httpstream

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/dgram"
	"github.com/mlagarrigue/sluice/net/quic"
)

// waitingHandler blocks each call until its context ends, or ten seconds,
// and reports the batch size on entry and the context's error on exit.
type waitingHandler struct {
	entered chan int
	left    chan error
	hold    time.Duration
}

func newWaitingHandler(hold time.Duration) *waitingHandler {
	return &waitingHandler{entered: make(chan int, 4), left: make(chan error, 4), hold: hold}
}

func (w *waitingHandler) handle(ctx context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
	w.entered <- b.Len()
	select {
	case <-ctx.Done():
		w.left <- ctx.Err()
	case <-time.After(w.hold):
		w.left <- nil
	}
	return answerTargets(b)
}

func rstStream(c *h2client, id uint32) {
	c.frame(h2Frame{Type: frameRSTStream, StreamID: id, Payload: []byte{0, 0, 0, 0x8}}) // CANCEL
	c.flush()
}

// RFC 9113 §8.1: a client cancels a request by resetting its stream. The
// handler's context is cancelled once no request of its batch is left, and
// not before: one request given up on does not cost the others their answer.
func TestH2ClientResetCancelsTheContext(t *testing.T) {
	t.Run("the only request", func(t *testing.T) {
		w := newWaitingHandler(10 * time.Second)
		addr, _ := serveTest(t, Config{}, w.handle)
		c := dialH2(t, addr)
		c.request(1, "GET", "/slow")
		c.flush()
		<-w.entered
		rstStream(c, 1)
		if err := <-w.left; !errors.Is(err, context.Canceled) {
			t.Fatalf("after RST_STREAM on the batch's one stream: %v, want context.Canceled", err)
		}
	})

	t.Run("one of two", func(t *testing.T) {
		w := newWaitingHandler(10 * time.Second)
		addr, _ := serveTest(t, Config{}, w.handle)
		c := dialH2(t, addr)
		c.request(1, "GET", "/a")
		c.request(3, "GET", "/b")
		c.flush()
		if n := <-w.entered; n != 2 {
			t.Fatalf("the two requests reached the handler in a batch of %d, want one batch of 2", n)
		}
		rstStream(c, 1)
		select {
		case err := <-w.left:
			t.Fatalf("the context ended (%v) with stream 3 still waiting", err)
		case <-time.After(300 * time.Millisecond):
		}
		rstStream(c, 3)
		if err := <-w.left; !errors.Is(err, context.Canceled) {
			t.Fatalf("after both streams were reset: %v, want context.Canceled", err)
		}
	})

	t.Run("the connection closes", func(t *testing.T) {
		w := newWaitingHandler(10 * time.Second)
		addr, _ := serveTest(t, Config{}, w.handle)
		c := dialH2(t, addr)
		c.request(1, "GET", "/slow")
		c.flush()
		<-w.entered
		_ = c.conn.Close()
		if err := <-w.left; !errors.Is(err, context.Canceled) {
			t.Fatalf("after the connection closed: %v, want context.Canceled", err)
		}
	})
}

// RFC 9114 §4.1.1: on HTTP/3 a client gives a request up with STOP_SENDING
// or RESET_STREAM on its stream, and the handler's context learns it.
func TestH3ClientCancelCancelsTheContext(t *testing.T) {
	serverPC, clientPC := dgram.Pair()
	defer func() { _ = serverPC.Close() }()
	defer func() { _ = clientPC.Close() }()
	w := newWaitingHandler(10 * time.Second)
	served := make(chan struct{})
	go func() {
		defer close(served)
		conn, err := acceptOne(t, serverPC, h3ServerTLS(t), quic.DefaultParameters())
		if err != nil {
			return
		}
		_ = ServeH3Conn(t.Context(), conn, Config{}, w.handle)
	}()
	conn, err := quic.Dial(t.Context(), clientPC, serverPC.LocalAddr(), h3ClientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close(); <-served }()
	st, err := conn.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.WriteFinal(h3Request("GET", "/slow")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-w.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the handler was never reached")
	}
	if err := st.StopSending(h3RequestCancelled); err != nil {
		t.Fatal(err)
	}
	if err := <-w.left; !errors.Is(err, context.Canceled) {
		t.Fatalf("after STOP_SENDING on the request stream: %v, want context.Canceled", err)
	}
}
