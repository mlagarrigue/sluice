package nats

import (
	"bufio"
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

// outsideBatchServer opens with INFO, answers PING, and answers the SUB of a
// PublishAcked with one message on an index of its inbox no message of the
// batch has: "<inbox>5" for a batch of one.
func outsideBatchServer(t *testing.T) DialFunc {
	t.Helper()
	return func(context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		t.Cleanup(func() { _ = server.Close() })
		out := make(chan string, 16)
		go func() { // the writer, apart: net.Pipe has no buffer
			for s := range out {
				if _, err := server.Write([]byte(s)); err != nil {
					return
				}
			}
		}()
		go func() {
			defer close(out)
			out <- "INFO {\"headers\":true,\"max_payload\":1048576}\r\n"
			br := bufio.NewReader(server)
			for {
				line, err := br.ReadString('\n')
				if err != nil {
					return
				}
				f := strings.Fields(line)
				switch {
				case len(f) > 0 && f[0] == "PING":
					out <- "PONG\r\n"
				case len(f) == 3 && f[0] == "SUB" && strings.HasSuffix(f[1], "*"):
					// SUB <inbox>* <sid>
					out <- "MSG " + strings.TrimSuffix(f[1], "*") + "5 " + f[2] + " 0\r\n\r\n"
				}
			}
		}()
		return client, nil
	}
}

// A reply to a PublishAcked on an index outside the batch is ErrProtocol and a
// broken connection, not an index out of range (the architecture's S2: no
// panic crosses a public boundary while data flows). The index comes from the
// reply's subject, which the server writes, or any client publishing on the
// inbox; no test ran PublishAcked past its refusal of an oversized message,
// and the guard could go without the suite noticing.
func TestAPublishReplyOutsideTheBatchIsRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second) // a guard, not a measure
	defer cancel()
	c, err := Connect(ctx, outsideBatchServer(t), Config{})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = c.Close() }()

	_, err = c.PublishAcked(ctx, []Outbound{{Subject: "orders.new", Data: []byte("x")}})
	if !errors.Is(err, ErrProtocol) || !errors.Is(err, ErrConnBroken) {
		t.Errorf("a reply on index 5 of a batch of one: %v, want ErrProtocol and ErrConnBroken", err)
	}
}

// silentServer opens with INFO and answers PING; it reads everything else
// and answers none of it.
func silentServer(t *testing.T) DialFunc {
	t.Helper()
	return func(context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		t.Cleanup(func() { _ = server.Close() })
		go func() {
			_, _ = server.Write([]byte("INFO {\"headers\":true,\"max_payload\":1048576}\r\n"))
			br := bufio.NewReader(server)
			for {
				line, err := br.ReadString('\n')
				if err != nil {
					return
				}
				if strings.HasPrefix(line, "PING") {
					_, _ = server.Write([]byte("PONG\r\n"))
				}
			}
		}()
		return client, nil
	}
}

// Request and PublishAcked against a server that stops answering end with
// their context, deadline or cancellation, and break the connection (the
// architecture's S5: the connectors are bounded by the caller's context). No
// NATS call was run against a silent server, and the bound each takes from
// its context could go without the suite noticing.
func TestACallToASilentServerEndsWithItsContext(t *testing.T) {
	calls := map[string]func(context.Context, *Conn) error{
		"Request": func(ctx context.Context, c *Conn) error {
			_, err := c.Request(ctx, "orders.get", []byte("x"))
			return err
		},
		"PublishAcked": func(ctx context.Context, c *Conn) error {
			_, err := c.PublishAcked(ctx, []Outbound{{Subject: "orders.new", Data: []byte("x")}})
			return err
		},
	}
	for name, call := range calls {
		for _, how := range []string{"deadline", "cancel"} {
			c, err := Connect(t.Context(), silentServer(t), Config{})
			if err != nil {
				t.Fatalf("Connect: %v", err)
			}
			var ctx context.Context
			var cancel context.CancelFunc
			want := context.DeadlineExceeded
			if how == "deadline" {
				ctx, cancel = context.WithTimeout(t.Context(), 100*time.Millisecond)
			} else {
				ctx, cancel = context.WithCancel(t.Context())
				time.AfterFunc(100*time.Millisecond, cancel)
				want = context.Canceled
			}
			done := make(chan error, 1)
			go func() { done <- call(ctx, c) }()
			select {
			case err := <-done:
				if !errors.Is(err, want) || !errors.Is(err, ErrConnBroken) {
					t.Errorf("%s, %s: %v, want %v and ErrConnBroken", name, how, err, want)
				}
			case <-time.After(10 * time.Second): // a guard, not a measure
				t.Errorf("%s, %s: the call outlived its context by ten seconds: nothing bounds it", name, how)
				_ = c.nc.Close() // release the call
				<-done
			}
			cancel()
			_ = c.Close()
		}
	}
}

// A pull batch is bounded in messages only, and the documentation gives its
// memory with the defaults: BatchMessages times MaxPayloadBytes, 2 GiB. A
// default that moves moves the figure, and this says so.
func TestTheDefaultBatchHoldsAtMostTwoGiB(t *testing.T) {
	if got := DefaultBatchMessages * DefaultMaxPayloadBytes; got != 2<<30 {
		t.Errorf("DefaultBatchMessages × DefaultMaxPayloadBytes = %d, the documentation says 2 GiB", got)
	}
}
