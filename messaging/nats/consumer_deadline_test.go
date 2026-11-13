package nats

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

// pingServer is a server that opens with INFO and answers PING; it reads and
// drops everything else.
func pingServer(t *testing.T) DialFunc {
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

// Every call sets the connection's deadline from its own context. The SUB
// that starts Messages did not, and wrote under whatever deadline the call
// before it left: after a Connect under a context with a deadline — the
// ordinary way to bound connecting — the SUB failed once that deadline had
// passed, and broke the connection. The control has a Publish in between,
// which sets its own.
func TestSubscribeSetsItsOwnDeadline(t *testing.T) {
	for _, publishFirst := range []bool{true, false} {
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		c, err := Connect(ctx, pingServer(t), Config{})
		if err != nil {
			cancel()
			t.Fatalf("Connect: %v", err)
		}
		<-ctx.Done()
		cancel()
		time.Sleep(50 * time.Millisecond)
		if publishFirst {
			if err := c.Publish(t.Context(), []Message{{Subject: "x", Data: []byte("x")}}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
		}
		k, err := c.Consume(ConsumerConfig{Stream: "S", Consumer: "C"})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := k.subscribe(t.Context()); err != nil {
			t.Errorf("publish first %v: subscribe after Connect's deadline passed: %v", publishFirst, err)
		}
		_ = c.Close()
	}
}
