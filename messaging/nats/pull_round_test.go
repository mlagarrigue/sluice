package nats

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// pullServer is a JetStream stand-in over a pipe: INFO, PONG to every PING,
// and for every pull on $JS.API.CONSUMER.MSG.NEXT a call to onPull with the
// pull's number, the inbox's sid and the subject the answer goes to.
func pullServer(t *testing.T, onPull func(w net.Conn, n int, sid, inbox string)) DialFunc {
	t.Helper()
	return func(context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		t.Cleanup(func() { _ = server.Close() })
		go func() {
			_, _ = server.Write([]byte("INFO {\"headers\":true,\"max_payload\":1048576}\r\n"))
			br := bufio.NewReader(server)
			sids := map[string]string{}
			pulls := 0
			for {
				line, err := br.ReadString('\n')
				if err != nil {
					return
				}
				f := strings.Fields(line)
				if len(f) == 0 {
					continue
				}
				switch strings.ToUpper(f[0]) {
				case "PING":
					_, _ = server.Write([]byte("PONG\r\n"))
				case "SUB":
					sids[f[1]] = f[2]
				case "PUB":
					n, _ := strconv.Atoi(f[len(f)-1])
					if _, err := br.Discard(n + 2); err != nil {
						return
					}
					if strings.HasPrefix(f[1], "$JS.API.CONSUMER.MSG.NEXT.") && len(f) == 4 {
						pulls++
						onPull(server, pulls, sids[f[2]], f[2])
					}
				}
			}
		}()
		return client, nil
	}
}

func sendMsgs(w net.Conn, sid, inbox string, n int) {
	for range n {
		_, _ = fmt.Fprintf(w, "MSG S.x %s 2\r\nhi\r\n", sid)
	}
}

// A pull answered with exactly BatchMessages messages is complete: the round
// stops reading at the batch and hands it over. Read with <=, it waited for
// one message more, which the server — its request satisfied — never sends,
// until the round's deadline, and then broke the connection.
func TestAFullBatchIsDeliveredWithoutWaiting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dial := pullServer(t, func(w net.Conn, n int, sid, inbox string) { sendMsgs(w, sid, inbox, 2) })
		c, err := Connect(t.Context(), dial, Config{})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		k, err := c.Consume(ConsumerConfig{Stream: "S", Consumer: "C", BatchMessages: 2, Expires: 100 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		start := time.Now()
		src := k.Messages(ctx)
		got := 0
		for b := range src.Stream() {
			got += len(b.Items)
			cancel()
		}
		if err := src.Err(); err != nil {
			t.Fatalf("Err() = %v after a full batch, want nil", err)
		}
		if got != 2 {
			t.Fatalf("%d messages delivered, want the batch's 2", got)
		}
		if d := time.Since(start); d >= time.Second {
			t.Errorf("the batch took %v of the clock to arrive, the server answered at once", d)
		}
	})
}

// The round's I/O is bounded by Expires plus a margin: a server answers its
// 408 a little after Expires, never at the very instant, and that must not
// break the connection — without the margin the read's deadline passed
// first. The control is a server that never answers: the round ends with
// ErrConnBroken instead of waiting for ever.
func TestTheRoundOutlastsALateAnswerAndEndsOnASilentServer(t *testing.T) {
	const expires = 100 * time.Millisecond
	t.Run("late 408", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			dial := pullServer(t, func(w net.Conn, n int, sid, inbox string) {
				if n == 1 {
					time.Sleep(expires + 50*time.Millisecond)
					hdr := "NATS/1.0 408 Request Timeout\r\n\r\n"
					_, _ = fmt.Fprintf(w, "HMSG %s %s %d %d\r\n%s\r\n", inbox, sid, len(hdr), len(hdr), hdr)
					return
				}
				sendMsgs(w, sid, inbox, 1)
			})
			c, err := Connect(t.Context(), dial, Config{})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			defer time.Sleep(time.Second) // the server's answer, late, still has to leave
			k, _ := c.Consume(ConsumerConfig{Stream: "S", Consumer: "C", BatchMessages: 1, Expires: expires})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			src := k.Messages(ctx)
			got := 0
			for b := range src.Stream() {
				got += len(b.Items)
				cancel()
			}
			if err := src.Err(); err != nil || got != 1 {
				t.Fatalf("%d messages, Err() = %v; want the one message of the second pull and no error", got, err)
			}
		})
	})
	t.Run("silent server", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			dial := pullServer(t, func(net.Conn, int, string, string) {})
			c, err := Connect(t.Context(), dial, Config{})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			k, _ := c.Consume(ConsumerConfig{Stream: "S", Consumer: "C", BatchMessages: 1, Expires: expires})
			src := k.Messages(t.Context())
			for range src.Stream() {
				t.Fatal("a batch from a server that sent nothing")
			}
			if err := src.Err(); !errors.Is(err, ErrConnBroken) {
				t.Fatalf("Err() = %v, want ErrConnBroken once the round's bound passed", err)
			}
		})
	})
}
