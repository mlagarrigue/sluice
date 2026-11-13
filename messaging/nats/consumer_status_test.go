package nats

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// statusJetStream answers every pull on $JS.API.CONSUMER.MSG.NEXT with a
// status-only HMSG carrying statusLine, as nats-server's
// processNextMsgRequest does for a refused pull, and counts the pulls.
func statusJetStream(t *testing.T, statusLine string, pulls *atomic.Int64) DialFunc {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = conn.Write([]byte(`INFO {"headers":true,"max_payload":1048576}` + "\r\n"))
		br := bufio.NewReader(conn)
		sids := map[string]string{} // inbox -> sid
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
				_, _ = conn.Write([]byte("PONG\r\n"))
			case "SUB":
				sids[f[1]] = f[2]
			case "PUB":
				n, _ := strconv.Atoi(f[len(f)-1])
				if _, err := br.Discard(n + 2); err != nil {
					return
				}
				if strings.HasPrefix(f[1], "$JS.API.CONSUMER.MSG.NEXT.") && len(f) == 4 {
					pulls.Add(1)
					hdr := "NATS/1.0 " + statusLine + "\r\n\r\n"
					msg := fmt.Sprintf("HMSG %s %s %d %d\r\n%s\r\n", f[2], sids[f[2]], len(hdr), len(hdr), hdr)
					if _, err := conn.Write([]byte(msg)); err != nil {
						return
					}
				}
			}
		}
	}()
	return DialTCP(ln.Addr().String())
}

// pullsAgainst runs Messages for a second against a server that answers
// every pull with statusLine, and reports the pulls it drew and the error
// the stream ended with.
func pullsAgainst(t *testing.T, statusLine string) (int64, error) {
	t.Helper()
	var pulls atomic.Int64
	dial := statusJetStream(t, statusLine, &pulls)
	c, err := Connect(t.Context(), dial, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	k, err := c.Consume(ConsumerConfig{Stream: "S", Consumer: "C", BatchMessages: 2})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	src := k.Messages(ctx)
	batches := 0
	for range src.Stream() {
		batches++
	}
	if batches != 0 {
		t.Errorf("%q: %d batches, want 0", statusLine, batches)
	}
	return pulls.Load(), src.Err()
}

// A 409 for a consumer deleted ends the stream on the first pull.
func TestA409ConsumerDeletedEndsTheStream(t *testing.T) {
	n, err := pullsAgainst(t, "409 Consumer Deleted")
	var se *StatusError
	if n != 1 || !errors.As(err, &se) || se.Code != 409 {
		t.Fatalf("%d pulls, err %v; want 1 pull and a *StatusError 409", n, err)
	}
}

// A pull the server refuses on its own terms — a batch over the consumer's
// MaxRequestBatch, an expiry over MaxRequestExpires — draws the same 409 at
// every pull: it ends the stream with a *StatusError 409 after one, as
// ConsumerConfig.BatchMessages' godoc says, rather than pulling again and
// again, delivering nothing and reporting nothing.
func TestA409RefusingThePullEndsTheStream(t *testing.T) {
	for _, line := range []string{
		"409 Exceeded MaxRequestBatch of 1",
		"409 Exceeded MaxRequestExpires of 500ms",
	} {
		n, err := pullsAgainst(t, line)
		var se *StatusError
		if n != 1 || !errors.As(err, &se) || se.Code != 409 {
			t.Errorf("%q: %d pulls in 1s, Err() = %v; want 1 pull and a *StatusError 409", line, n, err)
		}
	}
}
