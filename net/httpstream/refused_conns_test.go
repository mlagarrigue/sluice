package httpstream

import (
	"bufio"
	"net"
	"strings"
	"testing"
	"time"
)

// A Server says how many connections its bounds turned away: MaxConns+1
// connections leave one refused at MaxConns, and from one address past
// MaxConnsPerIP, one refused there. The operator who sets a bound can read
// what it costs, where before the refused connection left no trace.
func TestServerCountsRefusedConnections(t *testing.T) {
	for _, tc := range []struct {
		name          string
		cfg           Config
		wantMax, want uint64
	}{
		{"MaxConns", Config{MaxConns: 2}, 1, 0},
		{"MaxConnsPerIP", Config{MaxConns: 10, MaxConnsPerIP: 2}, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			srv := &Server{Config: tc.cfg, Handler: echoTarget}
			done := make(chan error, 1)
			go func() { done <- srv.Serve(ln) }()
			defer func() { _ = srv.Close(); <-done }()

			// Two served connections, held open.
			for i := range 2 {
				c := dial(t, ln.Addr().String())
				if _, err := c.Write([]byte("GET /x HTTP/1.1\r\nHost: h\r\n\r\n")); err != nil {
					t.Fatal(err)
				}
				if status, _ := readResponse(t, bufio.NewReader(c)); !strings.HasPrefix(status, "HTTP/1.1 200") {
					t.Fatalf("connection %d under the bound: status %q", i+1, status)
				}
			}
			third := dial(t, ln.Addr().String())
			_ = third.SetReadDeadline(time.Now().Add(5 * time.Second))
			if n, err := third.Read(make([]byte, 1)); n != 0 || err == nil {
				t.Fatalf("the third connection read %d bytes, err %v: it was served past the bound", n, err)
			}
			// The refusal is counted before the close the client just saw.
			if gotMax, got := srv.RefusedConns(); gotMax != tc.wantMax || got != tc.want {
				t.Errorf("RefusedConns = %d, %d; want %d, %d", gotMax, got, tc.wantMax, tc.want)
			}
		})
	}
	if m, p := (&Server{}).RefusedConns(); m != 0 || p != 0 {
		t.Errorf("a server that never served: RefusedConns = %d, %d", m, p)
	}
}
