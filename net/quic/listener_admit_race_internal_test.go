package quic

import (
	"fmt"
	"net"
	"sync"
	"testing"
)

// A listener with a preferred address (RFC 9000 §9.6) reads two sockets, each
// on its own loop, and both take Initials for unknown identifiers. The
// admission read the bounds under l.mu, released it, built the connection,
// and took l.mu again to count it: two Initials arriving together, one per
// socket, both read the counts before either was counted, and both were
// admitted — one connection past MaxConnsPerIP, MaxConns or MaxPendingConns.
// Here two dispatches run at once from one address under MaxConnsPerIP 1, as
// the two loops would; a sequential second Initial is answered with a Retry,
// so one connection is admitted, never two. The rounds give the interleaving
// its chance: on the old code, two were admitted within the first few.
func TestTwoReadLoopsAdmitOneConnectionPerAddressBound(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  ListenerConfig
	}{
		{"max-conns-per-ip", ListenerConfig{MaxConnsPerIP: 1}},
		{"max-conns", ListenerConfig{MaxConns: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for round := range 200 {
				if n := admitTwoAtOnce(t, tc.cfg, round); n != 1 {
					t.Fatalf("round %d: %d connections admitted from two simultaneous Initials, want 1", round, n)
				}
			}
		})
	}
}

// admitTwoAtOnce hands two Initials from 127.0.0.1 to dispatch at the same
// instant and returns how many connections the listener then holds.
func admitTwoAtOnce(t *testing.T, cfg ListenerConfig, round int) int {
	t.Helper()
	spc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(spc, ServerTLSForTest(t), DefaultConfig(), cfg)
	if err != nil {
		_ = spc.Close()
		t.Fatal(err)
	}
	defer l.Close()
	bh := newBlackHole()
	defer bh.Close()

	var initials [2][]byte
	for i := range initials {
		initials[i] = sealedClientInitial(t, fmt.Appendf(nil, "dcid-%03d-%d", round, i), fmt.Appendf(nil, "scid-%d", i))
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range initials {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			l.dispatch(initials[i], &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1000 + i}, bh, 0)
		}()
	}
	close(start)
	wg.Wait()

	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.pendingConns)
}
