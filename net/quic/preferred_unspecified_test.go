package quic

import (
	"context"
	"net"
	"net/netip"
	"runtime"
	"testing"
	"time"
)

// A preferred-address socket bound to the unspecified address, announced
// as 127.0.0.2 through PreferredIPv4, answers the client's probe from
// 127.0.0.2 — the address the client probed and the only one it takes the
// answer from (RFC 9000 §8.2.2) — so the client moves there. The socket
// was read without its datagrams' local address and written without a
// source: the answer left from the address the route picks toward the
// client, 127.0.0.1, the client's validation failed in silence, and it
// stayed on the original address. The main socket had that already
// (TestListenerAnswersFromTheAddressDialled). Skipped where 127.0.0.2 is
// not a local address, or packet information is not wired.
func TestAPreferredSocketOnTheUnspecifiedAddressAnswersFromTheAnnouncedOne(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("packet information is wired on Linux only")
	}
	probe, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 2)})
	if err != nil {
		t.Skipf("no 127.0.0.2 on this host: %v", err)
	}
	_ = probe.Close()
	main, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	pref, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero})
	if err != nil {
		_ = main.Close()
		t.Fatal(err)
	}
	announced := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.2"), uint16(pref.LocalAddr().(*net.UDPAddr).Port)) //nolint:gosec // G115: a port
	l, err := NewListener(main, ServerTLSForTest(t), DefaultConfig(), ListenerConfig{PreferredSocket: pref, PreferredIPv4: announced.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	accepted := make(chan *Conn, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- c
	}()
	cpc, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer cpc.Close()
	client, err := Dial(context.Background(), cpc, main.LocalAddr(), ClientTLSForTest(), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	select {
	case _, ok := <-accepted:
		if !ok {
			t.Fatal("Accept failed")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the handshake never completed")
	}
	want := net.UDPAddrFromAddrPort(announced)
	if got := client.preferredAddress(); !sameAddr(got, want) {
		t.Fatalf("the client learned preferred address %v, want %v", got, want)
	}
	waitFor(t, "the client to validate and adopt the preferred path", func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return sameAddr(client.peer, want) && !client.pathProbe.pending
	})
}
