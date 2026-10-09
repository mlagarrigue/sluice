package udp_test

import (
	"errors"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/net/quic/udp"
)

// supported lists the platforms whose configure is not the fallback. A
// test running elsewhere expects errors.ErrUnsupported and nothing else.
func supported() bool {
	switch runtime.GOOS {
	case "linux", "darwin", "windows":
		return true
	}
	return false
}

// A configured socket is still an ordinary UDP socket: two of them exchange
// a datagram on loopback. This is the test every platform runs, and the one
// that would catch an option that breaks the socket outright.
func TestListenExchangesDatagrams(t *testing.T) {
	for _, tc := range []struct{ network, addr string }{
		{"udp4", "127.0.0.1:0"},
		{"udp6", "[::1]:0"},
		{"udp", "127.0.0.1:0"},
	} {
		t.Run(tc.network, func(t *testing.T) {
			a, err := udp.Listen(tc.network, tc.addr)
			if err != nil {
				if !supported() && errors.Is(err, errors.ErrUnsupported) {
					t.Skipf("no DF option on %s: %v", runtime.GOOS, err)
				}
				if tc.network == "udp6" {
					t.Skipf("no IPv6 loopback here: %v", err)
				}
				t.Fatalf("Listen: %v", err)
			}
			defer a.Close()
			b, err := udp.Listen(tc.network, tc.addr)
			if err != nil {
				t.Fatalf("Listen: %v", err)
			}
			defer b.Close()

			msg := []byte("hello")
			if _, err := a.WriteTo(msg, b.LocalAddr()); err != nil {
				t.Fatalf("WriteTo: %v", err)
			}
			b.SetReadDeadline(time.Now().Add(2 * time.Second))
			buf := make([]byte, 64)
			n, from, err := b.ReadFrom(buf)
			if err != nil {
				t.Fatalf("ReadFrom: %v", err)
			}
			if string(buf[:n]) != "hello" {
				t.Fatalf("got %q", buf[:n])
			}
			if from.(*net.UDPAddr).Port != a.LocalAddr().(*net.UDPAddr).Port {
				t.Fatalf("datagram from %v, want %v", from, a.LocalAddr())
			}
		})
	}
}

// Configure applies to a socket the caller opened themselves, which is the
// path for anyone who already has a *net.UDPConn from elsewhere.
func TestConfigureExistingSocket(t *testing.T) {
	c, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	err = udp.Configure(c)
	switch {
	case supported() && err != nil:
		t.Fatalf("Configure: %v", err)
	case !supported() && !errors.Is(err, errors.ErrUnsupported):
		t.Fatalf("Configure on %s: got %v, want errors.ErrUnsupported", runtime.GOOS, err)
	}
}

// A network that is not UDP never yields a socket.
func TestListenRejectsNonUDP(t *testing.T) {
	if c, err := udp.Listen("tcp", "127.0.0.1:0"); err == nil {
		c.Close()
		t.Fatal("Listen(\"tcp\") returned a socket")
	}
}
