package udp_test

import (
	"errors"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/net/quic/udp"
)

// A datagram sent with AppendECN's control message arrives, on a socket
// EnableECN was applied to, with the codepoint PacketECN reads back — on
// an AF_INET socket and on a dual-stack AF_INET6 one receiving IPv4, where
// the kernel reports the field through the IPv4 option.
func TestECNMarkIsReadBack(t *testing.T) {
	for _, tc := range []struct{ network, bind string }{
		{"udp4", "127.0.0.1:0"},
		{"udp", ":0"},
	} {
		t.Run(tc.network, func(t *testing.T) {
			srv, err := udp.Listen(tc.network, tc.bind)
			if err != nil {
				t.Fatal(err)
			}
			defer srv.Close()
			if err := udp.EnableECN(srv); err != nil {
				switch runtime.GOOS {
				case "linux", "darwin", "windows":
				default:
					if errors.Is(err, errors.ErrUnsupported) {
						t.Skip("ECN is read on Linux, Darwin and Windows only")
					}
				}
				t.Fatalf("EnableECN: %v", err)
			}
			cli, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer cli.Close()
			to := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: srv.LocalAddr().(*net.UDPAddr).Port}
			for _, want := range []uint8{2, 3, 1} {
				oob := udp.AppendECN(nil, want)
				if _, _, err := cli.WriteMsgUDPAddrPort([]byte{want}, oob, to.AddrPort()); err != nil {
					t.Fatalf("WriteMsgUDP with mark %d: %v", want, err)
				}
				_ = srv.SetReadDeadline(time.Now().Add(2 * time.Second))
				buf, roob := make([]byte, 16), make([]byte, udp.PacketInfoLen)
				n, oobn, _, _, err := srv.ReadMsgUDP(buf, roob)
				if err != nil {
					t.Fatalf("ReadMsgUDP: %v", err)
				}
				if n != 1 || buf[0] != want {
					t.Fatalf("payload %x, want %x", buf[:n], want)
				}
				got, ok := udp.PacketECN(roob[:oobn])
				if !ok || got != want {
					t.Fatalf("mark %d ok %v in %d control bytes, want %d", got, ok, oobn, want)
				}
			}
			// Unmarked stays unmarked: the field is reported, as Not-ECT.
			if _, err := cli.WriteTo([]byte{0}, to); err != nil {
				t.Fatal(err)
			}
			buf, roob := make([]byte, 16), make([]byte, udp.PacketInfoLen)
			_, oobn, _, _, err := srv.ReadMsgUDP(buf, roob)
			if err != nil {
				t.Fatal(err)
			}
			if got, ok := udp.PacketECN(roob[:oobn]); !ok || got != 0 {
				t.Fatalf("unmarked datagram read as %d ok %v", got, ok)
			}
		})
	}
}
