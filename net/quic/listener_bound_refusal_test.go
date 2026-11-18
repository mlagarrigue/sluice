package quic

import (
	"net"
	"sync/atomic"
	"testing"
)

// countingHole is a blackHole that counts what is written through it.
type countingHole struct {
	*blackHole
	writes atomic.Int32
}

func (c *countingHole) WriteTo(p []byte, to net.Addr) (int, error) {
	c.writes.Add(1)
	return c.blackHole.WriteTo(p, to)
}

// A direct attempt bootstrapConn refuses at the address's bound — a
// handshake proved the address's last place after admission read the bound
// — is told CONNECTION_REFUSED, as an Initial refused at admission is (RFC
// 9000 §5.2.2). The connection built for it pays the amplification limit
// with nothing received yet, so the close abort queued was held, and the
// client heard nothing until its handshake timeout. The witness is the
// admission's own refusal of the same address.
func TestABootstrapRefusalAtTheBoundIsTold(t *testing.T) {
	spc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(spc, ServerTLSForTest(t), DefaultConfig(), ListenerConfig{MaxConnsPerIP: 1})
	if err != nil {
		_ = spc.Close()
		t.Fatal(err)
	}
	defer l.Close()
	from := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4000}
	l.mu.Lock()
	l.connsByIP[l.connKey(from)] = 1 // one proven connection at the address
	l.mu.Unlock()

	adm := &countingHole{blackHole: newBlackHole()}
	defer adm.Close()
	l.dispatch(sealedClientInitial(t, []byte("dcid-bound-0001"), []byte("scid-1")), from, adm, 0)
	if n := adm.writes.Load(); n != 1 {
		t.Fatalf("witness: admission wrote %d datagrams for an address at its bound, want its CONNECTION_REFUSED", n)
	}

	dg := sealedClientInitial(t, []byte("dcid-bound-0002"), []byte("scid-2"))
	h, _, err := parseLongHeader(dg)
	if err != nil {
		t.Fatal(err)
	}
	keys, ok := initialPacketDecrypts(h)
	if !ok {
		t.Fatal("setup: the Initial does not decrypt")
	}
	boot := &countingHole{blackHole: newBlackHole()}
	defer boot.Close()
	l.bootstrapConn(dg, from, h, nil, &keys, boot, 0)
	if n := boot.writes.Load(); n != 1 {
		t.Errorf("bootstrapConn refused a direct attempt at the bound with %d datagrams, want one CONNECTION_REFUSED", n)
	}
}
