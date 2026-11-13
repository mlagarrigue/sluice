package quic

import (
	"net"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// RFC 9000 §9.5: "An endpoint MUST NOT reuse a connection ID when sending
// from more than one local address". A client at a new address Q probes
// both the server's original address and its preferred one, as §9.6.3 asks
// of a client that migrates before it has moved to the preferred address:
// the two PATH_RESPONSEs leave from two local sockets and must name two
// different peer identifiers, whichever challenge comes first.
func TestAnswersFromTwoSocketsUseTwoIdentifiers(t *testing.T) {
	for _, order := range []string{"main-first", "preferred-first"} {
		t.Run(order, func(t *testing.T) {
			c, main := newMigratableServer(t, "two-sockets-"+order)
			pref := &recordingPC{addr: fakeFuzzAddr("two-sockets-" + order + ":preferred")}
			now := time.Now()
			tok1, tok2 := make([]byte, 16), make([]byte, 16)
			tok2[0] = 1
			if err := c.frames(spaceApplication, ncidFrame(1, 0, []byte("cli-cid1"), tok1), now); err != nil {
				t.Fatal(err)
			}
			if err := c.frames(spaceApplication, ncidFrame(2, 0, []byte("cli-cid2"), tok2), now); err != nil {
				t.Fatal(err)
			}
			q := fakeFuzzAddr("two-sockets-" + order + ":peerQ")
			challenge := func(via net.PacketConn, localSeq uint64, b byte) {
				rxFromNewAddress(c, q, 1200, true)
				c.mu.Lock()
				c.rxVia, c.rxLocalCIDSeq = via, localSeq
				c.mu.Unlock()
				f := append(varint.Append(nil, framePathChallenge), b, b, b, b, b, b, b, b)
				if err := c.frames(spaceApplication, f, now); err != nil {
					t.Fatal(err)
				}
			}
			if order == "main-first" {
				challenge(main, 2, 1)
				challenge(pref, 1, 2)
			} else {
				challenge(pref, 1, 2)
				challenge(main, 2, 1)
			}
			mw, pw := main.recorded(), pref.recorded()
			if len(mw) == 0 || len(pw) == 0 {
				t.Fatalf("main sent %d, preferred sent %d datagrams; want an answer from each", len(mw), len(pw))
			}
			md, pd := string(mw[len(mw)-1].data[1:9]), string(pw[len(pw)-1].data[1:9])
			if md == pd {
				t.Fatalf("both sockets answered Q under %q", md)
			}
		})
	}
}
