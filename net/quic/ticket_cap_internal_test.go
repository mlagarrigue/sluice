package quic

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/dgram"
)

// puttingCache counts the sessions a client stores.
type puttingCache struct {
	mu   sync.Mutex
	puts int
}

func (c *puttingCache) Get(string) (*tls.ClientSessionState, bool) { return nil, false }

func (c *puttingCache) Put(_ string, cs *tls.ClientSessionState) {
	if cs != nil {
		c.mu.Lock()
		c.puts++
		c.mu.Unlock()
	}
}

func (c *puttingCache) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.puts
}

// newSessionTicket is a TLS 1.3 NewSessionTicket message (RFC 9846
// §4.7.1): lifetime, age_add, nonce, an opaque ticket, no extensions. The
// client stores the ticket without reading inside it.
func newSessionTicket(i byte) []byte {
	label := make([]byte, 16)
	label[0] = i
	body := binary.BigEndian.AppendUint32(nil, 3600) // ticket_lifetime
	body = binary.BigEndian.AppendUint32(body, 0x01020304)
	body = append(body, 1, i) // ticket_nonce
	body = binary.BigEndian.AppendUint16(body, uint16(len(label)))
	body = append(body, label...)
	body = binary.BigEndian.AppendUint16(body, 0) // extensions
	msg := []byte{4, 0, byte(len(body) >> 8), byte(len(body))}
	return append(msg, body...)
}

// A server may send any number of tickets on one connection (RFC 9846
// §4.7.1), and each one a client stores carries a copy of the server's
// transport parameters. crypto/tls sends one ticket per connection, so the
// test server writes its NewSessionTicket messages itself, as CRYPTO data
// at the application level. The client stores maxStoredTickets of them
// and drops the rest.
func TestAClientStoresABoundedNumberOfTickets(t *testing.T) {
	const sent = 12 // crypto/tls closes the connection past 16
	serverPC, clientPC := dgram.Pair()
	defer serverPC.Close()
	defer clientPC.Close()

	accepted := make(chan *Conn, 1)
	go func() {
		buf := make([]byte, 2048)
		_ = serverPC.SetReadDeadline(time.Now().Add(10 * time.Second))
		n, peer, err := serverPC.ReadFrom(buf)
		if err != nil {
			close(accepted)
			return
		}
		conn, err := Accept(serverPC, datagram{Peer: peer, Payload: buf[:n]}, ServerTLSForTest(t), DefaultConfig())
		if err != nil {
			close(accepted)
			return
		}
		accepted <- conn
	}()
	cache := &puttingCache{}
	clientCfg := ClientTLSForTest()
	clientCfg.ClientSessionCache = cache
	client, err := Dial(context.Background(), clientPC, serverPC.LocalAddr(), clientCfg, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server, ok := <-accepted
	if !ok {
		t.Fatal("the server did not accept")
	}
	defer server.Close()
	waitFor(t, "the client to confirm", func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return client.handshakeConfirmed
	})

	server.mu.Lock()
	start := server.spaces[spaceApplication].cryptoOff
	server.mu.Unlock()
	var tickets []byte
	for i := range sent {
		tickets = append(tickets, newSessionTicket(byte(i))...)
	}
	if err := server.sendCrypto(tls.QUICEncryptionLevelApplication, tickets); err != nil {
		t.Fatal(err)
	}
	end := start + uint64(len(tickets))
	waitFor(t, "the client to read every ticket", func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return client.spaces[spaceApplication].crypto.consumed >= end
	})
	// The client reads datagrams one at a time, storing a datagram's tickets
	// before it reads the next: a stream arriving after the last ticket
	// was read says every ticket has been seen.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := server.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteFinal([]byte("x")); err != nil {
		t.Fatal(err)
	}
	r, err := client.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(r); err != nil {
		t.Fatal(err)
	}

	if got := cache.count(); got != maxStoredTickets {
		t.Fatalf("the client stored %d of %d tickets, want %d", got, sent, maxStoredTickets)
	}
}
