package quic

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// The peer's close reads with its reason when it gave one, without when it
// did not.
func TestConnectionErrorText(t *testing.T) {
	e := &ConnectionError{Code: 0x10}
	if got, want := e.Error(), "quic: the peer closed the connection: transport code 0x10"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
	e = &ConnectionError{Code: 0x10, Application: true, Reason: "bye"}
	if got, want := e.Error(), `quic: the peer closed the connection: application code 0x10 "bye"`; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

// 2^62-1 is a code a frame carries.
func TestCheckCodeAtTheVarintBound(t *testing.T) {
	if err := checkCode("Close", varint.Max); err != nil {
		t.Errorf("checkCode(2^62-1) = %v", err)
	}
	if err := checkCode("Close", varint.Max+1); err == nil {
		t.Error("checkCode(2^62) accepted")
	}
}

type sinkPC struct {
	net.PacketConn
	writes int
}

func (p *sinkPC) WriteTo(b []byte, _ net.Addr) (int, error) {
	p.writes++
	return len(b), nil
}

// A closing connection answers the 1st, 2nd and 4th datagram, each time
// within three times what it received — the bound itself included — and
// not past it.
func TestClosingAnswersAtItsCountsAndBudget(t *testing.T) {
	peer := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	closing := func(pc net.PacketConn) *Conn {
		c := &Conn{}
		c.closing.active.Store(true)
		c.closing.datagrams = [][]byte{make([]byte, 12)}
		c.closing.via, c.closing.to, c.closing.nextReply = pc, peer, 1
		return c
	}

	pc := &sinkPC{}
	c := closing(pc)
	var got []int
	for range 4 {
		c.answerClosing(make([]byte, 4), peer)
		got = append(got, pc.writes)
	}
	if !slices.Equal(got, []int{1, 2, 2, 3}) {
		t.Errorf("closes sent after each of four 4-byte datagrams: %v, want [1 2 2 3]", got)
	}

	pc = &sinkPC{}
	c = closing(pc)
	c.answerClosing(make([]byte, 3), peer)
	if pc.writes != 0 {
		t.Error("a 12-byte close answered a 3-byte datagram")
	}
}

// A handshake that outlives the budget this package chose ends as a
// timeout, not as a context abandoned.
func TestHandshakePastItsBudget(t *testing.T) {
	silent := loopbackSocket(t)
	c, err := newClientConn(context.Background(), loopbackSocket(t), silent.LocalAddr(), ClientTLSForTest(), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	c.handshakeBudget = 300 * time.Millisecond
	err = c.handshake(context.Background())
	// The deadline is caught at the top of the loop or by the read waiting
	// on it, whichever comes first.
	timeout := err != nil && (err.Error() == fmt.Sprintf("%v: the handshake did not complete", ErrQUIC) ||
		errors.Is(err, os.ErrDeadlineExceeded))
	if !timeout || strings.Contains(err.Error(), "abandoned") {
		t.Errorf("handshake = %v, want a timeout", err)
	}
}

// A ServerHello is a HelloRetryRequest when its Random is the fixed one,
// read from 38 bytes on; a shorter start is not one.
func TestHelloRetryRequestAtItsLength(t *testing.T) {
	b := append([]byte{2, 0, 0, 0, 3, 3}, helloRetryRequestRandom...)
	if !isHelloRetryRequest(b) {
		t.Error("38 bytes of HelloRetryRequest not recognised")
	}
	if isHelloRetryRequest(b[:len(b)-1]) || isHelloRetryRequest(b[:3]) {
		t.Error("a short start read as a HelloRetryRequest")
	}
}

// takingCache removes what Get returns, as some caches do.
type takingCache map[string]*tls.ClientSessionState

func (c takingCache) Get(k string) (*tls.ClientSessionState, bool) {
	cs, ok := c[k]
	delete(c, k)
	return cs, ok
}

func (c takingCache) Put(k string, cs *tls.ClientSessionState) {
	if cs == nil {
		delete(c, k)
		return
	}
	c[k] = cs
}

func earlySession(t *testing.T, group tls.CurveID) *tls.ClientSessionState {
	t.Helper()
	cs, err := tls.NewResumptionState([]byte("ticket"), &tls.SessionState{EarlyData: true, Extra: [][]byte{groupEntry(group)}})
	if err != nil {
		t.Fatal(err)
	}
	return cs
}

// DialEarly offers the remembered group when the caller named none, puts
// back the ticket it looked at, and keeps the caller's groups when they
// leave the remembered one out.
func TestEarlyConfigOffersTheRememberedGroup(t *testing.T) {
	cache := takingCache{"h": earlySession(t, tls.X25519)}
	cfg := &tls.Config{ServerName: "h", ClientSessionCache: cache}
	got := earlyTLSConfig(cfg)
	if got == cfg || !slices.Equal(got.CurvePreferences, []tls.CurveID{tls.X25519}) {
		t.Errorf("with no groups named: %v", got.CurvePreferences)
	}
	if _, ok := cache["h"]; !ok {
		t.Error("the ticket looked at was not put back")
	}
	cfg.CurvePreferences = []tls.CurveID{tls.CurveP256}
	if got := earlyTLSConfig(cfg); got != cfg {
		t.Errorf("a group the caller left out was offered: %v", got.CurvePreferences)
	}
}

// A certificate's parsed Leaf is the one checked, as crypto/tls uses it.
func TestOwnCertificateCheckReadsTheLeaf(t *testing.T) {
	cfg := &tls.Config{Certificates: []tls.Certificate{{
		Certificate: [][]byte{[]byte("not DER")},
		Leaf:        &x509.Certificate{KeyUsage: x509.KeyUsageCertSign},
	}}}
	if err := checkOwnCertificates(cfg); err == nil {
		t.Error("a leaf without digitalSignature passed")
	}
}

// keepingCache ignores removals.
type keepingCache struct{ cs *tls.ClientSessionState }

func (c *keepingCache) Get(string) (*tls.ClientSessionState, bool) { return c.cs, c.cs != nil }
func (c *keepingCache) Put(_ string, cs *tls.ClientSessionState) {
	if cs != nil {
		c.cs = cs
	}
}

// What the single-use cache hands out is a copy: a connection that clears
// its 0-RTT leaves the session a removal-ignoring cache keeps untouched.
func TestSingleUseCacheHandsOutACopy(t *testing.T) {
	inner := &keepingCache{cs: earlySession(t, tls.X25519)}
	got, ok := singleUseSessionCache{inner: inner}.Get("h")
	if !ok {
		t.Fatal("no session")
	}
	_, st, _ := got.ResumptionState()
	st.EarlyData = false
	if _, kept, _ := inner.cs.ResumptionState(); !kept.EarlyData {
		t.Error("clearing the copy's 0-RTT cleared the cached session's")
	}
}

// A config GetConfigForClient returns at TLS 1.3 already is used as it is:
// its live session ticket keys, not a copy's.
func TestServerConfigKeepsATLS13Answer(t *testing.T) {
	own := &tls.Config{MinVersion: tls.VersionTLS13}
	c := serverTLSConfig(&tls.Config{GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) { return own, nil }})
	if got, err := c.GetConfigForClient(nil); err != nil || got != own {
		t.Errorf("GetConfigForClient = %p, %v; want the TLS 1.3 config itself", got, err)
	}
}

// A chain is over at exactly seven days.
func TestTicketChainEndsAtItsBound(t *testing.T) {
	now := time.Now()
	if _, ok := (&Conn{ticketChainOrigin: now.Add(-maxTicketChain)}).ticketOptions(now); ok {
		t.Error("a ticket issued at the chain's seven days")
	}
	if _, ok := (&Conn{ticketChainOrigin: now.Add(-maxTicketChain + time.Nanosecond)}).ticketOptions(now); !ok {
		t.Error("no ticket a nanosecond before the chain's end")
	}
}

// An entry forgotten frees its slot, even when it was the oldest.
func TestSpentTokenForgottenFreesItsSlot(t *testing.T) {
	s := spentTokens{ttl: time.Minute, max: 1, failClosed: true}
	now := time.Now()
	if !s.consumeUntil([]byte("a"), now, now.Add(time.Minute)) {
		t.Fatal("first entry refused")
	}
	s.forget([]byte("a"))
	if !s.consumeUntil([]byte("b"), now, now.Add(time.Minute)) {
		t.Error("the slot of a forgotten entry stayed taken")
	}
}

// A token that is a valid MAC over nothing was issued here and is short,
// not foreign.
func TestRetryTokenThatIsItsMACAlone(t *testing.T) {
	key := [32]byte{1}
	mac := hmac.New(sha256.New, key[:]).Sum(nil)
	_, err := verifyRetryToken(mac, key, time.Minute, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1})
	if !errors.Is(err, ErrTruncated) || errors.Is(err, errForeignToken) {
		t.Errorf("verifyRetryToken(MAC alone) = %v, want ErrTruncated", err)
	}
	if bytes.Equal(mac, nil) {
		t.Fatal("no MAC")
	}
}
