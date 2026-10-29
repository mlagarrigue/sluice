package quic

import (
	"crypto/tls"
	"testing"
)

// mapSessionCache is the smallest tls.ClientSessionCache: what crypto/tls's
// LRU does, minus the eviction, so a test can see what was left behind.
type mapSessionCache map[string]*tls.ClientSessionState

func (m mapSessionCache) Get(key string) (*tls.ClientSessionState, bool) {
	cs, ok := m[key]
	return cs, ok
}

func (m mapSessionCache) Put(key string, cs *tls.ClientSessionState) {
	if cs == nil {
		delete(m, key)
		return
	}
	m[key] = cs
}

// RFC 9001 §4.5, RFC 9846 Appendix C.4: a ticket is presented once. The
// wrapper hands a session out and removes it in the same Get; a miss
// touches nothing; a Put goes through so the server's next ticket lands.
func TestSingleUseSessionCacheHandsATicketOutOnce(t *testing.T) {
	inner := mapSessionCache{}
	cs := &tls.ClientSessionState{}
	inner.Put("example.com", cs)
	cache := singleUseSessionCache{inner: inner}

	if got, ok := cache.Get("example.com"); !ok || got != cs {
		t.Fatalf("Get = (%v, %v), want the stored session", got, ok)
	}
	if _, left := inner["example.com"]; left {
		t.Error("the session is still in the cache after being handed out; a second Dial would present the same ticket")
	}
	if _, ok := cache.Get("example.com"); ok {
		t.Error("a second Get found a session; the ticket would be reused")
	}
	if _, ok := cache.Get("other.example"); ok {
		t.Error("a miss returned a session")
	}
	next := &tls.ClientSessionState{}
	cache.Put("example.com", next)
	if got, ok := inner.Get("example.com"); !ok || got != next {
		t.Error("Put did not reach the inner cache; the server's next ticket would be lost")
	}
}

// Dial wraps the caller's cache on a copy of its Config, and leaves a Config
// without a cache alone.
func TestClientTLSConfigWrapsTheSessionCacheOnACopy(t *testing.T) {
	inner := mapSessionCache{}
	cfg := &tls.Config{NextProtos: []string{"t"}, MinVersion: tls.VersionTLS13, ClientSessionCache: inner}
	got := clientTLSConfig(cfg)
	if got == cfg {
		t.Fatal("the caller's Config was used in place; it must be copied before its cache is replaced")
	}
	w, ok := got.ClientSessionCache.(singleUseSessionCache)
	if !ok {
		t.Fatalf("ClientSessionCache = %T, want singleUseSessionCache", got.ClientSessionCache)
	}
	if w.inner.(mapSessionCache) == nil || cfg.ClientSessionCache.(mapSessionCache) == nil {
		t.Fatal("the caller's cache is not the one wrapped")
	}
	if got.NextProtos[0] != "t" || got.MinVersion != tls.VersionTLS13 {
		t.Error("the copy lost the caller's settings")
	}
	plain := &tls.Config{NextProtos: []string{"t"}, MinVersion: tls.VersionTLS13}
	if clientTLSConfig(plain) != plain {
		t.Error("a Config without a cache was copied for nothing")
	}
}
