package quic

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"fmt"
)

// checkTLSConfig refuses, before anything is written, a tls.Config that
// crypto/tls or a conformant peer would refuse mid-handshake. Dial, Accept
// and NewListener all pass here, so a hand-built Config fails once, with an
// error naming the field, instead of failing every handshake: a Listener
// whose Config is refused by crypto/tls answers each client with a
// CONNECTION_CLOSE that says nothing about why.
func checkTLSConfig(cfg *tls.Config) error {
	if err := requireALPN(cfg); err != nil {
		return err
	}
	return requireTLS13(cfg)
}

// checkOwnCertificates refuses a cfg whose configured chains TLS 1.3
// forbids this end to send, before any packet goes out or is answered with
// them: a leaf whose Key Usage extension lacks digitalSignature (RFC 9846
// §4.5.1.2) and a chain member signed with SHA-1 (§4.3.3), the self-signed
// anchor excepted since no peer verifies its signature. The rules hold for
// a client's chain under mutual authentication as for a server's. crypto/tls
// reads neither, and a conformant peer refuses the chain mid-handshake,
// where this end can only close and the operator learns nothing of the
// cause. What GetCertificate, GetClientCertificate or GetConfigForClient
// return is not seen here, nor is the peer's signature_algorithms_cert,
// which only crypto/tls sees. A
// certificate that does not parse is left to crypto/tls: this is the
// operator's configuration, not bytes from the network, so one parser
// reporting it is enough.
func checkOwnCertificates(cfg *tls.Config) error {
	for i := range cfg.Certificates {
		cert := &cfg.Certificates[i]
		for j, der := range cert.Certificate {
			c := cert.Leaf
			if j > 0 || c == nil {
				var err error
				if c, err = x509.ParseCertificate(der); err != nil {
					continue
				}
			}
			if err := checkServedCertificate(c, j == 0); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkServedCertificate is checkOwnCertificates's rule for one
// certificate of a chain, leaf saying whether it is the first.
func checkServedCertificate(c *x509.Certificate, leaf bool) error {
	if leaf && c.KeyUsage != 0 && c.KeyUsage&x509.KeyUsageDigitalSignature == 0 {
		return fmt.Errorf("%w: certificate %q has a Key Usage without digitalSignature (RFC 9846 §4.5.1.2)", ErrQUIC, c.Subject)
	}
	switch c.SignatureAlgorithm {
	case x509.SHA1WithRSA, x509.DSAWithSHA1, x509.ECDSAWithSHA1:
		// A self-issued member above the leaf is the anchor, whose
		// signature no client verifies (nor does crypto/x509, which
		// refuses SHA-1 outright — so issuer and subject tell, not a
		// verification).
		if leaf || !bytes.Equal(c.RawIssuer, c.RawSubject) {
			return fmt.Errorf("%w: certificate %q is signed with SHA-1 (RFC 9846 §4.3.3)", ErrQUIC, c.Subject)
		}
	}
	return nil
}

// clientTLSConfig is cfg as a client handshake uses it: the caller's own
// when it carries no session cache, otherwise a copy whose cache hands each
// ticket out once (singleUseSessionCache). The copy leaves the caller's
// Config untouched — it may be shared with other dialers, or with a TCP
// listener.
func clientTLSConfig(cfg *tls.Config) *tls.Config {
	if cfg.ClientSessionCache == nil {
		return cfg
	}
	c := cfg.Clone()
	c.ClientSessionCache = singleUseSessionCache{inner: cfg.ClientSessionCache}
	return c
}

// singleUseSessionCache hands each session out once: RFC 9001 §4.5 and RFC
// 8446 Appendix C.4 have a client not reuse a ticket, since a ticket seen
// twice on the path ties the two connections together for any observer.
// crypto/tls's own LRU cache keeps an entry across any number of Gets, so
// the entry is removed as it is taken — Put with a nil session is the
// interface's way to remove — and the ticket the server issues on the
// resumed connection puts the next one in. An inner cache that ignores the
// removal reuses tickets, which is then its caller's decision. Safe for
// concurrent use when the inner cache is, as the interface requires.
type singleUseSessionCache struct{ inner tls.ClientSessionCache }

func (c singleUseSessionCache) Get(key string) (*tls.ClientSessionState, bool) {
	cs, ok := c.inner.Get(key)
	if !ok {
		return nil, false
	}
	c.inner.Put(key, nil)
	return sessionCopy(cs), true
}

// sessionCopy is cs over a copy of its session: the connection that resumes
// from it may decline the ticket's 0-RTT (handleTLSEvent, under Dial) by
// clearing SessionState.EarlyData, and the session a cache that ignores
// removals keeps must not lose its 0-RTT to that. A session that cannot be
// read back is handed out as it is.
func sessionCopy(cs *tls.ClientSessionState) *tls.ClientSessionState {
	ticket, state, err := cs.ResumptionState()
	if err != nil || state == nil {
		return cs
	}
	own := *state
	if cp, err := tls.NewResumptionState(ticket, &own); err == nil {
		return cp
	}
	return cs
}

func (c singleUseSessionCache) Put(key string, cs *tls.ClientSessionState) { c.inner.Put(key, cs) }

// requireALPN refuses a tls.Config that names no application protocol. QUIC
// mandates ALPN (RFC 9001 §8.1): against a conformant peer the handshake
// would fail anyway, mid-flight, with a bare no_application_protocol alert —
// refusing at construction turns that interoperability trap into an error
// that names the missing field.
func requireALPN(cfg *tls.Config) error {
	if cfg == nil || len(cfg.NextProtos) == 0 {
		return fmt.Errorf("%w: tls.Config.NextProtos is empty — RFC 9001 §8.1 requires ALPN "+
			"(a conformant peer ends such a handshake with no_application_protocol)", ErrQUIC)
	}
	return nil
}

// requireTLS13 refuses a tls.Config whose MinVersion is below TLS 1.3 — the
// zero value of a Config built by hand included. QUIC uses TLS 1.3 only
// (RFC 9001 §4.2), and crypto/tls enforces it when the handshake starts
// ([tls.QUICConn.Start]), where a Listener can only close the connection
// and the client learns nothing of the cause. The Config is refused, not
// corrected: it is the caller's, and may be shared with a TCP listener
// that still wants TLS 1.2.
func requireTLS13(cfg *tls.Config) error {
	if cfg.MinVersion < tls.VersionTLS13 {
		return fmt.Errorf("%w: tls.Config.MinVersion must be tls.VersionTLS13 — QUIC uses TLS 1.3 only "+
			"(RFC 9001 §4.2), and crypto/tls refuses a lower minimum at handshake", ErrQUIC)
	}
	return nil
}
