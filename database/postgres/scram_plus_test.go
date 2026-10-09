package postgres

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// RFC 5929 §4.1: the certificate is hashed with its signature's hash, except
// that MD5 and SHA-1 are replaced by SHA-256. The expected values are computed
// here from the hash functions directly, not through the code under test.
func TestTLSServerEndPointHash(t *testing.T) {
	raw := []byte("not a real certificate, only its bytes")
	s256 := sha256.Sum256(raw)
	s384 := sha512.Sum384(raw)
	s512 := sha512.Sum512(raw)
	for _, tc := range []struct {
		alg  x509.SignatureAlgorithm
		want []byte
	}{
		{x509.MD5WithRSA, s256[:]},
		{x509.SHA1WithRSA, s256[:]},
		{x509.ECDSAWithSHA1, s256[:]},
		{x509.SHA256WithRSA, s256[:]},
		{x509.SHA256WithRSAPSS, s256[:]},
		{x509.ECDSAWithSHA256, s256[:]},
		{x509.SHA384WithRSA, s384[:]},
		{x509.SHA384WithRSAPSS, s384[:]},
		{x509.ECDSAWithSHA384, s384[:]},
		{x509.SHA512WithRSA, s512[:]},
		{x509.SHA512WithRSAPSS, s512[:]},
		{x509.ECDSAWithSHA512, s512[:]},
	} {
		got, err := tlsServerEndPoint(&x509.Certificate{Raw: raw, SignatureAlgorithm: tc.alg})
		if err != nil {
			t.Errorf("%v: %v", tc.alg, err)
			continue
		}
		if !bytes.Equal(got, tc.want) {
			t.Errorf("%v: binding = %x, want %x", tc.alg, got, tc.want)
		}
	}
	if _, err := tlsServerEndPoint(&x509.Certificate{Raw: raw, SignatureAlgorithm: x509.PureEd25519}); !errors.Is(err, ErrAuthUnsupported) {
		t.Errorf("Ed25519: %v, want ErrAuthUnsupported — the RFC defines no hash for it", err)
	}
}

// selfSigned returns a certificate for "db.test" signed by its own key, with
// the signature algorithm the key type implies.
func selfSigned(t *testing.T, key any, pub any) tls.Certificate {
	t.Helper()
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "db.test"},
		DNSNames:              []string{"db.test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

// scramPlusServer plays the server half of a SCRAM-SHA-256-PLUS exchange over
// a real TLS connection, and checks what the client bound and proved with
// values it computes itself: the channel binding from the certificate it
// serves, the proof from scramProof (pinned against RFC 7677 elsewhere).
func scramPlusServer(nc net.Conn, cert tls.Certificate, password string, wantCB []byte) error {
	defer nc.Close()
	srv := tls.Server(nc, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	rd := pgwire.NewReader(srv)
	if _, err := rd.NextStartup(); err != nil {
		return fmt.Errorf("startup: %w", err)
	}
	if _, err := srv.Write(beAuth(authSASL, mechSCRAMSHA256Plus+"\x00"+mechSCRAMSHA256+"\x00\x00")); err != nil {
		return err
	}
	m, err := rd.Next()
	if err != nil {
		return fmt.Errorf("initial response: %w", err)
	}
	mech, rest, ok := cstring(m.Body)
	if !ok || mech != mechSCRAMSHA256Plus {
		return fmt.Errorf("mechanism %q, want %s", mech, mechSCRAMSHA256Plus)
	}
	first := string(rest[4:])
	if !strings.HasPrefix(first, gs2HeaderPlus) {
		return fmt.Errorf("client first message %q, want the gs2 header %q", first, gs2HeaderPlus)
	}
	clientFirstBare := strings.TrimPrefix(first, gs2HeaderPlus)
	clientNonce := strings.TrimPrefix(clientFirstBare, "n=,r=")
	serverNonce := clientNonce + "ServerHalf"
	serverFirst := "r=" + serverNonce + ",s=" + rfcSalt + ",i=4096"
	if _, err := srv.Write(beAuth(authSASLContinue, serverFirst)); err != nil {
		return err
	}
	m, err = rd.Next()
	if err != nil {
		return fmt.Errorf("final response: %w", err)
	}
	final := string(m.Body)
	wantC := "c=" + base64.StdEncoding.EncodeToString(append([]byte(gs2HeaderPlus), wantCB...))
	withoutProof, proof, ok := strings.Cut(final, ",p=")
	if !ok || withoutProof != wantC+",r="+serverNonce {
		return fmt.Errorf("client final %q, want it to start %q", final, wantC+",r="+serverNonce)
	}
	salt, _ := base64.StdEncoding.DecodeString(rfcSalt)
	authMessage := clientFirstBare + "," + serverFirst + "," + withoutProof
	wantProof, serverSig, err := scramProof(password, salt, 4096, authMessage)
	if err != nil {
		return err
	}
	if proof != base64.StdEncoding.EncodeToString(wantProof) {
		return errors.New("the client's proof does not verify against the bound AuthMessage")
	}
	var tail []byte
	tail = append(tail, beAuth(authSASLFinal, "v="+base64.StdEncoding.EncodeToString(serverSig))...)
	tail = append(tail, beAuth(authOK, "")...)
	tail = append(tail, beReady()...)
	_, err = srv.Write(tail)
	return err
}

// Over a verified TLS connection to a server offering -PLUS, the exchange is
// bound to the server's certificate. The certificate is ECDSA P-384, so its
// signature hash — and therefore the binding's — is SHA-384, not the SHA-256
// the SCRAM itself uses.
func TestStartupSCRAMPlusOverTLS(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := selfSigned(t, key, &key.PublicKey)
	if cert.Leaf.SignatureAlgorithm != x509.ECDSAWithSHA384 {
		t.Fatalf("certificate signed with %v, want ECDSAWithSHA384", cert.Leaf.SignatureAlgorithm)
	}
	wantCB := sha512.Sum384(cert.Leaf.Raw)

	cn, sn := net.Pipe()
	deadline := time.Now().Add(10 * time.Second)
	_ = cn.SetDeadline(deadline)
	_ = sn.SetDeadline(deadline)
	served := make(chan error, 1)
	go func() { served <- scramPlusServer(sn, cert, "pencil", wantCB[:]) }()

	roots := x509.NewCertPool()
	roots.AddCert(cert.Leaf)
	enc := tls.Client(cn, &tls.Config{ServerName: "db.test", RootCAs: roots, MinVersion: tls.VersionTLS12})
	conn, err := Startup(enc, StartupConfig{User: "orders", Password: "pencil"})
	if serr := <-served; serr != nil {
		t.Fatalf("server: %v", serr)
	}
	if err != nil {
		t.Fatalf("Startup: %v", err)
	}
	_ = conn.Close()
}

// tlsFake is a scripted server that reports a TLS state, for the cases a real
// handshake adds nothing to.
type tlsFake struct {
	*fakeServer
	cs tls.ConnectionState
}

func (f tlsFake) ConnectionState() tls.ConnectionState { return f.cs }

// Over TLS against a server that does not offer -PLUS, the client says it
// could have bound ('y'). A server that supports binding and receives 'y'
// knows its -PLUS was stripped on the way, and refuses.
func TestStartupSCRAMOverTLSWithoutPlusDeclaresCapability(t *testing.T) {
	clientNonce := fixNonce(t, 0x50)
	serverFirst := "r=" + clientNonce + "ServerHalf,s=" + rfcSalt + ",i=4096"
	srv := &fakeServer{responses: [][]byte{
		beAuth(authSASL, mechSCRAMSHA256+"\x00\x00"),
		beAuth(authSASLContinue, serverFirst),
		beAuth(authSASLFinal, "v="+base64.StdEncoding.EncodeToString(make([]byte, 32))),
	}}
	leaf := &x509.Certificate{Raw: []byte("cert"), SignatureAlgorithm: x509.SHA256WithRSA}
	_, _ = Startup(tlsFake{srv, tls.ConnectionState{HandshakeComplete: true, PeerCertificates: []*x509.Certificate{leaf}}},
		StartupConfig{User: "orders", Password: "pencil"})

	_, types, bodies := clientTraffic(t, srv.sent.Bytes())
	if len(types) < 2 {
		t.Fatalf("client sent %q, want the two SCRAM messages", types)
	}
	mech, rest, _ := cstring(bodies[0])
	if mech != mechSCRAMSHA256 {
		t.Errorf("mechanism = %q, want %q", mech, mechSCRAMSHA256)
	}
	if got := string(rest[4:]); !strings.HasPrefix(got, "y,,") {
		t.Errorf("client first message = %q, want the 'y' gs2 header", got)
	}
	if got := string(bodies[1]); !strings.HasPrefix(got, "c=eSws,") {
		t.Errorf("client final message = %q, want c=base64(\"y,,\")", got)
	}
}

// A server offering -PLUS over a certificate whose signature defines no
// binding hash is refused before anything derived from the password is sent,
// not quietly answered with the unbound mechanism.
func TestStartupSCRAMPlusRefusesAnUnbindableCertificate(t *testing.T) {
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := selfSigned(t, key, pub)
	srv := &fakeServer{responses: [][]byte{
		beAuth(authSASL, mechSCRAMSHA256Plus+"\x00"+mechSCRAMSHA256+"\x00\x00"),
	}}
	_, err = Startup(tlsFake{srv, tls.ConnectionState{HandshakeComplete: true, PeerCertificates: []*x509.Certificate{cert.Leaf}}},
		StartupConfig{User: "orders", Password: "pencil"})
	if !errors.Is(err, ErrAuthUnsupported) {
		t.Fatalf("Startup = %v, want ErrAuthUnsupported", err)
	}
	if _, types, _ := clientTraffic(t, srv.sent.Bytes()); len(types) != 0 {
		t.Errorf("client sent %q after the startup packet, want nothing", types)
	}
}

// A plain socket is unchanged: 'n', and a server offering both mechanisms
// gets the unbound one, since there is nothing to bind to.
func TestStartupSCRAMWithoutTLSIgnoresPlus(t *testing.T) {
	clientNonce := fixNonce(t, 0x30)
	serverFirst := "r=" + clientNonce + "ServerHalf,s=" + rfcSalt + ",i=4096"
	srv := &fakeServer{responses: [][]byte{
		beAuth(authSASL, mechSCRAMSHA256Plus+"\x00"+mechSCRAMSHA256+"\x00\x00"),
		beAuth(authSASLContinue, serverFirst),
	}}
	_, _ = Startup(srv, StartupConfig{User: "orders", Password: "pencil"})
	_, _, bodies := clientTraffic(t, srv.sent.Bytes())
	if len(bodies) == 0 {
		t.Fatal("client sent no SCRAM message")
	}
	mech, rest, _ := cstring(bodies[0])
	if mech != mechSCRAMSHA256 || !strings.HasPrefix(string(rest[4:]), gs2Header) {
		t.Errorf("mechanism %q with first message %q, want %s with %q", mech, rest[4:], mechSCRAMSHA256, gs2Header)
	}
}
