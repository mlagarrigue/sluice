package postgres

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// scramTLSServer plays the server half of a SCRAM exchange over a real TLS
// connection, offering both mechanisms, and checks the mechanism, the gs2
// header and the channel binding the client chose against what the test
// expects; the proof is checked with scramProof (pinned against RFC 7677
// elsewhere). wantCB is nil for an unbound exchange.
func scramTLSServer(nc net.Conn, cert tls.Certificate, password, wantMech, wantGS2 string, wantCB []byte) error {
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
	if !ok || mech != wantMech {
		return fmt.Errorf("mechanism %q, want %s", mech, wantMech)
	}
	first := string(rest[4:])
	if !strings.HasPrefix(first, wantGS2) {
		return fmt.Errorf("client first message %q, want the gs2 header %q", first, wantGS2)
	}
	clientFirstBare := strings.TrimPrefix(first, wantGS2)
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
	wantC := "c=" + base64.StdEncoding.EncodeToString(append([]byte(wantGS2), wantCB...))
	withoutProof, proof, ok := strings.Cut(final, ",p=")
	if !ok || withoutProof != wantC+",r="+serverNonce {
		return fmt.Errorf("client final %q, want it to start %q", final, wantC+",r="+serverNonce)
	}
	salt, _ := base64.StdEncoding.DecodeString(rfcSalt)
	authMessage := clientFirstBare + "," + serverFirst + "," + withoutProof
	wantProof, serverSig, err := scramProof(password, salt, 4096, authMessage, defaultMinSaltLen)
	if err != nil {
		return err
	}
	if proof != base64.StdEncoding.EncodeToString(wantProof) {
		return errors.New("the client's proof does not verify against the AuthMessage")
	}
	var tail []byte
	tail = append(tail, beAuth(authSASLFinal, "v="+base64.StdEncoding.EncodeToString(serverSig))...)
	tail = append(tail, beAuth(authOK, "")...)
	tail = append(tail, beReady()...)
	_, err = srv.Write(tail)
	return err
}

// ed25519Server is a server whose certificate has no tls-server-end-point
// hash (RFC 5929 §4.1 defines none for a signature algorithm with no hash),
// offering SCRAM-SHA-256-PLUS all the same — what PostgreSQL does with an
// Ed25519 server certificate. It returns the dial for Startup and a channel
// the server's verdict arrives on.
func ed25519Server(t *testing.T, wantMech, wantGS2 string) (*tls.Conn, <-chan error) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := selfSigned(t, key, pub)
	if cert.Leaf.SignatureAlgorithm != x509.PureEd25519 {
		t.Fatalf("certificate signed with %v, want PureEd25519", cert.Leaf.SignatureAlgorithm)
	}
	cn, sn := net.Pipe()
	deadline := time.Now().Add(10 * time.Second)
	_ = cn.SetDeadline(deadline)
	_ = sn.SetDeadline(deadline)
	served := make(chan error, 1)
	go func() { served <- scramTLSServer(sn, cert, "pencil", wantMech, wantGS2, nil) }()
	roots := x509.NewCertPool()
	roots.AddCert(cert.Leaf)
	return tls.Client(cn, &tls.Config{ServerName: "db.test", RootCAs: roots, MinVersion: tls.VersionTLS12}), served
}

// A server whose certificate the binding cannot hash, offering -PLUS: the
// default refuses rather than downgrade silently, and ChannelBindingDisable
// is the way through — the unbound mechanism, declared with the 'n' flag
// RFC 5802 §6 gives a client that does not do channel binding, as libpq's
// channel_binding=disable sends.
func TestStartupChannelBindingDisableReachesAnEd25519Server(t *testing.T) {
	t.Run("the default refuses", func(t *testing.T) {
		enc, served := ed25519Server(t, mechSCRAMSHA256Plus, gs2HeaderPlus)
		_, err := startup(enc, Config{User: "orders", Password: "pencil"})
		if !errors.Is(err, ErrAuthUnsupported) {
			t.Fatalf("Startup: %v, want ErrAuthUnsupported (no tls-server-end-point hash)", err)
		}
		<-served // the server failed too: the client never sent a first message
	})
	t.Run("ChannelBindingDisable goes unbound", func(t *testing.T) {
		enc, served := ed25519Server(t, mechSCRAMSHA256, gs2Header)
		conn, err := startup(enc, Config{User: "orders", Password: "pencil", ChannelBinding: ChannelBindingDisable})
		if serr := <-served; serr != nil {
			t.Fatalf("server: %v", serr)
		}
		if err != nil {
			t.Fatalf("Startup: %v", err)
		}
		_ = conn.Close()
	})
}

// begin, the choice itself: disabled, the client picks the unbound
// mechanism under the 'n' flag even over TLS with -PLUS on offer, and
// refuses a server that offers nothing else — naming the mode, since the
// server's policy and the caller's cannot both be met.
func TestBeginDisabledNeverBinds(t *testing.T) {
	cs := &tls.ConnectionState{HandshakeComplete: true, PeerCertificates: []*x509.Certificate{
		{Raw: []byte("cert"), SignatureAlgorithm: x509.SHA256WithRSA},
	}}
	both := []byte(mechSCRAMSHA256Plus + "\x00" + mechSCRAMSHA256 + "\x00\x00")

	s := &scram{}
	mech, first, err := s.begin(both, rfcPassword, cs, true)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if mech != mechSCRAMSHA256 || !strings.HasPrefix(string(first), gs2Header) {
		t.Errorf("disabled: mechanism %s, first %q; want %s under %q", mech, first, mechSCRAMSHA256, gs2Header)
	}

	s = &scram{}
	if mech, _, err := s.begin(both, rfcPassword, cs, false); err != nil || mech != mechSCRAMSHA256Plus {
		t.Errorf("the control, binding enabled: mechanism %s, %v; want %s", mech, err, mechSCRAMSHA256Plus)
	}

	s = &scram{}
	_, _, err = s.begin([]byte(mechSCRAMSHA256Plus+"\x00\x00"), rfcPassword, cs, true)
	if !errors.Is(err, ErrAuthUnsupported) || !strings.Contains(err.Error(), "ChannelBindingDisable") {
		t.Errorf("only -PLUS offered, disabled: %v; want ErrAuthUnsupported naming ChannelBindingDisable", err)
	}
}

// Config.ChannelBinding takes three values and nothing else; the refusal is
// at startup, before anything is sent, like every other configuration error.
func TestChannelBindingModeIsValidated(t *testing.T) {
	srv := &fakeServer{}
	_, err := startup(srv, Config{User: "orders", Password: "pencil", ChannelBinding: ChannelBindingMode(7)})
	if err == nil || !strings.Contains(err.Error(), "ChannelBindingMode(7)") {
		t.Errorf("Startup with ChannelBinding 7: %v; want a refusal naming the value", err)
	}
	if srv.sent.Len() != 0 {
		t.Errorf("%d bytes sent before the configuration was refused", srv.sent.Len())
	}
	for _, m := range []ChannelBindingMode{ChannelBindingPrefer, ChannelBindingRequire, ChannelBindingDisable} {
		if !strings.HasPrefix(m.String(), "ChannelBinding") || strings.Contains(m.String(), "(") {
			t.Errorf("%d.String() = %q", int(m), m.String())
		}
	}
}
