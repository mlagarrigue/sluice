// Package contract is what the Go contenders of the study share and what
// none of them is measured on: the environment of comparison/README.md
// ("The contract every contender honours"), the TLS every contender runs
// under, the static document, the write table and the metrics point. The
// contender's own code — routes, handlers, the driver — lives in its main
// package, where the study counts its lines (§1.1).
package contract

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Config is the contract's environment, read once at startup.
type Config struct {
	Listen      string // STUDY_LISTEN, host:port
	Protocol    string // STUDY_PROTOCOL: h1, h2 or h3
	TLS         bool   // STUDY_TLS, on unless "off"
	Cert, Key   string // STUDY_TLS_CERT, STUDY_TLS_KEY: PEM files, or none
	Resume      bool   // STUDY_TLS_RESUME: issue session tickets
	Compression string // STUDY_COMPRESSION: off, gzip, deflate, br, zstd
	Pool        int32  // STUDY_DB_POOL, the size of the PostgreSQL pool
}

// The study server's defaults, so that a contender started bare listens
// where R does and holds as many connections.
const (
	defaultListen = "127.0.0.1:8443"
	defaultPool   = 8
)

// FromEnv reads the contract's variables through getenv (os.Getenv, or a
// map in a test). The database is not here: PGHOST and its siblings are
// libpq's, and pgx reads them itself.
func FromEnv(getenv func(string) string) (Config, error) {
	c := Config{
		Listen:      cmpOr(getenv("STUDY_LISTEN"), defaultListen),
		Protocol:    cmpOr(getenv("STUDY_PROTOCOL"), "h1"),
		Cert:        getenv("STUDY_TLS_CERT"),
		Key:         getenv("STUDY_TLS_KEY"),
		Compression: cmpOr(getenv("STUDY_COMPRESSION"), "off"),
		Pool:        defaultPool,
	}
	var err error
	if c.TLS, err = onOff("STUDY_TLS", getenv("STUDY_TLS"), true); err != nil {
		return c, err
	}
	if c.Resume, err = onOff("STUDY_TLS_RESUME", getenv("STUDY_TLS_RESUME"), false); err != nil {
		return c, err
	}
	if s := getenv("STUDY_DB_POOL"); s != "" {
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil || n <= 0 {
			return c, fmt.Errorf("STUDY_DB_POOL=%q: a positive count", s)
		}
		c.Pool = int32(n)
	}
	switch {
	case !slices.Contains([]string{"h1", "h2", "h3"}, c.Protocol):
		return c, fmt.Errorf("STUDY_PROTOCOL=%q: h1, h2 or h3", c.Protocol)
	case !slices.Contains([]string{"off", "gzip", "deflate", "br", "zstd"}, c.Compression):
		return c, fmt.Errorf("STUDY_COMPRESSION=%q: off, gzip, deflate, br or zstd", c.Compression)
	case !c.TLS && c.Protocol != "h1":
		return c, fmt.Errorf("STUDY_TLS=off with STUDY_PROTOCOL=%s: in clear the study runs HTTP/1.1 only", c.Protocol)
	case (c.Cert == "") != (c.Key == ""):
		return c, errors.New("STUDY_TLS_CERT and STUDY_TLS_KEY go together: both, or neither for a certificate made at startup")
	}
	return c, nil
}

// Offers refuses what a contender does not implement, before it listens:
// a configuration it cannot serve is published "not supported", never
// served as something else (study §2.4).
func (c Config) Offers(protocols, codings []string) error {
	if !slices.Contains(protocols, c.Protocol) {
		return fmt.Errorf("STUDY_PROTOCOL=%s: not supported by this contender", c.Protocol)
	}
	if c.Compression != "off" && !slices.Contains(codings, c.Compression) {
		return fmt.Errorf("STUDY_COMPRESSION=%s: not offered by this contender", c.Compression)
	}
	return nil
}

func cmpOr(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func onOff(name, v string, def bool) (bool, error) {
	switch v {
	case "":
		return def, nil
	case "on":
		return true, nil
	case "off":
		return false, nil
	}
	return false, fmt.Errorf("%s=%q: on or off", name, v)
}

// ALPN is the one protocol identifier of the run.
func (c Config) ALPN() string {
	return map[string]string{"h1": "http/1.1", "h2": "h2", "h3": "h3"}[c.Protocol]
}

// TLSConfig is the TLS of every contender (comparison/README.md, "The TLS
// every contender runs under"): TLS 1.3 only, the campaign's certificate or
// an ECDSA P-256 one made at startup when none is named — as the study's
// server does, which is what the local chain relies on —, ALPN naming the
// run's one protocol, tickets only when asked. Enabling a protocol is not
// an optimisation (study §2.4): both versions of a contender take this as
// it is.
func (c Config) TLSConfig() (*tls.Config, error) {
	var cert tls.Certificate
	var err error
	if c.Cert != "" {
		cert, err = tls.LoadX509KeyPair(c.Cert, c.Key)
	} else {
		cert, err = selfSigned()
	}
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates:           []tls.Certificate{cert},
		MinVersion:             tls.VersionTLS13,
		NextProtos:             []string{c.ALPN()},
		SessionTicketsDisabled: !c.Resume,
	}, nil
}

// Summary is the startup line's account of the transport, worded as the
// study's server words it, so that a campaign's logs read alike.
func (c Config) Summary(cfg *tls.Config) string {
	if cfg == nil {
		return "clear text, HTTP/1.1"
	}
	key := "unknown key"
	if len(cfg.Certificates) > 0 {
		switch k := cfg.Certificates[0].PrivateKey.(type) {
		case *ecdsa.PrivateKey:
			key = "ECDSA " + k.Curve.Params().Name
		case *rsa.PrivateKey:
			key = fmt.Sprintf("RSA %d", k.N.BitLen())
		case ed25519.PrivateKey:
			key = "Ed25519"
		}
	}
	resumption := "off"
	if c.Resume {
		resumption = "on"
	}
	return fmt.Sprintf("TLS %s+, %s certificate, ALPN %s, resumption %s",
		strings.TrimPrefix(tls.VersionName(cfg.MinVersion), "TLS "), key, strings.Join(cfg.NextProtos, ","), resumption)
}

// The write table, as the study's server creates it at startup: no read
// ever touches it, so that a read's answer does not depend on the writes
// before it (§2.2), and autovacuum is off so that nothing fires mid-step.
var WriteTable = []string{
	`CREATE TABLE IF NOT EXISTS comparison_writes (
		id bigserial PRIMARY KEY, key bigint NOT NULL, total bigint NOT NULL,
		created timestamptz NOT NULL DEFAULT now())`,
	`ALTER TABLE comparison_writes SET (autovacuum_enabled = off)`,
}

// ListMax is the largest limit a list request may ask for, R's -list-max.
const ListMax = 100000

// ReadHeaderTimeout is R's (web.DefaultReadHeaderTimeout): net/http's
// examples leave it unset, and a server without one holds a connection for
// every client that never finishes its headers — not acceptable in
// production, which both versions must be (study §2.4).
const ReadHeaderTimeout = 10 * time.Second

// Static is R's static document, about 1 KB once encoded (§2.2): the
// contract makes every contender answer it value for value.
type Static struct {
	Status  string   `json:"status"`
	Service string   `json:"service"`
	Version string   `json:"version"`
	Padding []string `json:"padding"`
}

// StaticDoc builds it.
func StaticDoc() Static {
	d := Static{Status: "ok", Service: "sluice-study", Version: "r"}
	for i := range 40 {
		d.Padding = append(d.Padding, fmt.Sprintf("line-%02d-abcdefghijklmnop", i))
	}
	return d
}

// selfSigned is the study's server's certificate for a local run: ECDSA
// P-256, a day long, for localhost and the loopback addresses; a client
// that checks it cannot, and the generator runs with -no-verify.
func selfSigned() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "study-contender"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, nil
}
