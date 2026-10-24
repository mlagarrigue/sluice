package main

import (
	"crypto/tls"
	"crypto/x509"
	"testing"
)

// TestTLSResume: under -tls-resume the server issues session tickets and a
// returning client resumes; without it (R), crypto/tls's default of issuing
// them is turned off and the client handshakes in full every time. The
// summary written at startup says which.
func TestTLSResume(t *testing.T) {
	for _, resume := range []bool{false, true} {
		args := []string{"-transport", transportNetHTTPH2}
		if resume {
			args = append(args, "-tls-resume")
		}
		o, err := parseOptions(args)
		if err != nil {
			t.Fatal(err)
		}
		cfg, err := tlsConfig(o)
		if err != nil {
			t.Fatal(err)
		}
		want := "TLS 1.3+, ECDSA P-256 certificate, ALPN h2, resumption off"
		if resume {
			want = "TLS 1.3+, ECDSA P-256 certificate, ALPN h2, resumption on"
		}
		if got := tlsSummary(cfg, o.tlsResume); got != want {
			t.Errorf("summary %q, want %q", got, want)
		}

		ln, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				_, _ = c.Write([]byte("x")) // the ticket travels before it
				_ = c.Close()
			}
		}()
		pool := x509.NewCertPool()
		pool.AddCert(cfg.Certificates[0].Leaf)
		client := &tls.Config{RootCAs: pool, ServerName: "127.0.0.1", NextProtos: []string{"h2"}, ClientSessionCache: tls.NewLRUClientSessionCache(4)}
		var resumed []bool
		for range 2 {
			c, err := tls.Dial("tcp", ln.Addr().String(), client)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = c.Read(make([]byte, 1))
			resumed = append(resumed, c.ConnectionState().DidResume)
			_ = c.Close()
		}
		_ = ln.Close()
		if resumed[0] || resumed[1] != resume {
			t.Errorf("-tls-resume %v: resumed %v, want [false %v]", resume, resumed, resume)
		}
	}
	if _, err := parseOptions([]string{"-tls=false", "-tls-resume"}); err == nil {
		t.Error("-tls-resume in clear accepted")
	}
	o, err := parseOptions([]string{"-transport", transportHTTPStreamH3, "-tls-resume"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg, err := tlsConfig(o); err != nil || cfg.SessionTicketsDisabled {
		t.Errorf("h3: %v, SessionTicketsDisabled %v: quic.NewListener refuses it with SessionTickets", err, cfg.SessionTicketsDisabled)
	}
}

// TestContractEnv: the variables every contender reads set the flags'
// defaults, and a flag still wins; a value outside the contract is refused
// before anything listens.
func TestContractEnv(t *testing.T) {
	t.Setenv("STUDY_PROTOCOL", "h2")
	t.Setenv("STUDY_TLS_RESUME", "on")
	t.Setenv("STUDY_COMPRESSION", "gzip")
	t.Setenv("STUDY_DB_POOL", "16")
	t.Setenv("STUDY_LISTEN", "127.0.0.1:9443")
	t.Setenv("PGHOST", "db.local")
	t.Setenv("PGPORT", "6432")
	t.Setenv("PGUSER", "study")
	o, err := parseOptions(nil)
	if err != nil {
		t.Fatal(err)
	}
	if o.transport != transportNetHTTPH2 || !o.tlsResume || o.compression != codecGzip1 || o.dbConns != 16 ||
		o.listen != "127.0.0.1:9443" || o.db.addr != "db.local:6432" || o.db.user != "study" {
		t.Fatalf("%+v", o)
	}
	if o, err = parseOptions([]string{"-transport", transportHTTPStreamH2, "-db-conns", "4"}); err != nil || o.transport != transportHTTPStreamH2 || o.dbConns != 4 {
		t.Fatalf("a flag does not win over the environment: %v %+v", err, o)
	}
	t.Setenv("STUDY_TLS", "yes")
	if _, err := parseOptions(nil); err == nil {
		t.Fatal("STUDY_TLS=yes accepted")
	}
}
