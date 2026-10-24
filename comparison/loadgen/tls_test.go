package main

import (
	"strings"
	"testing"
)

// TestClear: HTTP/1.1 in clear (TLS off, the study's level 2) is a stage
// like any other, no handshake in it and "none" published; HTTP/2 and
// HTTP/3 in clear are refused before anything is sent.
func TestClear(t *testing.T) {
	srv := sleepServer(t, clearH1, 0, nil)
	if !strings.HasPrefix(srv.URL, "http://") {
		t.Fatalf("url %s", srv.URL)
	}
	c := testCampaign(t, protoH1, srv.URL, "-scenario", "unit", "-rate", "200", "-duration", "1s", "-conns", "4", "-new-conns", "10")
	rep := runStage(t, c)
	if rep.Failed() || rep.Handshakes != 0 || rep.TLSVersion != "none" || rep.TLSSuite != "none" {
		t.Fatalf("errors %d handshakes %d tls %q %q, want a stage in clear", rep.Errors, rep.Handshakes, rep.TLSVersion, rep.TLSSuite)
	}
	if got := srv.conns.Load(); got <= 4 {
		t.Fatalf("%d connections: the fresh ones did not open", got)
	}
	for _, proto := range []string{protoH2, protoH3} {
		if _, err := parse([]string{"-url", srv.URL, "-proto", proto}); err == nil {
			t.Errorf("-proto %s in clear accepted", proto)
		}
	}
	if _, err := parse([]string{"-url", "127.0.0.1:8443"}); err == nil {
		t.Error("a URL with no scheme accepted")
	}
}

// TestTLSResume: with -tls-resume, the fresh connections of a stage resume
// the session the budget's handshakes left a ticket for, and the line says
// so; without it, none does. The negotiated setting is published: TLS 1.3,
// the suite, the protocol's ALPN.
func TestTLSResume(t *testing.T) {
	protos(t, func(t *testing.T, proto string) { //nolint:thelper // a sub-test body
		srv := sleepServer(t, proto, 0, nil)
		for _, resume := range []bool{false, true} {
			args := []string{"-scenario", "unit", "-rate", "500", "-duration", "1s", "-conns", "4", "-new-conns", "10"}
			if resume {
				args = append(args, "-tls-resume")
			}
			rep := runStage(t, testCampaign(t, proto, srv.URL, args...))
			t.Logf("resume %v: handshakes %d resumed %d tls %s %s alpn %s", resume, rep.Handshakes, rep.Resumed, rep.TLSVersion, rep.TLSSuite, rep.ALPN)
			if rep.Failed() || rep.Handshakes == 0 || rep.TLSResume != resume {
				t.Fatalf("errors %d handshakes %d tls_resume %v", rep.Errors, rep.Handshakes, rep.TLSResume)
			}
			if want := map[bool]int{false: 0, true: rep.Handshakes}[resume]; rep.Resumed != want {
				t.Fatalf("resume %v: %d of %d handshakes resumed, want %d", resume, rep.Resumed, rep.Handshakes, want)
			}
			alpn := map[string]string{protoH1: "http/1.1", protoH2: "h2", protoH3: "h3"}[proto]
			if rep.TLSVersion != "TLS 1.3" || !strings.HasPrefix(rep.TLSSuite, "TLS_") || strings.Contains(rep.TLSSuite, "|") || rep.ALPN != alpn {
				t.Fatalf("tls %q suite %q alpn %q, want TLS 1.3, one suite, %s", rep.TLSVersion, rep.TLSSuite, rep.ALPN, alpn)
			}
		}
	})
}
