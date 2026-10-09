package stream

import (
	"errors"
	"net/http"
	"testing"

	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/web"
)

// acceptToken is the verifier the tests share: one token is good, everything
// else is rejected.
type acceptToken struct{}

func (acceptToken) Verify(token string) (web.Principal, error) {
	if token == "good" {
		return web.Principal{Subject: "u1", Tenant: "acme"}, nil
	}
	return web.Principal{}, errors.New("verifier: not the token this test accepts")
}

func withAuthz(value string) Exchange {
	e := exchangeFor("GET", "/health")
	if value != "" {
		e.Request.Headers = []httpstream.Header{
			{Name: []byte("Authorization"), Value: []byte(value)},
		}
	}
	return e
}

// web.Bearer (string, over *http.Request) and this package's bearer (bytes,
// over the raw header) implement the same RFC 7235 rule twice — the
// []byte→string conversion a batch path can't afford to pay per element is
// the whole reason there are two. Nothing stops them drifting apart as each
// evolves on its own, so this table drives both real implementations side
// by side rather than one standing in for the other: a case the two
// disagree on fails here before it fails two services agreeing to trust the
// same token.
func TestBearerAgreesWithWebBearerOnTheSameRule(t *testing.T) {
	tests := []struct {
		name   string
		header string // "" means no Authorization header at all
		noHdr  bool
	}{
		{name: "absent header", noHdr: true},
		{name: "empty header", header: ""},
		{name: "a bearer token", header: "Bearer good"},
		{name: "scheme is case-insensitive", header: "bEaReR good"},
		{name: "not bearer", header: "Basic good"},
		{name: "no space before the token", header: "Bearergood"},
		{name: "a space inside the token", header: "Bearer go od"},
		{name: "a tab inside the token", header: "Bearer go\tod"},
		{name: "scheme with nothing after it", header: "Bearer"},
		{name: "scheme and one trailing space, no token", header: "Bearer "},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var raw []byte
			req := &http.Request{Header: http.Header{}}
			if !tc.noHdr {
				raw = []byte(tc.header)
				req.Header.Set("Authorization", tc.header)
			}

			gotToken, messageID := bearer(raw)
			wantToken, _, wantOK := web.Bearer(req)

			if gotOK := messageID == ""; gotOK != wantOK {
				t.Fatalf("bearer accepted = %v, web.Bearer accepted = %v (header %q)",
					gotOK, wantOK, tc.header)
			}
			if gotToken != wantToken {
				t.Errorf("bearer token = %q, web.Bearer token = %q (header %q)",
					gotToken, wantToken, tc.header)
			}
		})
	}
}

func TestAuthenticate(t *testing.T) {
	tests := []struct {
		name, header string
		wantSubject  string
	}{
		{"a bearer token that verifies", "Bearer good", "u1"},
		{"the scheme is case-insensitive", "bEaReR good", "u1"},
		{"no header", "", ""},
		{"not bearer", "Basic good", ""},
		{"a space inside the token", "Bearer go od", ""},
		{"a rejected token", "Bearer forged", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ex := []Exchange{withAuthz(tc.header)}
			Authenticate(acceptToken{}, ex)
			e := &ex[0]
			if tc.wantSubject != "" {
				if e.Refused() {
					t.Fatalf("refused with %d: %v", e.Status, e.Diagnostics)
				}
				if e.Principal.Subject != tc.wantSubject {
					t.Errorf("subject = %q, want %q", e.Principal.Subject, tc.wantSubject)
				}
				return
			}
			if e.Status != 401 {
				t.Fatalf("status = %d, want 401", e.Status)
			}
			if len(e.Diagnostics) != 1 || e.Diagnostics[0].Code != web.CodeUnauthenticated {
				t.Errorf("diagnostics = %v, want one %s", e.Diagnostics, web.CodeUnauthenticated)
			}
		})
	}
}

// The refusal says it was refused, never why: which half of a forgery was
// right is not information to hand back.
func TestAuthenticateDoesNotSayWhichHalfFailed(t *testing.T) {
	forged := []Exchange{withAuthz("Bearer forged")}
	missing := []Exchange{withAuthz("")}
	Authenticate(acceptToken{}, forged)
	Authenticate(acceptToken{}, missing)
	if forged[0].Diagnostics[0].Code != missing[0].Diagnostics[0].Code {
		t.Errorf("a forged token and a missing one carry different codes: %q vs %q",
			forged[0].Diagnostics[0].Code, missing[0].Diagnostics[0].Code)
	}
}

func TestAuthenticateSkipsARefusedExchange(t *testing.T) {
	ex := []Exchange{withAuthz("Bearer good")}
	ex[0].Status = 404
	Authenticate(acceptToken{}, ex)
	if ex[0].Principal.Subject != "" {
		t.Error("a refused exchange was authenticated")
	}
}

// grantTenant authorizes any principal with a tenant, which is what
// [web.ClaimAuthorizer] does without needing a token to parse.
type grantTenant struct{}

func (grantTenant) Authorize(p web.Principal) (web.Grant, error) {
	if p.Tenant == "" {
		return web.Grant{}, errors.New("authorizer: no tenant")
	}
	return web.Grant{Tenant: p.Tenant}, nil
}

func TestAuthorize(t *testing.T) {
	t.Run("a principal with a tenant gets its grant", func(t *testing.T) {
		ex := []Exchange{withAuthz("Bearer good")}
		Authenticate(acceptToken{}, ex)
		Authorize(grantTenant{}, ex)
		e := &ex[0]
		if e.Refused() {
			t.Fatalf("refused with %d: %v", e.Status, e.Diagnostics)
		}
		if e.Grant.Tenant != "acme" {
			t.Errorf("grant tenant = %q, want acme", e.Grant.Tenant)
		}
	})

	t.Run("a declined principal is 403", func(t *testing.T) {
		ex := []Exchange{exchangeFor("GET", "/health")} // never authenticated: zero principal
		Authorize(grantTenant{}, ex)
		e := &ex[0]
		if e.Status != 403 {
			t.Fatalf("status = %d, want 403", e.Status)
		}
		if len(e.Diagnostics) != 1 || e.Diagnostics[0].Code != web.CodeForbidden {
			t.Errorf("diagnostics = %v, want one %s", e.Diagnostics, web.CodeForbidden)
		}
	})

	t.Run("a refused exchange is stepped over", func(t *testing.T) {
		ex := []Exchange{withAuthz("Bearer good")}
		ex[0].Status = 401
		Authorize(grantTenant{}, ex)
		if ex[0].Grant.Tenant != "" {
			t.Error("a refused exchange was authorized")
		}
	})
}

// Two Authorization fields are two credentials: whichever one this boundary
// picked, another hop may pick the other, and one request authenticates as
// two different callers on its way through. Refused outright — the same
// stance the transport takes on a Host/:authority disagreement — even when
// both lines agree, since parsing "do they agree" is itself the creative
// parsing an authentication boundary must not do.
func TestAuthenticateRefusesDuplicateAuthorization(t *testing.T) {
	for _, tc := range []struct {
		name   string
		second string
	}{
		{"two different credentials", "Bearer other"},
		{"the same credential twice", "Bearer good"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := exchangeFor("GET", "/health")
			e.Request.Headers = []httpstream.Header{
				{Name: []byte("Authorization"), Value: []byte("Bearer good")},
				{Name: []byte("authorization"), Value: []byte(tc.second)},
			}
			ex := []Exchange{e}
			Authenticate(acceptToken{}, ex)
			if ex[0].Status != 401 {
				t.Fatalf("status = %d, want 401", ex[0].Status)
			}
			if ex[0].Principal.Subject != "" {
				t.Errorf("a principal was established from one of two competing credentials: %q", ex[0].Principal.Subject)
			}
		})
	}
}
