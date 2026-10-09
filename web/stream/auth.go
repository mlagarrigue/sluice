package stream

import (
	"bytes"

	"github.com/mlagarrigue/sluice/diagnostics"
	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/web"
)

// Authenticate establishes every open exchange's principal from its bearer
// token, refusing with 401 the ones whose credentials are absent, malformed
// or rejected.
//
// It is [web.Authenticate] made batch-shaped and protocol-neutral: the token
// comes off the authorization field through [httpstream.Request.Get], so the
// header's case does not matter whichever protocol carried it, and the
// verifier is the same [web.Verifier] the supported path uses — one boundary,
// two transports.
//
// The refusal never says which half of a credential was wrong, for the reason
// [web.Authenticate] states: telling a requester which part of a forgery was
// right is telling them how to fix it. What web's version returns as an error
// for the caller to log is not surfaced here — a verifier that must observe
// its rejections wraps itself.
func Authenticate(v web.Verifier, exchanges []Exchange) {
	for i := range exchanges {
		e := &exchanges[i]
		if e.Refused() {
			continue
		}
		raw, dup := authorization(e.Request.Headers)
		if dup {
			// Two Authorization fields is two credentials for one request:
			// whichever one this boundary picked, another hop may pick the
			// other, and the request authenticates as two different callers
			// on its way through. Refused outright, the way the transport
			// refuses a Host/:authority disagreement, rather than resolved
			// in favour of either.
			e.Refuse(401, unauthenticated("CredentialsDuplicated"))
			continue
		}
		token, messageID := bearer(raw)
		if messageID != "" {
			e.Refuse(401, unauthenticated(messageID))
			continue
		}
		p, err := v.Verify(token)
		if err != nil {
			e.Refuse(401, unauthenticated("CredentialsRejected"))
			continue
		}
		e.Principal = p
	}
}

// authorization returns the request's one authorization value, or reports
// that the request carried more than one. Authorization is not a list-valued
// field, so two lines are not one value split — they are two credentials,
// and [Authenticate] refuses the request rather than choosing between them.
func authorization(headers []httpstream.Header) (raw []byte, duplicated bool) {
	found := false
	for _, h := range headers {
		if !foldEqual(h.Name, "authorization") {
			continue
		}
		if found {
			return nil, true
		}
		raw, found = h.Value, true
	}
	return raw, false
}

// bearer extracts the token from a raw authorization value, under the same
// rules as [web.Bearer]: the scheme case-insensitive as RFC 7235 requires,
// exactly one space, no creative parsing at an authentication boundary. A
// failure is named by the message identifier the refusal carries.
func bearer(raw []byte) (token, messageID string) {
	if len(raw) == 0 {
		return "", "CredentialsMissing"
	}
	const prefix = "bearer "
	if len(raw) <= len(prefix) || !foldEqual(raw[:len(prefix)], prefix) {
		return "", "CredentialsNotBearer"
	}
	t := raw[len(prefix):]
	if bytes.IndexByte(t, ' ') >= 0 || bytes.IndexByte(t, '\t') >= 0 {
		return "", "CredentialsMalformed"
	}
	return string(t), ""
}

func unauthenticated(messageID string) diagnostics.Diagnostic {
	return diagnostics.NewDiagnostic(diagnostics.Error, web.CodeUnauthenticated, diagnostics.Path{}).
		WithMessage(messageID, nil).
		WithOrigin(diagnostics.OriginProtocol)
}

// Authorize establishes every open exchange's grant from its principal,
// refusing with 403 the ones the authorizer declines. It is [web.Authorize]
// applied along the batch — same authorizer, same diagnostic, same silence
// about which dimension fell short.
func Authorize(a web.Authorizer, exchanges []Exchange) {
	for i := range exchanges {
		e := &exchanges[i]
		if e.Refused() {
			continue
		}
		g, d, err := web.Authorize(e.Principal, a)
		if err != nil {
			e.Refuse(403, d)
			continue
		}
		e.Grant = g
	}
}
