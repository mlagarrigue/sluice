package web

import (
	"errors"
	"fmt"
	"slices"

	"github.com/mlagarrigue/sluice/diagnostics"
)

// # Authorisation here decides what may be *asked for*, not what is returned
//
// This is the half of the boundary that runs in this process. The other half
// runs in the database, as a row-level-security policy, and the two are not
// alternatives: the grant below narrows the question, the policy decides the
// answer. If the two ever disagree, the database wins — which is the property
// that makes the arrangement worth having, because a bug here becomes a query
// that returns nothing rather than a query that returns somebody else's rows.
//
// The test for that is not a unit test. It is
// `example/orders/integration_test.go`, where the application-side filter is
// removed and the row count must not change.

// ErrForbidden reports a principal that has no grant for what it asked.
var ErrForbidden = errors.New("web: the principal is not authorised for this")

// CodeForbidden is the diagnostic code for a request the principal was
// authenticated for and not permitted.
const CodeForbidden = "Transport.Request.Forbidden"

// Grant is what a principal may see: the security principal that becomes part
// of the batching key, plus whatever other dimensions bound the request.
//
// # The zero Grant permits nothing
//
// That is the whole design of this type. A grant assembled from claims that
// were absent, or built by a code path that forgot to fill it in, must deny
// rather than allow — so [Grant.Allows] refuses an empty tenant, and
// [Grant.Dimension] returns "no values" rather than "any value" for a
// dimension nobody set. Reading an unset field as *unrestricted* is how an
// authorisation bug becomes a data leak instead of a 403.
type Grant struct {
	// Tenant is the security principal. It is what a pipeline partitions its
	// batches by and what `SET LOCAL app.tenant_id` carries, so it is the one
	// field with no useful empty value.
	Tenant string

	// Dimensions bounds the other axes a request may span — region, business
	// unit, whatever the domain has. A dimension that is absent from the map
	// is one the principal has **no** access to, not one they have unlimited
	// access to.
	//
	// Read it through [Grant.Allows] and [Grant.Dimension] rather than
	// directly: both deny when Tenant is empty, whatever the map holds, and a
	// direct read of the map skips that check.
	Dimensions map[string][]string
}

// Allows reports whether the grant permits a value on a dimension.
//
// An unset dimension denies. So does an empty tenant, whatever the dimensions
// say: without one there is nothing to set on the connection, and a query that
// ran with no principal set would either return nothing or — if someone had
// disabled the policy — everything.
func (g Grant) Allows(dimension, value string) bool {
	if g.Tenant == "" {
		return false
	}
	return slices.Contains(g.Dimensions[dimension], value)
}

// Dimension returns the values a principal may see on an axis, or nil.
//
// nil means none. A caller turning this into a SQL predicate must treat an
// empty result as "match nothing" — an `IN ()` that becomes a missing WHERE
// clause is the classic way a filter disappears.
func (g Grant) Dimension(name string) []string {
	if g.Tenant == "" {
		return nil
	}
	return g.Dimensions[name]
}

// IsZero reports a grant that permits nothing.
func (g Grant) IsZero() bool { return g.Tenant == "" }

// Authorizer turns a principal into a grant.
//
// It is an interface because the mapping is domain knowledge: it may be claims
// on the token, a lookup in a policy service, or a table. What the library
// fixes is the *shape* of the answer, so that the pipeline downstream can
// partition on it without knowing where it came from.
type Authorizer interface {
	Authorize(Principal) (Grant, error)
}

// ClaimAuthorizer builds a grant out of the token's own claims.
//
// It is the simplest thing that can work, and it is only correct when the
// issuer is trusted to state the grant — which is the case when the issuer is
// your own service and not when it is a general-purpose identity provider
// whose claims a user can influence. When in doubt, implement [Authorizer]
// against something that knows the policy.
type ClaimAuthorizer struct {
	// DimensionClaims maps a dimension name to the claim carrying its values.
	// A claim may hold a string or an array of them.
	DimensionClaims map[string]string
}

// Authorize reads the grant off the principal.
//
// A principal with no tenant is refused rather than granted an empty one: the
// tenant is what the database boundary keys on, and continuing without it
// means running a query whose security depends on a policy nobody set.
func (a ClaimAuthorizer) Authorize(p Principal) (Grant, error) {
	if p.Tenant == "" {
		return Grant{}, fmt.Errorf("%w: the principal carries no tenant", ErrForbidden)
	}
	g := Grant{Tenant: p.Tenant}
	if len(a.DimensionClaims) == 0 {
		return g, nil
	}
	g.Dimensions = make(map[string][]string, len(a.DimensionClaims))
	for dimension, claim := range a.DimensionClaims {
		if values := claimStrings(p.Claims[claim]); len(values) > 0 {
			g.Dimensions[dimension] = values
		}
	}
	return g, nil
}

// claimStrings reads a claim that may be a single string or an array of them.
// Anything else yields nothing, which denies — the safe direction for a value
// this code did not understand.
func claimStrings(v any) []string {
	switch value := v.(type) {
	case string:
		if value == "" {
			return nil
		}
		return []string{value}
	case []any:
		out := make([]string, 0, len(value))
		for _, item := range value {
			if s, ok := item.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		if len(out) == 0 {
			return nil
		}
		return out
	}
	return nil
}

// Authorize runs an authorizer and turns a refusal into a diagnostic.
//
// Like [Authenticate], the diagnostic says only that the request was refused.
// Which dimension failed is in the error, for the caller to log — telling a
// requester which axis they were short on tells them what to probe next.
func Authorize(p Principal, a Authorizer) (Grant, diagnostics.Diagnostic, error) {
	g, err := a.Authorize(p)
	if err != nil {
		return Grant{}, forbidden(), err
	}
	// A grant an authorizer returned without error but which permits nothing
	// is a bug in the authorizer, and it is caught here rather than turned
	// into a query with no principal.
	if g.IsZero() {
		return Grant{}, forbidden(), fmt.Errorf("%w: the authorizer returned an empty grant", ErrForbidden)
	}
	return g, diagnostics.Diagnostic{}, nil
}

func forbidden() diagnostics.Diagnostic {
	return diagnostics.NewDiagnostic(diagnostics.Error, CodeForbidden, diagnostics.Path{}).
		WithMessage("RequestForbidden", nil).
		WithOrigin(diagnostics.OriginBusinessRule)
}
