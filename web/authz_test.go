package web_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/mlagarrigue/sluice/web"
)

// The invariant this whole type exists for: an unset field denies. Reading a
// missing dimension as *unrestricted* is how an authorisation bug becomes a
// data leak instead of a 403.
func TestZeroGrantPermitsNothing(t *testing.T) {
	var g web.Grant

	if !g.IsZero() {
		t.Error("the zero Grant does not report itself as empty")
	}
	if g.Allows("region", "eu-west") {
		t.Error("the zero Grant allowed a value")
	}
	if g.Dimension("region") != nil {
		t.Error("the zero Grant returned values for a dimension")
	}

	// A grant with dimensions but no tenant is still empty: without a tenant
	// there is nothing to set on the connection, and a query that ran with no
	// principal would return nothing — or everything, if someone had disabled
	// the policy.
	noTenant := web.Grant{Dimensions: map[string][]string{"region": {"eu-west"}}}
	if !noTenant.IsZero() {
		t.Error("a grant with no tenant does not report itself as empty")
	}
	if noTenant.Allows("region", "eu-west") {
		t.Error("a grant with no tenant allowed a value its dimensions listed")
	}
	if noTenant.Dimension("region") != nil {
		t.Error("a grant with no tenant returned its dimension values")
	}
}

// A dimension nobody set is one the principal has no access to, not one they
// have unlimited access to.
func TestGrantUnsetDimensionDenies(t *testing.T) {
	g := web.Grant{Tenant: "acme", Dimensions: map[string][]string{"region": {"eu-west"}}}

	if !g.Allows("region", "eu-west") {
		t.Error("a listed value was denied")
	}
	if g.Allows("region", "us-east") {
		t.Error("an unlisted value on a known dimension was allowed")
	}
	if g.Allows("businessUnit", "retail") {
		t.Error("a value on a dimension nobody granted was allowed")
	}
	if g.Dimension("businessUnit") != nil {
		t.Error("an unset dimension returned values")
	}
	// nil, not an empty non-nil slice: a caller turning this into a predicate
	// must be able to tell "no values" from "some values" without guessing.
	if got := g.Dimension("region"); !slices.Equal(got, []string{"eu-west"}) {
		t.Errorf("Dimension = %v", got)
	}
}

func TestClaimAuthorizer(t *testing.T) {
	a := web.ClaimAuthorizer{DimensionClaims: map[string]string{
		"region":       "regions",
		"businessUnit": "bu",
	}}

	t.Run("reads dimensions from claims", func(t *testing.T) {
		p := web.Principal{Tenant: "acme", Claims: map[string]any{
			"regions": []any{"eu-west", "eu-north"},
			"bu":      "retail",
		}}
		g, err := a.Authorize(p)
		if err != nil {
			t.Fatal(err)
		}
		if g.Tenant != "acme" {
			t.Errorf("tenant = %q", g.Tenant)
		}
		if !g.Allows("region", "eu-north") || g.Allows("region", "us-east") {
			t.Errorf("regions = %v", g.Dimension("region"))
		}
		// A claim may be a single string as well as an array.
		if !g.Allows("businessUnit", "retail") {
			t.Errorf("businessUnit = %v", g.Dimension("businessUnit"))
		}
	})

	// The tenant is what the database boundary keys on. Continuing without one
	// means running a query whose security depends on a setting nobody made.
	t.Run("refuses a principal with no tenant", func(t *testing.T) {
		_, err := a.Authorize(web.Principal{Subject: "user-1"})
		if !errors.Is(err, web.ErrForbidden) {
			t.Fatalf("err = %v, want ErrForbidden", err)
		}
	})

	// A claim of a shape this code did not understand yields nothing, which
	// denies — the safe direction.
	t.Run("an unreadable claim denies", func(t *testing.T) {
		p := web.Principal{Tenant: "acme", Claims: map[string]any{
			"regions": 42, // not a string, not an array of them
			"bu":      []any{1, 2, 3},
		}}
		g, err := a.Authorize(p)
		if err != nil {
			t.Fatal(err)
		}
		if g.Allows("region", "eu-west") || g.Dimension("region") != nil {
			t.Error("a claim of an unexpected type was read as permitting something")
		}
		if g.Dimension("businessUnit") != nil {
			t.Error("an array of non-strings produced dimension values")
		}
	})

	t.Run("empty strings are not values", func(t *testing.T) {
		p := web.Principal{Tenant: "acme", Claims: map[string]any{
			"regions": []any{"", "eu-west", ""},
			"bu":      "",
		}}
		g, err := a.Authorize(p)
		if err != nil {
			t.Fatal(err)
		}
		if got := g.Dimension("region"); !slices.Equal(got, []string{"eu-west"}) {
			t.Errorf("regions = %v, want only the non-empty one", got)
		}
		if g.Allows("businessUnit", "") {
			t.Error("an empty string was granted as a value")
		}
	})
}

// An authorizer that returns no error and an empty grant is a bug in the
// authorizer. Caught here rather than turned into a query with no principal.
func TestAuthorizeRejectsAnEmptyGrantReturnedWithoutError(t *testing.T) {
	broken := authorizerFunc(func(web.Principal) (web.Grant, error) {
		return web.Grant{}, nil
	})

	g, d, err := web.Authorize(web.Principal{Tenant: "acme"}, broken)
	if err == nil {
		t.Fatal("an empty grant was accepted")
	}
	if !errors.Is(err, web.ErrForbidden) {
		t.Errorf("err = %v, want ErrForbidden", err)
	}
	if !g.IsZero() {
		t.Error("a grant came back from a refusal")
	}
	if d.Code != web.CodeForbidden {
		t.Errorf("diagnostic code = %q", d.Code)
	}
}

// Like authentication, the client is told it was refused and not what to probe
// next. Which dimension fell short stays in the error, for the log.
func TestAuthorizeDoesNotLeakWhichDimensionFailed(t *testing.T) {
	region := authorizerFunc(func(web.Principal) (web.Grant, error) {
		return web.Grant{}, errors.New("no grant on dimension region for eu-west")
	})
	unit := authorizerFunc(func(web.Principal) (web.Grant, error) {
		return web.Grant{}, errors.New("no grant on dimension businessUnit for retail")
	})

	_, d1, err1 := web.Authorize(web.Principal{Tenant: "acme"}, region)
	_, d2, err2 := web.Authorize(web.Principal{Tenant: "acme"}, unit)

	if err1 == nil || err2 == nil {
		t.Fatal("a refusal came back without an error")
	}
	if d1.Code != d2.Code || d1.MessageID != d2.MessageID {
		t.Errorf("the client can tell the two refusals apart: %q/%q vs %q/%q",
			d1.Code, d1.MessageID, d2.Code, d2.MessageID)
	}
	if len(d1.Args) != 0 {
		t.Errorf("the diagnostic carries details a requester could probe with: %v", d1.Args)
	}
}

func TestAuthorizeSucceeds(t *testing.T) {
	a := web.ClaimAuthorizer{}
	g, d, err := web.Authorize(web.Principal{Tenant: "acme"}, a)
	if err != nil {
		t.Fatalf("a valid principal was refused: %v", err)
	}
	if g.Tenant != "acme" {
		t.Errorf("tenant = %q", g.Tenant)
	}
	if d.Code != "" {
		t.Errorf("a diagnostic came back from a success: %+v", d)
	}
}

type authorizerFunc func(web.Principal) (web.Grant, error)

func (f authorizerFunc) Authorize(p web.Principal) (web.Grant, error) { return f(p) }
