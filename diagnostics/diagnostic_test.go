package diagnostics

import (
	"errors"
	"testing"
)

func TestSeverityString(t *testing.T) {
	tests := []struct {
		sev  Severity
		want string
	}{
		{Info, "Info"},
		{Warning, "Warning"},
		{Error, "Error"},
		{Critical, "Critical"},
		{Severity(9), "Severity(9)"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			if got := tc.sev.String(); got != tc.want {
				t.Errorf("String = %q, want %q", got, tc.want)
			}
		})
	}
}

// The zero value must be the least alarming severity, so a diagnostic built
// without stating one does not read as Critical.
func TestSeverityZeroIsInfo(t *testing.T) {
	var s Severity
	if s != Info {
		t.Errorf("zero Severity = %v, want Info", s)
	}
}

func TestOriginString(t *testing.T) {
	tests := []struct {
		origin Origin
		want   string
	}{
		{OriginUnknown, "Unknown"},
		{OriginBusinessRule, "BusinessRule"},
		{OriginSQL, "SQL"},
		{OriginHydration, "Hydration"},
		{OriginProtocol, "Protocol"},
		{Origin(9), "Origin(9)"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			if got := tc.origin.String(); got != tc.want {
				t.Errorf("String = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDiagnosticError(t *testing.T) {
	tests := []struct {
		name string
		diag Diagnostic
		want string
	}{
		{
			"with a path",
			NewDiagnostic(Error, "Sales.Order.InvalidQty", Field("lines").Index(7).Field("saleQty")),
			"Error Sales.Order.InvalidQty at lines[7].saleQty",
		},
		{
			"at the root",
			NewDiagnostic(Critical, "Sales.Order.NotFound", Path{}),
			"Critical Sales.Order.NotFound",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.diag.String(); got != tc.want {
				t.Errorf("String = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDiagnosticWithers(t *testing.T) {
	base := NewDiagnostic(Warning, "Sales.Order.OddPrice", Field("price"))
	args := map[string]any{"expected": 10}

	d := base.
		WithMessage("PrixInhabituel", args).
		WithOrigin(OriginBusinessRule).
		WithRule("PriceBand.v2")

	if d.MessageID != "PrixInhabituel" {
		t.Errorf("MessageID = %q, want %q", d.MessageID, "PrixInhabituel")
	}
	if d.Args["expected"] != 10 {
		t.Errorf("Args = %v, want expected=10", d.Args)
	}
	if d.Origin != OriginBusinessRule {
		t.Errorf("Origin = %v, want BusinessRule", d.Origin)
	}
	if d.RuleID != "PriceBand.v2" {
		t.Errorf("RuleID = %q, want %q", d.RuleID, "PriceBand.v2")
	}

	// The withers return copies: the base must be untouched, or a diagnostic
	// held as a template would mutate under its own reuse.
	if base.MessageID != "" || base.Origin != OriginUnknown || base.RuleID != "" {
		t.Error("a With method modified the receiver instead of returning a copy")
	}
}

// The cause is reachable for logging through errors.Is, and unexported so a
// serializer cannot reach it by walking exported fields (guarantee S7).
func TestDiagnosticCauseIsWrappedNotExported(t *testing.T) {
	inner := errors.New("pq: null value in column \"client_id\"")
	d := NewDiagnostic(Critical, "Sales.Order.ClientMustExist", Field("clientID")).
		WithCause(inner)

	if got := d.Unwrap(); !errors.Is(got, inner) {
		t.Errorf("Unwrap = %v, want %v", got, inner)
	}
	// The rendered form is what reaches a log line, and it must not carry the
	// internal text.
	if got := d.String(); got != "Critical Sales.Order.ClientMustExist at clientID" {
		t.Errorf("String = %q, must not include the cause", got)
	}
}

func TestDiagnosticNoCause(t *testing.T) {
	d := NewDiagnostic(Info, "Sales.Order.Normalised", Path{})
	if err := d.Unwrap(); err != nil {
		t.Errorf("Unwrap on a causeless diagnostic = %v, want nil", err)
	}
}

// A Diagnostic must not satisfy the error interface. §4.6 says a warning is not
// a processing error; with an Error method it would be one to the compiler, and
// a Warning returned as an error fires every `if err != nil` it reaches.
//
// The check is a runtime type assertion — an interface satisfaction cannot be
// forbidden at compile time — but the mistake it guards against is a
// compile-time one: someone adding an Error method later.
func TestDiagnosticIsNotAnError(t *testing.T) {
	var d any = Diagnostic{}

	if _, isError := d.(error); isError {
		t.Error("Diagnostic satisfies error: a Warning can now be returned where " +
			"a failure is expected, which is exactly what §4.6 forbids")
	}
	// It is a Stringer instead: the rendering stays available to fmt and logs.
	if _, isStringer := d.(interface{ String() string }); !isStringer {
		t.Error("Diagnostic must render through String for logging")
	}
}
