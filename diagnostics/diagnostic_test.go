package diagnostics

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
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
		WithMessage("UnusualPrice", args).
		WithOrigin(OriginBusinessRule).
		WithRule("PriceBand.v2")

	if d.MessageID != "UnusualPrice" {
		t.Errorf("MessageID = %q, want %q", d.MessageID, "UnusualPrice")
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
// serializer cannot reach it by walking exported fields (guarantee S7). Both
// halves are checked on what a serializer sees: no exported field of the type
// can hold an error, and encoding/json, given a cause whose own fields are
// exported, writes none of them.
func TestDiagnosticCauseIsWrappedNotExported(t *testing.T) {
	inner := &leakyError{Query: "insert into orders", Detail: "null value in column client_id"}
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

	errType := reflect.TypeFor[error]()
	for f := range reflect.TypeFor[Diagnostic]().Fields() {
		if f.IsExported() && (f.Type == errType || f.Type.Kind() == reflect.Interface && f.Type.NumMethod() == 0 || f.Type.Implements(errType)) {
			t.Errorf("Diagnostic.%s is exported and can hold an error: a serializer walking the fields reaches the cause", f.Name)
		}
	}
	out, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{inner.Query, inner.Detail} {
		if strings.Contains(string(out), secret) {
			t.Errorf("json.Marshal of a diagnostic carries its cause's %q: %s", secret, out)
		}
	}
}

// leakyError is an internal error whose fields are exported, the kind a
// driver returns: a serializer that reached it would write them.
type leakyError struct {
	Query, Detail string
}

func (e *leakyError) Error() string { return e.Detail }

func TestDiagnosticNoCause(t *testing.T) {
	d := NewDiagnostic(Info, "Sales.Order.Normalised", Path{})
	if err := d.Unwrap(); err != nil {
		t.Errorf("Unwrap on a causeless diagnostic = %v, want nil", err)
	}
}

// A Diagnostic must not satisfy the error interface: a warning is not
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
			"a failure is expected, which is exactly what a diagnostic must not say")
	}
	// It is a Stringer instead: the rendering stays available to fmt and logs.
	if _, isStringer := d.(interface{ String() string }); !isStringer {
		t.Error("Diagnostic must render through String for logging")
	}
}
