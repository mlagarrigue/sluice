package diagnostics_test

import (
	"errors"
	"fmt"

	"github.com/mlagarrigue/sluice/diagnostics"
)

func ExamplePath() {
	p := diagnostics.Field("lines").Index(7).Field("saleQty")

	// Two renderings of one value: readable for a log, RFC 6901 for a wire.
	fmt.Println(p)
	fmt.Println(p.JSONPointer())
	// Output:
	// lines[7].saleQty
	// /lines/7/saleQty
}

func ExamplePath_HasPrefix() {
	line7 := diagnostics.Field("lines").Index(7)

	// Grouping by subtree is why a Path keeps its segments: string matching
	// would report lines[70] as being under lines[7].
	fmt.Println(diagnostics.Field("lines").Index(7).Field("saleQty").HasPrefix(line7))
	fmt.Println(diagnostics.Field("lines").Index(70).HasPrefix(line7))
	// Output:
	// true
	// false
}

func ExampleNewDiagnostic() {
	d := diagnostics.NewDiagnostic(diagnostics.Error, "Sales.Order.InvalidQty",
		diagnostics.Field("lines").Index(7).Field("saleQty")).
		WithMessage("QteMustBeOverZero", map[string]any{"min": 1}).
		WithOrigin(diagnostics.OriginBusinessRule)

	// The rendered form is for a log line. What travels to a client is the
	// message key and its named arguments, rendered in the reader's language.
	fmt.Println(d)
	fmt.Println(d.MessageID, d.Args["min"])
	// Output:
	// Error Sales.Order.InvalidQty at lines[7].saleQty
	// QteMustBeOverZero 1
}

func ExampleDiagnostic_Unwrap() {
	// A NOT NULL constraint at read time becomes a business diagnostic, with
	// the driver error kept for the log and unreachable from a serializer.
	pqErr := errors.New(`pq: null value in column "client_id"`)

	d := diagnostics.NewDiagnostic(diagnostics.Critical, "Sales.Order.ClientMustExist",
		diagnostics.Field("clientID")).
		WithOrigin(diagnostics.OriginSQL).
		WithCause(pqErr)

	// The diagnostic is safe to render for a client; the driver error is
	// reachable only by asking for it explicitly, which is what keeps it out of
	// a response by accident.
	fmt.Println(d)
	fmt.Println(errors.Is(d.Unwrap(), pqErr))
	// Output:
	// Critical Sales.Order.ClientMustExist at clientID
	// true
}

func ExampleCollector() {
	// A ceiling is required, not defaulted: one pathological batch can produce
	// tens of thousands of diagnostics, and the interesting one is buried.
	diags := diagnostics.NewCollector(2)

	for i := range 5 {
		diags.Add(diagnostics.NewDiagnostic(diagnostics.Warning, "Sales.Order.OddPrice",
			diagnostics.Field("lines").Index(i).Field("unitPrice")))
	}

	for _, d := range diags.All() {
		fmt.Println(d)
	}
	fmt.Println("dropped:", diags.Truncated())
	// Output:
	// Warning Sales.Order.OddPrice at lines[0].unitPrice
	// Warning Sales.Order.OddPrice at lines[1].unitPrice
	// dropped: 3
}

func ExampleCollector_Worst() {
	diags := diagnostics.NewCollector(10)
	diags.Add(diagnostics.NewDiagnostic(diagnostics.Info, "Sales.Order.Normalised", diagnostics.Path{}))
	diags.Add(diagnostics.NewDiagnostic(diagnostics.Critical, "Sales.Order.ClientMustExist", diagnostics.Field("clientID")))

	// The question a caller actually asks: did anything serious happen here?
	worst, found := diags.Worst()
	fmt.Println(worst, found)
	// Output:
	// Critical true
}

// An Affordance says what can be done about a value, at the same Path as the
// diagnostic saying what is wrong with it. Pairing them is a comparison rather
// than a convention, which is what lets a client render the remedy beside the
// problem without knowing what either means.
func ExampleAffordance() {
	at := diagnostics.Field("lines").Index(7).Field("saleQty")

	diags := diagnostics.NewCollector(16)
	diags.Add(diagnostics.NewDiagnostic(diagnostics.Error, "Sales.Order.InvalidQty", at).
		WithMessage("QtyMustBeOverZero", nil).
		WithOrigin(diagnostics.OriginBusinessRule))

	remedy := diagnostics.Affordance{
		Path:   at,
		Action: "Sales.Order.CorrectQty",
		Method: "PATCH",
		Target: "/orders/42/lines/7",
	}.WithInput(diagnostics.Input{Name: "saleQty", Kind: "number", Required: true})
	remedies := []diagnostics.Affordance{remedy}

	problem := diags.All()[0]
	fmt.Println("problem:", problem.Code, "at", problem.Path)
	for _, r := range remedies {
		if r.Path.HasPrefix(problem.Path) {
			fmt.Println("remedy: ", r.Method, r.Target, "takes", r.Inputs[0].Name)
		}
	}
	// Output:
	// problem: Sales.Order.InvalidQty at lines[7].saleQty
	// remedy:  PATCH /orders/42/lines/7 takes saleQty
}
