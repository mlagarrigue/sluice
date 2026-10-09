package diagnostics

import (
	"testing"
)

func TestAffordanceIsZero(t *testing.T) {
	tests := []struct {
		name string
		af   Affordance
		want bool
	}{
		{"empty", Affordance{}, true},
		{"an action alone", Affordance{Action: "Fix"}, false},
		{"a path alone", Affordance{Path: Field("qty")}, false},
		{"an input alone", Affordance{Inputs: []Input{{Name: "v"}}}, false},
		{"a method alone", Affordance{Method: "PATCH"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.af.IsZero(); got != tt.want {
				t.Errorf("IsZero = %v, want %v", got, tt.want)
			}
		})
	}
}

// WithInput copies, so an affordance held as a template and extended along two
// branches does not have the branches overwrite each other — the
// append-aliasing trap, and a remedy is exactly the kind of value held as a
// template.
func TestWithInputDoesNotAlias(t *testing.T) {
	base := Affordance{Action: "Sales.Order.CorrectQty"}.
		WithInput(Input{Name: "qty", Kind: "number", Required: true})

	a := base.WithInput(Input{Name: "reason", Kind: "string"})
	b := base.WithInput(Input{Name: "approver", Kind: "string"})

	if len(base.Inputs) != 1 {
		t.Errorf("the template grew to %d inputs", len(base.Inputs))
	}
	if a.Inputs[1].Name != "reason" {
		t.Errorf("branch a's second input is %q", a.Inputs[1].Name)
	}
	if b.Inputs[1].Name != "approver" {
		t.Errorf("branch b's second input is %q — the branches share a backing array", b.Inputs[1].Name)
	}
}
