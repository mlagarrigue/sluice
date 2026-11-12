package diagnostics_test

import (
	"fmt"

	"github.com/mlagarrigue/sluice/diagnostics"
)

// A path that starts at a field of the element, the usual first step of the
// path a diagnostic names.
func ExampleField() {
	p := diagnostics.Field("customer").Field("email")
	fmt.Println(p, p.JSONPointer())
	// Output: customer.email /customer/email
}
