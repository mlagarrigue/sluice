package quic

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// The fragment bound of the reassembly (reassembly.go) closes the
// connection with INTERNAL_ERROR stated as such — a transportError carrying
// the reason — not an error closeCodeFor happens to map there. Below the
// floor of 256 bytes held, the floor of 256 fragments applies: the 257th
// disjoint one-byte run trips it, the 256 before it are kept.
func TestFragmentBoundClosesWithAStatedInternalError(t *testing.T) {
	c, _ := newMigratableServer(t, "frag")
	now := time.Now()
	for i := range 256 {
		if err := c.frames(spaceApplication, streamFrame(nil, 0, uint64(2+2*i), "x", false), now); err != nil {
			t.Fatalf("fragment %d of 256 refused: %v", i+1, err)
		}
	}
	err := c.frames(spaceApplication, streamFrame(nil, 0, 2+2*256, "x", false), now)
	te, ok := errors.AsType[*transportError](err)
	if !ok {
		t.Fatalf("the 257th fragment gave %v (%T), want a *transportError with the code stated", err, err)
	}
	if te.code != transportInternalError {
		t.Errorf("code = %#x, want INTERNAL_ERROR (%#x)", te.code, transportInternalError)
	}
	if !strings.Contains(err.Error(), "fragments") {
		t.Errorf("reason = %q, want it to name the fragment bound", err)
	}
}
