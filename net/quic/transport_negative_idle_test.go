package quic

import (
	"strings"
	"testing"
	"time"
)

// A negative MaxIdleTimeout is refused where the caller is. Accepted, it was
// announced to the peer as zero — no timeout — while this end kept the
// negative value and ended every connection after three PTOs of silence,
// the floor its idle deadline is raised to.
func TestValidateRefusesANegativeIdleTimeout(t *testing.T) {
	p := DefaultConfig()
	p.MaxIdleTimeout = -time.Second
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "MaxIdleTimeout") {
		t.Fatalf("Validate = %v, want a refusal naming MaxIdleTimeout", err)
	}
	p.MaxIdleTimeout = 0 // no timeout: allowed by the RFC, and by Validate
	if err := p.Validate(); err != nil {
		t.Errorf("Validate with no idle timeout = %v", err)
	}
}
