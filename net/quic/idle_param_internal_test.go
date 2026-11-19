package quic

import (
	"testing"
	"time"
)

// max_idle_timeout travels in milliseconds, and zero there means no idle
// timeout. Validate refuses a value that is not positive for that reason; a
// positive one under a millisecond is truncated to the same zero on the
// wire, and it passed. One millisecond, the smallest the wire carries, is
// still accepted.
func TestASubMillisecondIdleTimeoutIsRefused(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxIdleTimeout = 500 * time.Microsecond
	if err := cfg.Validate(); err == nil {
		p, perr := parseParameters(appendParameters(nil, cfg.TransportParameters))
		t.Errorf("Validate accepted %v, which announces %v on the wire (parse error %v)", cfg.MaxIdleTimeout, p.MaxIdleTimeout, perr)
	}
	cfg.MaxIdleTimeout = time.Millisecond
	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate refused one millisecond: %v", err)
	}
}
