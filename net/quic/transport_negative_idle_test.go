package quic

import (
	"strings"
	"testing"
	"time"
)

// A MaxIdleTimeout that is not positive is refused where the caller is. A
// negative one was announced to the peer as zero — no timeout — while this
// end kept the negative value and ended every connection after three PTOs
// of silence, the floor its idle deadline is raised to. Zero itself, which
// RFC 9000 §10.1 reads as no timeout, left a connection whose peer had
// vanished — the peer announcing none too — held for good, in a Listener's
// tables as well. A long duration is the way to
// ask for a long life.
func TestValidateRefusesAnIdleTimeoutThatIsNotPositive(t *testing.T) {
	for _, v := range []time.Duration{-time.Second, 0} {
		p := DefaultConfig()
		p.MaxIdleTimeout = v
		if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "MaxIdleTimeout") {
			t.Errorf("Validate(MaxIdleTimeout %v) = %v, want a refusal naming MaxIdleTimeout", v, err)
		}
		if _, err := NewListener(nil, nil, p, ListenerConfig{}); err == nil || !strings.Contains(err.Error(), "MaxIdleTimeout") {
			t.Errorf("NewListener(MaxIdleTimeout %v) = %v, want a refusal naming MaxIdleTimeout", v, err)
		}
	}
	p := DefaultConfig()
	p.MaxIdleTimeout = 24 * time.Hour
	if err := p.Validate(); err != nil {
		t.Errorf("Validate with a day's idle timeout = %v", err)
	}
}
