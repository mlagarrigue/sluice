package httpstream

import (
	"testing"

	"github.com/mlagarrigue/sluice/net/quic"
)

// The read-timeout sweep is armed at the assembler's earliest deadline
// (h3Serving.arm) and re-armed after each run at whatever nextExpiry then
// says. expire let a sweep in from the deadline on (it returns only while
// now is before nextExpiry) but failed a stream only once now was after its
// deadline: a sweep whose clock read the deadline exactly failed nothing and
// left nextExpiry where it was — at now. The timer was then re-armed with a
// wait of zero, fired at once, and so on for as long as the clock read that
// instant: for ever on the clock of a testing/synctest bubble, which does
// not move while a goroutine runs. A stream at its deadline fails now.
func TestH3ASweepAtTheDeadlineFailsTheStream(t *testing.T) {
	cfg, err := Config{}.resolve()
	if err != nil {
		t.Fatal(err)
	}
	a := newH3Assembler(cfg)
	request := h3Request("GET", "/")
	if _, _, _, err := a.Frames([]quic.Frame{streamFrame(0, request[:len(request)-2], false)}); err != nil {
		t.Fatal(err)
	}
	deadline := a.nextExpiry
	if deadline.IsZero() {
		t.Fatal("a stream is assembling and no deadline is armed: the test proves nothing")
	}
	failed := a.sweep(deadline) // the timer fired, and the clock reads the deadline
	if len(failed) == 0 && !a.nextExpiry.IsZero() && !a.nextExpiry.After(deadline) {
		t.Fatalf("a sweep at the deadline failed no stream and left the next expiry at the same instant: the timer is re-armed with a wait of zero, and fires again with the clock unmoved")
	}
}
