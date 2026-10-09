package batch

// Sentinel is an error an operator raises by panic rather than by bug: the
// named condition an iter.Seq has nowhere to return — a bounded operator at
// its limit, an ordered input seen out of order, a Split branch consumed in
// the wrong order.
//
// sluice.Try recovers exactly these and re-raises everything else. Making
// the type the discriminator, rather than a list of values, is what lets
// every operator package declare its own sentinels without the core knowing
// any of them: a panic whose error chain contains a *Sentinel was raised on
// purpose, and errors.As finds it through any wrapping the raiser added.
type Sentinel struct {
	msg string
}

// NewSentinel declares a sentinel error. The returned value is compared by
// identity, so each sentinel is declared once, as a package-level variable.
func NewSentinel(msg string) error {
	return &Sentinel{msg: msg}
}

func (s *Sentinel) Error() string { return s.msg }
