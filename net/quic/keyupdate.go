package quic

import (
	"fmt"
	"time"
)

// rotateSendKeysLocked moves this end's 1-RTT send keys to the next phase
// (RFC 9001 §6.1, §6.4): new key and IV, the phase bit toggled, the
// confidentiality count restarted, and the old keys dropped at once — a
// sender never protects anything under old keys. The next rotation waits
// for an acknowledgement of a packet sent under these.
func (c *Conn) rotateSendKeysLocked() error {
	sp := &c.spaces[spaceApplication]
	next, err := sp.sealer.k.next()
	if err != nil {
		return err
	}
	sp.sealer = &packetSealer{k: next}
	c.ku.phase ^= keyPhaseBit
	c.ku.firstPN = sp.nextPN
	c.ku.acked = false
	c.ku.updates++
	return nil
}

// canInitiateKeyUpdateLocked is RFC 9001 §6.1's two MUSTs: the handshake
// is confirmed and a packet under the current phase was acknowledged.
func (c *Conn) canInitiateKeyUpdateLocked() bool {
	return c.handshakeConfirmed && c.ku.acked && !c.ku.responseDue
}

// initiateKeyUpdateLocked rotates this end's send keys on its own
// initiative (RFC 9001 §6.1), when permitted. Reports whether it did. The
// read keys need no change: the peer's answer arrives under the next phase
// the opener already holds, and is promoted when it authenticates.
func (c *Conn) initiateKeyUpdateLocked() (bool, error) {
	if !c.canInitiateKeyUpdateLocked() {
		return false, nil
	}
	return true, c.rotateSendKeysLocked()
}

// onKeyPhaseOpenedLocked follows a 1-RTT packet that authenticated under
// the opener's next keys at packet number pn: the peer has moved to that
// phase (RFC 9001 §6.2). The read keys advance, with the old ones kept for
// three probe timeouts of stragglers (§6.5), and the send keys follow if
// they are still in the old phase. A second advance while the answer to
// the first has not carried an acknowledgement is the peer rotating twice
// without waiting — KEY_UPDATE_ERROR, as §6.2 allows and nothing sound
// requires tolerating.
func (c *Conn) onKeyPhaseOpenedLocked(opener *packetOpener, pn uint64, now time.Time) error {
	sp := &c.spaces[spaceApplication]
	if sp.opener != opener {
		// Another packet already advanced the phase under the same keys;
		// this one is read under what is now the current set.
		return nil
	}
	if c.ku.responseDue {
		return &transportError{
			code: transportKeyUpdateError,
			err:  fmt.Errorf("%w: the peer rotated its keys again before this end acknowledged the first rotation", ErrQUIC),
		}
	}
	rotated, err := opener.rotated(pn)
	if err != nil {
		return err
	}
	sp.opener = rotated
	c.ku.dropPrevAt = now.Add(3 * c.rtt.pto(c.peerMaxAckDelayLocked()))
	if c.ku.phase == rotated.phase.bit {
		return nil // this end initiated; the peer's answer completes it
	}
	if err := c.rotateSendKeysLocked(); err != nil {
		return err
	}
	c.ku.responseDue, c.ku.responsePN = true, pn
	return nil
}
