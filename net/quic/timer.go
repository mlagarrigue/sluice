package quic

import "time"

// The timer loop: one goroutine per connection that owns every deadline —
// the delayed acknowledgement, the loss timer, the probe timeout and the
// idle timeout. It sleeps until the earliest of them, acts, and recomputes.
// Everything it does is also recomputed when the send or receive path kicks
// it, so a deadline that moved is never waited out at its old value.

// timerLoop runs from newConn until the connection closes.
func (c *Conn) timerLoop() {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		now := time.Now()
		next := c.actOnDeadlines(now)

		var wait time.Duration
		if next.IsZero() {
			wait = time.Hour // nothing armed; a kick will wake us
		} else if wait = next.Sub(now); wait < 0 {
			wait = 0
		}
		// Go ≥1.23 timers: Reset discards a pending expiry, no drain needed.
		timer.Reset(wait)

		select {
		case <-c.closed:
			return
		case <-c.timerKick:
		case <-timer.C:
		}
	}
}

// actOnDeadlines fires whatever matured and returns the earliest deadline
// still pending.
// maxIdleFloor caps §10.1's floor on the idle timeout, which the section
// states as a MUST — "endpoints MUST increase the idle timeout period to be
// at least three times the current Probe Timeout (PTO)" — with no bound on
// the result. The PTO is current() + max(4 x variance, granularity) + the
// peer's max_ack_delay, and §18.2 lets the peer announce a max_ack_delay of
// up to 2^14-1 ms, which the PTO adds whole; the smoothed RTT it can inflate
// by delaying its acknowledgements adds to it with no ceiling of its own.
// Uncapped, the floor therefore passed DefaultParameters' MaxIdleTimeout by
// a factor the peer chose with a single transport parameter, and a
// Listener's connection keeps its routing entry and its MaxConns and
// MaxConnsPerIP slots for the whole of it.
//
// This is the same deviation, for the same root, that maxClosingPeriod takes
// against §10.2's "at least three times the current PTO" (close.go), and the
// same bound: ten seconds is three PTOs of a path with a 2.5 s round trip,
// past any satellite hop. What ending earlier costs is what the MUST buys —
// room for several probes to be sent and lost before the connection is given
// up — and ten seconds is room for three on any path that exists.
//
// The floor is left unconditional otherwise. §10.1 states it without one:
// its "allows for multiple PTOs to expire" is the reason for the rule, not a
// condition on it, and applying the floor only with something in flight
// would be a second deviation where the cap already closes the exposure.
const maxIdleFloor = 10 * time.Second

// idleDeadlineLocked is the idle deadline the connection acts on: the last
// activity plus the effective timeout, floored at three probe timeouts
// (RFC 9000 §10.1) and capped at maxIdleFloor. Zero when no idle timeout
// runs. Every loop that waits for the idle timeout reads it from here, never
// c.idleDeadline: the raw deadline is past while the floor still holds the
// connection, and a loop that waited on it would wake at once and spin,
// holding c.mu at each turn, until the floored one.
//
// On a lossy or high-RTT path the recovery probes still legitimately
// outstanding are the opposite of idleness, and tearing down under them
// abandons a connection that was busy repairing itself: that is what the
// floor is for. Its cap is what §10.1's MUST does not allow for — see
// maxIdleFloor.
func (c *Conn) idleDeadlineLocked() time.Time {
	if c.idleTimeout == 0 || c.idleDeadline.IsZero() {
		return time.Time{}
	}
	deadline := c.idleDeadline
	if floor := min(3*c.rtt.pto(c.peerMaxAckDelayLocked()), maxIdleFloor); c.idleTimeout < floor {
		deadline = deadline.Add(floor - c.idleTimeout)
	}
	return deadline
}

// actOnDeadlines runs in this order: close a connection whose idle timeout
// passed; send the keepalive PING that is due; retransmit an unanswered
// PATH_CHALLENGE; wake a writer parked on the pacer; fire the streams' read
// and write deadlines; re-announce a *_BLOCKED; then, per packet number space,
// see whether its probe timeout matured and send the probes, counting a black
// hole after repeated ones; and return the earliest deadline still ahead.
func (c *Conn) actOnDeadlines(now time.Time) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	// The idle timeout, first: if the connection is dead there is nothing
	// else worth doing.
	if idleDeadline := c.idleDeadlineLocked(); !idleDeadline.IsZero() && !now.Before(idleDeadline) {
		c.mu.Unlock()
		c.idleExpired()
		c.mu.Lock()
		return time.Time{}
	}

	var next time.Time
	earlier := func(t time.Time) {
		if !t.IsZero() && (next.IsZero() || t.Before(next)) {
			next = t
		}
	}

	// The keepalive (Conn.SetKeepAlive): a PING into silence, so that both
	// idle clocks restart (§10.1.2). Only with 1-RTT keys in hand — before
	// them the handshake's own packets are the activity — and rearmed here
	// before the send, so a send that fails cannot make the loop spin.
	if sp := &c.spaces[spaceApplication]; !c.keepAliveDeadline.IsZero() && !sp.discarded && sp.sealer != nil {
		if !now.Before(c.keepAliveDeadline) {
			c.armKeepAliveLocked(now)
			_ = c.sendPacketLocked(spaceApplication, []byte{framePing}, sendOpts{}) // ungated: see sendOpts.waitCwnd
		}
		earlier(c.keepAliveDeadline)
	}

	// An unanswered PATH_CHALLENGE on a migrated path retransmits on its own
	// probe-timeout backoff — the same timer loop, not a second timer system.
	earlier(c.pathProbeDeadlineLocked(now))

	// The pace deadline: a writer parked by sendPacketLocked, or a
	// retransmission pass the pacer stopped. When it matures, run the
	// pass — it arms the deadline again if the bucket still falls short —
	// then wake the writer to re-price behind it. The broadcast is cheap
	// and the writer re-checks everything, so firing early (an ACK moved
	// the deadline) or spuriously costs one loop turn, not correctness.
	// The deadline is read again at the end of this pass: the loss timer
	// below retransmits too, and may arm it.
	if !c.paceDeadline.IsZero() && !now.Before(c.paceDeadline) {
		c.paceDeadline = time.Time{}
		c.retransmitLocked()
		c.cond.Broadcast()
	}

	// A stream's read or write deadline (Stream.SetReadDeadline): the
	// earliest one armed is what sleeps here; when it matures, the streams
	// it holds are woken and the next earliest takes its place.
	if !c.streamDeadline.IsZero() {
		if !now.Before(c.streamDeadline) {
			c.streamDeadline = c.expireStreamDeadlinesLocked(now)
		}
		earlier(c.streamDeadline)
	}

	// A writer parked at a flow-control limit re-announces *_BLOCKED
	// periodically (§4.1): with its one announcement acknowledged it has
	// nothing else in flight, and silence is how a waiting sender gets
	// idle-timed out by the very peer it is waiting on.
	if !c.blockedResend.IsZero() {
		if !now.Before(c.blockedResend) {
			c.resendBlockedFramesLocked(now)
		}
		earlier(c.blockedResend)
	}

	// Spaces whose probe timeout matured this pass. The probes go out after
	// the loop, under one backoff step: RFC 9002 A.9 counts PTO expiries,
	// not spaces probed, and a pass that probes Initial and Handshake
	// together is one expiry — counting it twice quadruples the next
	// interval and reaches the persistent-congestion collapse a probe early.
	var expired [3]bool
	anyExpired := false
	for space := range c.spaces {
		sp := &c.spaces[space]
		if sp.discarded {
			continue
		}

		// A delayed acknowledgement that matured.
		if sp.sealer != nil && sp.ack.owes(now) {
			payload := sp.ack.appendAck(nil, now)
			if len(payload) > 0 {
				_ = c.writePacketLocked(sp, space, payload, nil, sendOpts{ackOnly: true}, now)
			}
		}
		earlier(sp.ack.due)

		// The loss timer: packets an ACK left too young to declare. BBR
		// §4.1.2.4's check at the beginning of a timer that may send,
		// judged before the losses move the flight, run only before the
		// retransmission it decides (see handleAck).
		if lt := sp.sent.lossTime; !lt.IsZero() && !now.Before(lt) {
			idle := c.writers == 0 && c.cc.bytesInFlight() < c.cc.window()
			out := sp.sent.collectTimeLosses(now, &c.rtt, &c.delivery)
			if out.lostCount > 0 {
				if idle && c.checkAppLimitedLocked(now) {
					c.appLimitedResends++
				}
				c.queueLostLocked(sp, out)
				if out.lostBytes > 0 { // see handleAck: a previous path's losses carry no bytes
					c.cc.onLost(out.lossEvent(now))
				}
				c.retransmitLocked()
			}
			c.settleProbeLocked(out)
		}
		earlier(sp.sent.lossTime)

		// The probe timeout: ACKs stopped arriving entirely. Not while a
		// loss timer is armed anywhere (RFC 9002 §6.2.1): the question a
		// probe would ask — is anything getting through? — is one loss
		// detection is already about to answer, sooner.
		if c.anyLossTimerArmedLocked() {
			continue
		}
		if pto := c.ptoDeadlineLocked(sp, space); !pto.IsZero() {
			if !now.Before(pto) {
				expired[space], anyExpired = true, true
			} else {
				earlier(pto)
			}
		}
	}
	if anyExpired {
		c.ptoCount++
		c.ptoFired++
		if c.ptoCount >= pmtuMaxProbes {
			// Nothing acknowledged for three probe intervals: on a path
			// that carried full-size datagrams, that is a black hole
			// until proven otherwise (pmtu.go), and the probes below
			// leave at 1200 bytes.
			c.blackHoleLocked()
		}
		// The window is not touched: a timeout proves nothing about the
		// path (RFC 9002 §6.2), and persistent congestion is a span of
		// losses an ACK establishes (§7.6, handleAck), not a count of
		// probes: a count would collapse it under a sender that pauses
		// between small writes, which restarts this timer at every send
		// without a single acknowledgement (§7.6.1).
		for space, fired := range expired {
			sp := &c.spaces[space]
			if !fired {
				// RFC 9002 §6.2.4: a space whose own timer has not fired
				// is probed along with the one that did when it has data
				// in flight and may be probed at all (ptoDeadlineLocked
				// says when: keys, handshake confirmed for 1-RTT, budget)
				// — the peer may hold receive keys for only one of the
				// two spaces, and would otherwise wait out the other's
				// own, later deadline.
				fired = !sp.discarded && sp.sealer != nil && !c.ptoDeadlineLocked(sp, space).IsZero()
			}
			if fired {
				// The same check before a probe, which resends old data
				// (sendProbeLocked): the probe timeout is the other timer
				// BBR §4.1.2.4 names.
				if c.writers == 0 && c.cc.bytesInFlight() < c.cc.window() && c.checkAppLimitedLocked(now) {
					c.appLimitedResends++
				}
				c.sendProbeLocked(space)
				earlier(c.ptoDeadlineLocked(sp, space))
			}
		}
	}
	earlier(c.paceDeadline)
	earlier(c.idleDeadlineLocked())
	return next
}

// anyLossTimerArmedLocked reports whether any space has a time-threshold
// loss deadline pending, which is what suppresses the probe timeout.
func (c *Conn) anyLossTimerArmedLocked() bool {
	for i := range c.spaces {
		if !c.spaces[i].discarded && !c.spaces[i].sent.lossTime.IsZero() {
			return true
		}
	}
	return false
}

// ptoDeadlineLocked is when space's probe fires: a probe interval after the
// last ack-eliciting packet left, doubled per unanswered probe (§6.2.1).
// Zero when nothing is in flight there — with one exception, the client's
// anti-deadlock arm below.
func (c *Conn) ptoDeadlineLocked(sp *space, space int) time.Time {
	// §6.2.1: no application-space probe before the handshake confirms —
	// the server may not be able to read 1-RTT packets yet, and the
	// Handshake space's own PTO covers the interval.
	if space == spaceApplication && !c.handshakeConfirmed {
		return time.Time{}
	}
	// §6.2.2.1: a server that has spent its anti-amplification budget does
	// not arm the PTO at all — what a probe sends counts against the very
	// limit that stops it — until the client's next datagram reopens the
	// budget; that datagram kicks the timer (receive), and a deadline then
	// found in the past fires at once. The count is left alone meanwhile:
	// climbing with every probe the budget withholds would double the
	// interval of probes that never leave, and make the first one to leave
	// a pile of PINGs.
	if c.amplActive && c.amplSent >= 3*c.amplRecv {
		return time.Time{}
	}
	if _, ok := sp.sent.oldestEliciting(); !ok {
		if len(sp.retrans) == 0 {
			// §6.2.2.1: a client whose flight is fully acknowledged while
			// the server may still be amplification-blocked keeps the PTO
			// armed anyway — the server has nothing it is allowed to send
			// until this end sends again, and two endpoints each waiting
			// for the other is the deadlock the probe exists to break.
			antiDeadlock := c.isClient && !c.peerAddrValidated && space == c.antiDeadlockSpaceLocked()
			if !antiDeadlock {
				return time.Time{}
			}
		}
	}
	maxAckDelay := time.Duration(0)
	if space == spaceApplication {
		maxAckDelay = c.peerMaxAckDelayLocked()
	}
	// RFC 9002 §6.2.1 doubles the PTO at every expiry; the shift is capped
	// at 2^10 so it cannot overflow a Duration, which stops the doubling at
	// the eleventh consecutive PTO. Reached only on a connection kept past
	// its idle timeout with nothing ever acknowledged, and the cost is one
	// probe pair per 1024 further PTOs instead of none.
	interval := c.rtt.pto(maxAckDelay) << min(c.ptoCount, 10)
	base := sp.lastElicit
	if base.IsZero() {
		// The anti-deadlock arm can land on a space nothing has left from
		// yet (Handshake keys installed, Finished not yet sent); the clock
		// then runs from the newest send anywhere.
		for i := range c.spaces {
			if le := c.spaces[i].lastElicit; le.After(base) {
				base = le
			}
		}
	}
	if base.IsZero() {
		return time.Time{}
	}
	return base.Add(interval)
}

// antiDeadlockSpaceLocked is where an unvalidated client's empty-flight
// probe goes: the Handshake space once its keys exist — proof enough for
// the server to validate on — and the Initial space before that (whose
// probes RFC 9000 §14.1 pads to 1200, like any client Initial).
func (c *Conn) antiDeadlockSpaceLocked() int {
	if hs := &c.spaces[spaceHandshake]; !hs.discarded && hs.sealer != nil {
		return spaceHandshake
	}
	if in := &c.spaces[spaceInitial]; !in.discarded && in.sealer != nil {
		return spaceInitial
	}
	return -1
}
