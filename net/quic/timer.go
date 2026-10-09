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
func (c *Conn) actOnDeadlines(now time.Time) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	// The idle timeout, first: if the connection is dead there is nothing
	// else worth doing. The effective deadline is floored at three probe
	// timeouts past the last activity (RFC 9000 §10.1): on a lossy or
	// high-RTT path the recovery probes still legitimately outstanding are
	// the opposite of idleness, and tearing down under them abandons a
	// connection that was busy repairing itself.
	idleDeadline := c.idleDeadline
	if c.idleTimeout != 0 && !idleDeadline.IsZero() {
		if floor := 3 * c.rtt.pto(c.peerMaxAckDelayLocked()); c.idleTimeout < floor {
			idleDeadline = idleDeadline.Add(floor - c.idleTimeout)
		}
		if !now.Before(idleDeadline) {
			c.mu.Unlock()
			c.idleExpired()
			c.mu.Lock()
			return time.Time{}
		}
	}

	var next time.Time
	earlier := func(t time.Time) {
		if !t.IsZero() && (next.IsZero() || t.Before(next)) {
			next = t
		}
	}

	// An unanswered PATH_CHALLENGE on a migrated path retransmits on its own
	// probe-timeout backoff — the same timer loop, not a second timer system.
	earlier(c.pathProbeDeadlineLocked(now))

	// A pacer-blocked writer parked by sendPacketLocked: when its deadline
	// matures, wake it to re-price. The broadcast is cheap and the writer
	// re-checks everything, so firing early (an ACK moved the deadline) or
	// spuriously costs one loop turn, not correctness.
	if !c.paceDeadline.IsZero() {
		if !now.Before(c.paceDeadline) {
			c.paceDeadline = time.Time{}
			c.cond.Broadcast()
		} else {
			earlier(c.paceDeadline)
		}
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

		// The loss timer: packets an ACK left too young to declare.
		if lt := sp.sent.lossTime; !lt.IsZero() && !now.Before(lt) {
			lost, lostBytes, latestSent := sp.sent.collectTimeLosses(now, &c.rtt)
			if lostBytes > 0 {
				sp.retrans = append(sp.retrans, lost...)
				c.cc.onLost(lostBytes, latestSent, now)
				c.retransmitLocked()
			}
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
		// Three unanswered probes with exponential backoff: the path is
		// not dropping a packet, it is gone. Collapse the window (a
		// simplification of RFC 9002 §7.6's persistent-congestion test,
		// on the safe side of it).
		if c.ptoCount >= 3 {
			c.cc.onPersistentCongestion()
		}
		for space, fired := range expired {
			if fired {
				c.sendProbeLocked(space)
				earlier(c.ptoDeadlineLocked(&c.spaces[space], space))
			}
		}
	}
	if c.idleTimeout != 0 {
		earlier(c.idleDeadline)
	}
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
