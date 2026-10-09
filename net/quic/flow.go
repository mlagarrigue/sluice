package quic

import "fmt"

// Flow control, RFC 9000 §4. Two directions, two levels.
//
// Sending honours the peer's limits: stream data stops at what MAX_DATA and
// MAX_STREAM_DATA granted, and the writer *waits* for more credit rather than
// refusing — the same decision the HTTP/2 side records in ADR 0004, made at
// the transport here. Receiving enforces this endpoint's own limits: a peer
// that sends past what it was granted has violated an authenticated protocol
// rule, and that is one of the few things that legitimately ends a
// connection.
//
// Credit is granted back as data is *consumed*, not as it arrives — the gap
// between the two is exactly the memory a slow reader is allowed to occupy,
// which is what makes the receive window the S1 bound for this transport.

// sendQuota is one level's view of how much the peer allows.
type sendQuota struct {
	max         uint64 // the peer's grant, cumulative
	sent        uint64 // consumed against it
	blockedSent bool   // a *_BLOCKED frame went out at this limit already
}

// avail is how much may be sent right now.
func (q *sendQuota) avail() uint64 {
	if q.sent >= q.max {
		return 0
	}
	return q.max - q.sent
}

// grant raises the limit; a peer may re-announce an older one, which means
// nothing and is ignored (§4.1). It reports whether anything was gained.
func (q *sendQuota) grant(limit uint64) bool {
	if limit <= q.max {
		return false
	}
	q.max = limit
	q.blockedSent = false
	return true
}

// recvQuota is one level's view of what this endpoint has granted.
type recvQuota struct {
	max      uint64 // announced to the peer, cumulative
	window   uint64 // how far ahead of consumption the announcement runs
	highest  uint64 // highest offset (or offset sum) received
	consumed uint64 // released to the application
}

func newRecvQuota(window uint64) recvQuota {
	return recvQuota{max: window, window: window}
}

// receive accounts newly received bytes and refuses what was never granted.
func (q *recvQuota) receive(newHighest uint64) error {
	if newHighest <= q.highest {
		return nil
	}
	if newHighest > q.max {
		return fmt.Errorf("%w: the peer sent to offset %d of %d granted", errFlowControl, newHighest, q.max)
	}
	q.highest = newHighest
	return nil
}

// consume releases bytes to the application and reports whether the peer
// should be granted more: once half the window is spent, the announcement is
// renewed rather than letting the sender drain it to zero and stall a round
// trip (§4.2 leaves the policy open; half-window is the usual one).
func (q *recvQuota) consume(n uint64) (grant bool) {
	q.consumed += n
	return q.max-q.consumed < q.window/2
}

// nextMax is the announcement consume asked for.
func (q *recvQuota) nextMax() uint64 {
	q.max = q.consumed + q.window
	return q.max
}

// errFlowControl marks a peer that sent past its grant — a protocol
// violation on an authenticated packet, and connection-fatal by design.
var errFlowControl = fmt.Errorf("%w: flow control violated", ErrQUIC)

// streamLimits picks the initial per-stream credit both ways, which depends
// on who opened the stream and in which direction it runs (§18.2: "local"
// and "remote" are named from the announcer's side).
func initialStreamSendLimit(params TransportParameters, id uint64, isClient bool) uint64 {
	switch {
	case id&0x02 != 0: // unidirectional: only its opener sends
		return params.InitialMaxStreamDataUni
	case (id&0x01 == 0) == isClient:
		// This endpoint opened it, so the peer's "remote" limit applies.
		return params.InitialMaxStreamDataBidiRemote
	default:
		return params.InitialMaxStreamDataBidiLocal
	}
}

func initialStreamRecvLimit(params TransportParameters, id uint64, isClient bool) uint64 {
	switch {
	case id&0x02 != 0:
		return params.InitialMaxStreamDataUni
	case (id&0x01 == 0) == isClient:
		// This endpoint opened it, so its own "local" announcement applies.
		return params.InitialMaxStreamDataBidiLocal
	default:
		return params.InitialMaxStreamDataBidiRemote
	}
}
