package quic

import "time"

// bbrPath drives a BBR controller by hand over a path of one round trip and
// one link rate, doing the delivery estimator's bookkeeping the way
// processAck does: each packet remembers the delivered count at its send,
// and its acknowledgement carries what was delivered since, over the longer
// of the round trip and the link's serialisation of those bytes — so a
// round's last acknowledgement samples the round's rate, capped at the
// link's (draft-ietf-ccwg-bbr-06 §4.1.1.2.4).
type bbrPath struct {
	cc        congestionController // the controller driven, bbr3_test.go's
	now       time.Time
	rtt       time.Duration
	link      float64 // bytes per second
	delivered int
	lost      int  // C.lost
	afterLoss bool // the last datagram off the queue was lost: a loss extends its range
	queue     []bbrSent
	// acks holds the delivered count after each acknowledgement since the
	// last send: the path is ACK-clocked, its k-th datagram sent as the k-th
	// of them arrived, so its sample spans what was delivered since — a
	// round-start sample reads the previous round's flight, as it does on
	// a paced path, not the one datagram a burst sent after a drained pipe
	// would. A loss or a mark clears it: what is sent next was sent after
	// the congestion signal.
	acks []int
	// est is the controller's estimator, for its application-limited mark
	// alone: delivered is kept equal to the path's, the mark is snapshot
	// into each datagram sent, and settle lifts it as finish does.
	est *deliveryEstimator
}

type bbrSent struct {
	sentAt     time.Time
	prior      int
	txInFlight int
	lost       int
	appLimited bool
}

func (p *bbrPath) settle() {
	if p.est.appLimited != 0 && p.est.delivered > p.est.appLimited {
		p.est.appLimited = 0
	}
}

// send puts n datagrams on the path now.
func (p *bbrPath) send(n int) {
	p.settle()
	for i := range n {
		prior := p.delivered
		if i < len(p.acks) {
			prior = p.acks[i]
		}
		p.cc.onSent(p.now, 1200)
		p.queue = append(p.queue, bbrSent{sentAt: p.now, prior: prior, txInFlight: p.cc.bytesInFlight(), lost: p.lost, appLimited: p.est.appLimited != 0})
	}
	p.acks = p.acks[:0]
}

// ack acknowledges the oldest n datagrams now, in order, each by a frame
// of its own: each carries its frame's sample.
func (p *bbrPath) ack(n int) {
	for range n {
		p.cc.onAcked(p.acked(true))
	}
}

// acked takes the oldest datagram off the queue as delivered now: its
// sample reads the link's rate. rated says it is its frame's newest.
func (p *bbrPath) acked(rated bool) ackEvent {
	p.settle()
	s := p.queue[0]
	p.queue = p.queue[1:]
	p.delivered += 1200
	p.est.delivered = p.delivered
	p.acks = append(p.acks, p.delivered)
	p.afterLoss = false
	ev := ackEvent{now: p.now, sentAt: s.sentAt, size: 1200, srtt: p.rtt, delivered: p.delivered - s.prior, appLimited: s.appLimited, rated: rated}
	ev.txInFlight, ev.lost = s.txInFlight, p.lost-s.lost
	ev.sendElapsed = time.Duration(float64(ev.delivered) / p.link * float64(time.Second))
	return ev
}

// lose declares the oldest n datagrams lost now, as one batch: one lost
// range, or none if the datagram before them was lost too, and the
// crossing as declareLost finds it.
func (p *bbrPath) lose(n int) {
	ev := lossEvent{now: p.now, latestSent: p.queue[n-1].sentAt, bytes: n * 1200}
	if !p.afterLoss {
		ev.ranges = 1
	}
	for _, s := range p.queue[:n] {
		p.lost += 1200
		if lost := p.lost - s.lost; ev.crossing.size == 0 && lossTooHigh(lost, s.txInFlight) {
			ev.crossing = lostPacket{size: 1200, txInFlight: s.txInFlight, lost: lost, appLimited: s.appLimited}
		}
	}
	p.queue = p.queue[n:]
	p.afterLoss = true
	p.acks = p.acks[:0]
	p.cc.onLost(ev)
}

// mark reports n CE-marked datagrams now, as one validated ACK frame does.
func (p *bbrPath) mark(n int) {
	p.acks = p.acks[:0]
	p.cc.onMarked(markEvent{now: p.now, packets: n})
}

// round sends n datagrams and acknowledges them a round trip later.
func (p *bbrPath) round(n int) {
	p.send(n)
	p.now = p.now.Add(p.rtt)
	p.ack(n)
}

// bbrAtLink is a controller that has just found a 4.8 MB/s link over 20 ms
// (a bandwidth-delay product of 80 datagrams): four doubling rounds, the
// last at the link's rate. Startup is still on.
