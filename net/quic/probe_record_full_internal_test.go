package quic

import "testing"

// charges counts the application space's payloads still somewhere: queued
// for retransmission, or carried by a packet of the record.
func charges(c *Conn) (queued, inRecord int) {
	sp := &c.spaces[spaceApplication]
	queued = len(sp.retrans)
	for i := range sp.sent.packets {
		if len(sp.sent.packets[i].payload) > 0 {
			inRecord++
		}
	}
	return queued, inRecord
}

// A probe takes its payloads off the retransmission queue or lifts them off
// the oldest packets in flight before it sends them. With the record at its
// bound, writePacketLocked refuses the packet (errRecordFull): what the
// probe took goes back to the queue, rather than nowhere — a loss declared
// later of a packet whose payload was lifted puts nothing back. The control,
// at the default bound, sends the probe and loses nothing either.
func TestARefusedProbeKeepsEveryPayload(t *testing.T) {
	for _, tc := range []struct {
		name   string
		bound  int
		spaces []int
	}{
		{"control", 0, []int{spaceApplication}},
		{"one space", 2, []int{spaceApplication}},
		{"with the handshake space", 2, []int{spaceHandshake, spaceApplication}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := appConn(t)
			c.mu.Lock()
			defer c.mu.Unlock()
			sp := &c.spaces[spaceApplication]
			sp.sent.record(sentPacket{pn: 0, ackEliciting: true, payload: []byte{framePing}})
			sp.sent.record(sentPacket{pn: 1, ackEliciting: true, payload: []byte{framePing}})
			sp.nextPN = 2
			sp.retrans = [][]byte{{framePing}, {framePing}}
			sp.sent.limit = tc.bound
			q0, r0 := charges(c)
			if q0+r0 != 4 {
				t.Fatalf("setup: %d payloads, want 4", q0+r0)
			}
			c.sendProbesLocked(tc.spaces)
			q1, r1 := charges(c)
			sent := sp.nextPN - 2
			if tc.bound == 0 {
				if sent == 0 || c.recordFull {
					t.Fatalf("control: the probe must go (sent %d, recordFull %v)", sent, c.recordFull)
				}
				if q1+r1 < 4 {
					t.Fatalf("control: a probe that went lost payloads: %d left", q1+r1)
				}
				return
			}
			if !c.recordFull || sent != 0 {
				t.Fatalf("precondition: recordFull %v, %d sent; the bound should refuse the probe", c.recordFull, sent)
			}
			if q1+r1 != 4 {
				t.Errorf("a refused probe left %d of 4 payloads queued or in the record (queued %d, in the record %d)", q1+r1, q1, r1)
			}
		})
	}
}
