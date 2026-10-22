package quic

import (
	"bytes"
	"io"
	"testing"
	"time"
)

// A packet that carries only a STOP_SENDING reaches the batch callback: the
// frame used to be answered by the transport and dropped, so an application
// whose stream the peer refused to read — HTTP/3's control stream, a
// connection error under RFC 9114 §6.2.1 — learned of it only at the next
// datagram that carried stream data. A retransmission of the same frame is
// not handed up a second time.
func TestStopSendingAloneReachesTheCallback(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	defer c.Close()

	// Application keys, so the RESET_STREAM that answers the STOP_SENDING
	// (RFC 9000 §3.5) has somewhere to go.
	sec, err := initialSecrets([]byte("appkeys0"))
	if err != nil {
		t.Fatal(err)
	}
	sealer, err := newPacketSealer(sec.Client)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.spaces[spaceApplication].sealer = sealer
	c.peerMaxStreamsBidi = 1 // the grant the handshake would have brought
	c.mu.Unlock()

	var got [][]Frame
	c.OnStreamFrames(func(frames []Frame) error {
		got = append(got, cloneFrames(frames))
		return nil
	})
	s, err := c.Stream(0) // this client's first bidirectional stream
	if err != nil {
		t.Fatal(err)
	}

	payload := appendFrame(nil, FrameStopSending, 0, 0x10c)
	if err := c.frames(spaceApplication, payload, time.Now()); err != nil {
		t.Fatalf("STOP_SENDING on a local stream: %v", err)
	}
	if len(got) != 1 || len(got[0]) != 1 {
		t.Fatalf("the callback saw %v, want one batch of one frame", got)
	}
	if f := got[0][0]; f.Type != FrameStopSending || f.StreamID != 0 {
		t.Fatalf("the callback saw type %#x on stream %d, want STOP_SENDING (%#x) on stream 0",
			f.Type, f.StreamID, FrameStopSending)
	}
	if code, reset := s.WasReset(); !reset || code != 0x10c {
		t.Fatalf("WasReset = (%#x, %v), want (0x10c, true)", code, reset)
	}

	if err := c.frames(spaceApplication, payload, time.Now()); err != nil {
		t.Fatalf("the retransmitted STOP_SENDING: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("the retransmitted STOP_SENDING reached the callback again: %v", got)
	}
}

// A peer that answers a request whole — its bytes, its FIN — and refuses
// the rest of ours with STOP_SENDING, all in one packet, is read to the end:
// nginx's HTTP/0.9 server does exactly this. The STOP_SENDING ended the last
// open direction of a stream this end opened, which retired it before the
// packet's stream data was dispatched, and the 1024 bytes were dropped; the
// reader waited for them until the idle timeout, its own deadline unheard,
// since the deadline timer walks the open streams.
func TestStopSendingBehindTheAnswerKeepsTheAnswer(t *testing.T) {
	// No read loop: frames is fed here alone, as the read loop would.
	client := newConn(&recordingPC{addr: "stop:self"}, fakeFuzzAddr("stop:peer"),
		[]byte("peercid0"), []byte("selfcid0"), true, DefaultParameters())
	t.Cleanup(func() { _ = client.Close() })
	installAppSealer(t, client)
	client.mu.Lock()
	client.hasPeerParams, client.peerParams = true, DefaultParameters()
	client.peerMaxStreamsBidi = 1
	client.connSend.grant(1 << 20)
	client.handshakeConfirmed = true
	client.mu.Unlock()
	cs, err := client.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cs.WriteClose([]byte("GET /x\r\n")); err != nil {
		t.Fatal(err)
	}
	answer := make([]byte, 1024)
	for i := range answer {
		answer[i] = byte(i)
	}
	payload := appendFrame(nil, FrameStream|streamOFF|streamLEN, cs.id, 0, uint64(len(answer)))
	payload = append(payload, answer...)
	payload = appendFrame(payload, FrameStream|streamOFF|streamLEN|streamFIN, cs.id, uint64(len(answer)), 0)
	payload = appendFrame(payload, FrameStopSending, cs.id, 0)
	if err := client.frames(spaceApplication, payload, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := cs.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(cs)
	if err != nil || !bytes.Equal(got, answer) {
		t.Fatalf("read %d bytes, %v; want the %d-byte answer and its end", len(got), err, len(answer))
	}
}
