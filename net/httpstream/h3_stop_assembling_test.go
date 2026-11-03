package httpstream

import (
	"testing"

	"github.com/mlagarrigue/sluice/net/quic"
)

// A STOP_SENDING on a request stream still assembling cancels the request
// (RFC 9114 §4.1.1) and was charged as one, but assembly went on: completed
// by its DATA and FIN, the request reached the handler, whose response then
// failed on a stream the transport had already reset — and it held its
// admission slot until then. It is dropped now, as a RESET_STREAM drops it.
func TestH3AStopSendingDuringAssemblyDropsTheRequest(t *testing.T) {
	a := testAssembler(Config{BodyDelivery: BodyWhole})
	head := h3Request("POST", "/u", [2]string{"content-length", "4"})
	if _, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, head, false)}); err != nil || len(failed) != 0 {
		t.Fatalf("the request's head: %v, failed %v", err, failed)
	}
	if _, _, _, err := a.Frames([]quic.Frame{{Type: quic.FrameStopSending, StreamID: 0}}); err != nil {
		t.Fatalf("the STOP_SENDING: %v", err)
	}
	body := append(h3DataHead(4), "abcd"...)
	batch, _, _, err := a.Frames([]quic.Frame{streamFrame(0, body, true)})
	if err != nil {
		t.Fatalf("the body: %v", err)
	}
	if batch.Len() != 0 {
		t.Errorf("a request cancelled while assembling reached the handler (%d in the batch)", batch.Len())
	}
	if n := a.inFlight.Load(); n != 0 {
		t.Errorf("%d requests in flight, want none: the cancelled one kept its admission", n)
	}
	if a.cancelled != 1 {
		t.Errorf("cancelled = %d, want one charge", a.cancelled)
	}
}
