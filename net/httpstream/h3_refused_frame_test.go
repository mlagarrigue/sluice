package httpstream

import (
	"testing"

	"github.com/mlagarrigue/sluice/internal/varint"
	"github.com/mlagarrigue/sluice/net/quic"
)

// A frame sequence RFC 9114 §4.1 makes a connection error — DATA before
// HEADERS, a control frame on a request stream — is one whatever follows
// it: a HEADERS frame declared over MaxBodyBytes behind it in the same
// delivery used to be refused first, before the frames ahead were judged,
// and the fault was answered on the stream alone (H3_EXCESSIVE_LOAD). The
// control is the same sequence with the HEADERS frame under the bound.
func TestH3AFrameOverTheBoundDoesNotHideTheFaultAheadOfIt(t *testing.T) {
	cut := func(typ, length uint64) []byte {
		return append(varint.Append(varint.Append(nil, typ), length), 1, 2, 3)
	}
	data := appendH3Frame(nil, h3Data, []byte{'x'})
	zero := varint.Append(nil, 0)
	ahead := func(frame []byte) []byte { return append(append([]byte(nil), frame...), cut(h3Headers, 4097)...) }
	for _, tc := range []struct {
		name   string
		stream []byte
	}{
		{"DATA before HEADERS, then HEADERS under the bound (control)", append(append([]byte(nil), data...), cut(h3Headers, 100)...)},
		{"DATA before HEADERS, then HEADERS over MaxBodyBytes", ahead(data)},
		{"SETTINGS, then HEADERS over MaxBodyBytes", ahead(appendH3Frame(nil, h3Settings, nil))},
		{"GOAWAY, then HEADERS over MaxBodyBytes", ahead(appendH3Frame(nil, h3GoAway, zero))},
		{"MAX_PUSH_ID, then HEADERS over MaxBodyBytes", ahead(appendH3Frame(nil, h3MaxPushID, zero))},
		{"CANCEL_PUSH, then HEADERS over MaxBodyBytes", ahead(appendH3Frame(nil, h3CancelPush, zero))},
		{"PUSH_PROMISE from a client, then HEADERS over MaxBodyBytes", ahead(appendH3Frame(nil, h3PushPromise, []byte{0, 0, 0}))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := testAssembler(Config{MaxHeaderBytes: 1024, MaxBodyBytes: 4096, BodyDelivery: BodyWhole})
			batch, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, tc.stream, false)})
			if batch.Len() != 0 {
				t.Fatalf("served %d requests", batch.Len())
			}
			if err == nil {
				t.Fatalf("no connection error; answered on the stream: %v", failed)
			}
			connErr(t, err, h3FrameUnexpected)
		})
	}
}
