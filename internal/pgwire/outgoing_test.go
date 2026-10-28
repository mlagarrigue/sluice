package pgwire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

// A message too long to send and a message too long to read are two
// failures: the first leaves the stream where it was and no setting moves
// its bound, the second is the end of the connection and Config.MaxMessage
// moves it. One sentinel for both made the connector break a connection
// that had sent nothing and advise the read limit (RP-20261025-pg-02).
func TestOutgoingAndIncomingBoundsAreToldApart(t *testing.T) {
	var out bytes.Buffer
	wr := NewWriter(&out)
	wr.max = 64 // stand-in for MaxOutgoingMessage, so the test allocates nothing
	wr.Message('Q', bytes.Repeat([]byte{'x'}, 128))
	wr.Message('S', nil) // a second, legal message in the same batch
	writeErr := wr.Flush()
	if !errors.Is(writeErr, ErrOutgoingTooLarge) || errors.Is(writeErr, ErrMessageTooLarge) {
		t.Fatalf("Flush = %v, want ErrOutgoingTooLarge and not ErrMessageTooLarge", writeErr)
	}
	if out.Len() != 0 {
		t.Fatalf("Flush wrote %d bytes of a refused batch", out.Len())
	}

	var msg bytes.Buffer
	msg.WriteByte('D')
	_ = binary.Write(&msg, binary.BigEndian, uint32(200))
	msg.Write(bytes.Repeat([]byte{'y'}, 196))
	rd := NewReader(bytes.NewReader(msg.Bytes()))
	rd.SetMaxMessage(64)
	if _, readErr := rd.Next(); !errors.Is(readErr, ErrMessageTooLarge) || errors.Is(readErr, ErrOutgoingTooLarge) {
		t.Fatalf("Next = %v, want ErrMessageTooLarge and not ErrOutgoingTooLarge", readErr)
	}

	wr.Message('S', nil)
	if err := wr.Flush(); err != nil || out.Len() == 0 {
		t.Fatalf("the writer stayed broken: %v, %d bytes out", err, out.Len())
	}
}

// BindLen is what BindBinary frames, so that a caller refusing an
// oversized Bind before buffering refuses exactly what Flush would.
func TestBindLenIsTheFramedLength(t *testing.T) {
	for _, params := range [][][]byte{nil, {nil}, {[]byte("a"), nil, bytes.Repeat([]byte{'b'}, 300)}} {
		var out bytes.Buffer
		wr := NewWriter(&out)
		wr.BindBinary("portal", "stmt1", params)
		if err := wr.Flush(); err != nil {
			t.Fatal(err)
		}
		if got, want := BindLen("portal", "stmt1", params), out.Len()-1; got != want {
			t.Errorf("%d parameters: BindLen %d, the framed Bind is %d bytes past its type byte", len(params), got, want)
		}
	}
}
