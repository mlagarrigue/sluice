package pgwire

import (
	"bytes"
	"errors"
	"testing"
)

// A message past the outbound bound is not framed, and Flush reports it
// before a byte of the batch goes out — the server would close on it, or,
// past 4 GiB, read a wrapped length and lose the stream.
func TestWriterRefusesAMessageOverTheBound(t *testing.T) {
	var sink bytes.Buffer
	w := NewWriter(&sink)
	w.max = 64

	w.Message('Q', make([]byte, 16))
	w.Message('Q', make([]byte, 100))
	if err := w.Flush(); !errors.Is(err, ErrOutgoingTooLarge) {
		t.Fatalf("Flush = %v, want ErrOutgoingTooLarge", err)
	}
	if sink.Len() != 0 {
		t.Errorf("%d bytes were written although the batch held a message over the bound", sink.Len())
	}

	// The writer is usable again, and a message built in place is bounded
	// the same way at End.
	w.Message('Q', make([]byte, 16))
	if err := w.Flush(); err != nil || sink.Len() != 1+4+16 {
		t.Fatalf("after the refusal: Flush = %v, %d bytes written, want the 21-byte message", err, sink.Len())
	}
	sink.Reset()
	at := w.Begin('B')
	w.AppendBytes(make([]byte, 100))
	w.End(at)
	if err := w.Flush(); !errors.Is(err, ErrOutgoingTooLarge) {
		t.Fatalf("Flush after an oversized in-place message = %v, want ErrOutgoingTooLarge", err)
	}
	if sink.Len() != 0 {
		t.Errorf("%d bytes were written for an in-place message over the bound", sink.Len())
	}
	if w.Buffered() != 0 {
		t.Errorf("%d bytes left buffered after the refusal", w.Buffered())
	}
}

// The bound is inclusive and counts only the message being framed: a
// message of exactly the bound goes, whatever the batch already holds, and
// one byte more is refused in every form, the in-place one dropped whole.
func TestWriterBoundIsInclusive(t *testing.T) {
	var sink bytes.Buffer
	w := NewWriter(&sink)
	w.max = 64

	w.Message('Q', make([]byte, 60))
	w.Startup(make([]byte, 60))
	at := w.Begin('B')
	w.AppendBytes(make([]byte, 60))
	w.End(at)
	if err := w.Flush(); err != nil || sink.Len() != 3*1+3*64-1 {
		t.Fatalf("Flush = %v, %d bytes written, want three messages of the bound", err, sink.Len())
	}

	w.Startup(make([]byte, 61))
	if err := w.Flush(); !errors.Is(err, ErrOutgoingTooLarge) {
		t.Fatalf("Startup one byte over: Flush = %v, want ErrOutgoingTooLarge", err)
	}

	w.Message('Q', make([]byte, 8))
	before := w.Buffered()
	at = w.Begin('B')
	w.AppendBytes(make([]byte, 61))
	w.End(at)
	if w.Buffered() != before {
		t.Errorf("Buffered = %d after a refused in-place message, want the %d before it", w.Buffered(), before)
	}
}
