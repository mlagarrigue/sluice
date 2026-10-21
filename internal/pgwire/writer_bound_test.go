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
	if err := w.Flush(); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("Flush = %v, want ErrMessageTooLarge", err)
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
	if err := w.Flush(); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("Flush after an oversized in-place message = %v, want ErrMessageTooLarge", err)
	}
	if sink.Len() != 0 {
		t.Errorf("%d bytes were written for an in-place message over the bound", sink.Len())
	}
	if w.Buffered() != 0 {
		t.Errorf("%d bytes left buffered after the refusal", w.Buffered())
	}
}
