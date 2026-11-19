package httpstream

import (
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

// The examples of RFC 9204 Appendix B, byte for byte from the text, replayed
// against a server that announces a dynamic table capacity of zero. B.1 uses
// the static table only and decodes; B.2 to B.5 build and read a dynamic
// table, and each of their instructions and field sections is refused with
// the code §4.2 and §2.2.3 name — save Stream Cancellation (B.4's 48), the
// one decoder instruction legal without a table.
func TestQPACKAppendixBExamplesAgainstNoDynamicTable(t *testing.T) {
	unhex := func(s string) []byte {
		t.Helper()
		b, err := hex.DecodeString(strings.ReplaceAll(s, " ", ""))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}

	// B.1: a literal with a static name reference, :path=/index.html.
	got, err := newQPACKDecoder(nil, 0).Decode(nil, unhex("0000 510b 2f69 6e64 6578 2e68 746d 6c"))
	if err != nil || len(got) != 1 || string(got[0].Name) != ":path" || string(got[0].Value) != "/index.html" {
		t.Fatalf("B.1: %v, %v; want :path=/index.html", got, err)
	}

	var a h3Assembler
	for _, tc := range []struct{ name, bytes string }{
		{"B.2 Set Dynamic Table Capacity=220", "3fbd01"},
		{"B.2 Insert With Name Reference", "c00f 7777 772e 6578 616d 706c 652e 636f 6d"},
		{"B.3 Insert With Literal Name", "4a63 7573 746f 6d2d 6b65 790c 6375 7374 6f6d 2d76 616c 7565"},
		{"B.4 Duplicate", "02"},
		{"B.5 Insert With Name Reference, dynamic", "810d 6375 7374 6f6d 2d76 616c 7565 32"},
	} {
		_, err := a.encoderInstructions(unhex(tc.bytes))
		var ce h3ConnectionError
		if !errors.As(err, &ce) || ce.Code != qpackEncoderStreamError {
			t.Errorf("encoder stream, %s: %v; want QPACK_ENCODER_STREAM_ERROR", tc.name, err)
		}
	}
	for _, tc := range []struct{ name, bytes string }{
		{"B.2 Section Acknowledgment", "84"},
		{"B.3 Insert Count Increment", "01"},
	} {
		_, err := a.decoderInstructions(unhex(tc.bytes))
		var ce h3ConnectionError
		if !errors.As(err, &ce) || ce.Code != qpackDecoderStreamError {
			t.Errorf("decoder stream, %s: %v; want QPACK_DECODER_STREAM_ERROR", tc.name, err)
		}
	}
	if rest, err := a.decoderInstructions(unhex("48")); err != nil || len(rest) != 0 {
		t.Errorf("decoder stream, B.4 Stream Cancellation: rest %x, %v; want it consumed", rest, err)
	}
	for _, tc := range []struct{ name, bytes string }{
		{"B.2 RIC=2, post-base indexed lines", "0381 10 11"},
		{"B.4 RIC=4, dynamic indexed lines", "0500 80 c1 81"},
	} {
		if _, err := newQPACKDecoder(nil, 0).Decode(nil, unhex(tc.bytes)); !errors.Is(err, ErrH3Protocol) {
			t.Errorf("field section, %s: %v; want it refused", tc.name, err)
		}
	}
}
