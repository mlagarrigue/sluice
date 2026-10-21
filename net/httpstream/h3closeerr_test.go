package httpstream

import (
	"errors"
	"testing"

	"github.com/mlagarrigue/sluice/net/quic"
)

// ServeH3 reports the connection's end the way an HTTP/3 server reads it:
// the client leaving — H3_NO_ERROR, a transport NO_ERROR, silence past the
// idle timeout — is nil; any other cause is the transport's own error,
// unchanged, so the caller still reads it with errors.As.
func TestH3CloseErrKeepsOnlyFailures(t *testing.T) {
	refused := &quic.ConnectionError{Code: 0x2}
	appError := &quic.ConnectionError{Code: 0x0101, Application: true} // H3_GENERAL_PROTOCOL_ERROR
	cases := []struct {
		name string
		in   error
		want error
	}{
		{"still open", nil, nil},
		{"H3_NO_ERROR", &quic.ConnectionError{Code: h3NoError, Application: true}, nil},
		{"transport NO_ERROR", &quic.ConnectionError{Code: 0}, nil},
		{"idle timeout", quic.ErrIdleTimeout, nil},
		{"stateless reset", quic.ErrStatelessReset, quic.ErrStatelessReset},
		{"transport error", refused, refused},
		{"application error", appError, appError},
		{"transport code 0x100 is not H3_NO_ERROR", &quic.ConnectionError{Code: h3NoError}, &quic.ConnectionError{Code: h3NoError}},
		// RFC 9114 §8: an unknown application code is H3_NO_ERROR's
		// equivalent — the reserved 0x1f·N+0x21 codes §8.1 has a peer send
		// in its place, and anything a later document may define — where a
		// defined code other than H3_NO_ERROR is still the failure it names.
		{"a reserved code", &quic.ConnectionError{Code: 0x21 + 0x1f*3, Application: true}, nil},
		{"an undefined code", &quic.ConnectionError{Code: 0x0300, Application: true}, nil},
		{"H3_VERSION_FALLBACK, the last defined", &quic.ConnectionError{Code: h3VersionFallback, Application: true}, &quic.ConnectionError{Code: h3VersionFallback, Application: true}},
		{"QPACK_DECODER_STREAM_ERROR", &quic.ConnectionError{Code: qpackDecoderStreamError, Application: true}, &quic.ConnectionError{Code: qpackDecoderStreamError, Application: true}},
	}
	for _, c := range cases {
		got := h3CloseErr(c.in)
		if (got == nil) != (c.want == nil) || (got != nil && !errors.Is(got, c.in)) {
			t.Errorf("%s: h3CloseErr(%v) = %v, want %v", c.name, c.in, got, c.want)
		}
	}
}
