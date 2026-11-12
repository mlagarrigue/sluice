package httpstream

import (
	"bufio"
	"io"
	"log/slog"
	"strings"
	"testing"
)

// An invalid header name was quoted whole with %q into the error the warn
// record carries, so 30000 bytes of 0x80 sent by the peer became a log line
// of about four times that. The err attribute stays under the MaxHeaderBytes
// the peer may send, cited by its first bytes and its length; the witness is
// a short invalid name, quoted whole.
func TestAnInvalidHeaderNameIsCitedShortInTheLog(t *testing.T) {
	run := func(name string) (status string, errText string) {
		rec := &logRecorder{}
		addr, _ := serveTest(t, Config{Logger: slog.New(rec)}, echoTarget)
		c := dial(t, addr)
		go func() {
			_, _ = io.WriteString(c, "GET /a HTTP/1.1\r\nHost: h\r\n"+name+": v\r\n\r\n")
		}()
		br := bufio.NewReader(c)
		status, _ = br.ReadString('\n')
		_, _ = io.Copy(io.Discard, br)
		rec.mu.Lock()
		defer rec.mu.Unlock()
		for _, r := range rec.records {
			r.Attrs(func(a slog.Attr) bool {
				if a.Key == "err" {
					errText = a.Value.String()
				}
				return true
			})
		}
		return status, errText
	}

	if status, e := run("bad name"); !strings.HasPrefix(status, "HTTP/1.1 400") || !strings.Contains(e, `"bad name"`) {
		t.Fatalf("witness: status %q, logged err %q; want 400 and the name quoted", status, e)
	}
	const maxHeader = 32 << 10 // the default MaxHeaderBytes
	status, e := run(strings.Repeat("\x80", 30000))
	if !strings.HasPrefix(status, "HTTP/1.1 400") {
		t.Fatalf("status %q, want 400", status)
	}
	if len(e) > maxHeader {
		t.Errorf("a 30000-byte invalid name became a logged err of %d bytes, past the %d-byte header section the peer may send", len(e), maxHeader)
	}
	if !strings.Contains(e, "(30000 bytes)") {
		t.Errorf("logged err %q does not give the name's length", e)
	}
}
