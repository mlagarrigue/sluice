package postgres

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

// A *Conn handed to a format verb, or to slog's text handler, does not
// print its cancellation key's secret. CancelKey hides it under every verb
// (String, GoString), but the Conn keeps the key in an unexported field,
// on which fmt calls no method: the struct was printed field by field, the
// secret's bytes among them — what Config.Logger promises is never logged.
// The control is the key itself.
func TestAPrintedConnHidesItsCancelSecret(t *testing.T) {
	const secret = "s3cr3t-cancel-key-0123456789abcd"
	c := NewConn(&fakeServer{}, Config{})
	c.cancelKey = CancelKey{PID: 4242, Secret: secret}

	// Control: the key itself is redacted under every verb.
	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		if s := fmt.Sprintf(verb, c.CancelKey()); strings.Contains(s, secret) {
			t.Fatalf("control: CancelKey under %s prints its secret: %s", verb, s)
		}
	}

	for _, verb := range []string{"%v", "%+v", "%#v"} {
		if s := fmt.Sprintf(verb, c); strings.Contains(s, secret) {
			t.Errorf("a *Conn under %s prints the cancellation secret", verb)
		}
	}
	var buf bytes.Buffer
	slog.New(slog.NewTextHandler(&buf, nil)).Info("query failed", "conn", c)
	if strings.Contains(buf.String(), secret) {
		t.Error("a *Conn given to slog's text handler logs the cancellation secret")
	}
	// What it prints instead names the backend, and says when there is none.
	var none *Conn
	for _, tc := range []struct {
		c    *Conn
		want string
	}{{c, "postgres.Conn{PID: 4242}"}, {NewConn(&fakeServer{}, Config{}), "postgres.Conn{}"}, {none, "postgres.Conn(nil)"}} {
		if got := fmt.Sprintf("conn %v", tc.c); got != "conn "+tc.want {
			t.Errorf("%%v of the Conn: %q, want %q", got, "conn "+tc.want)
		}
	}
}
