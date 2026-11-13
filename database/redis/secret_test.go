package redis

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

// A Config handed to a format verb or to either slog handler hides its
// password, and a Conn does not keep the AUTH command once it is sent: the
// write buffer is a field fmt prints with the rest, a byte slice as its
// bytes in decimal, and a later, shorter batch would have left the end of
// the command in place.
func TestThePasswordStaysOutOfPrintsAndLogs(t *testing.T) {
	const password = "correct-horse-battery"
	cfg := Config{User: "app", Password: password, Database: 2}

	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		if got := fmt.Sprintf(verb, cfg); strings.Contains(got, password) {
			t.Errorf("fmt %s of a Config prints the password: %s", verb, got)
		}
	}
	if got := cfg.String(); !strings.Contains(got, "app") || !strings.Contains(got, "<redacted>") {
		t.Errorf("String of a Config: %s, want the user and <redacted>", got)
	}
	if got := (Config{User: "app"}).String(); strings.Contains(got, "<redacted>") {
		t.Errorf("String of a Config without a password: %s, want nothing redacted", got)
	}
	for name, h := range map[string]func(*bytes.Buffer) slog.Handler{
		"text": func(b *bytes.Buffer) slog.Handler { return slog.NewTextHandler(b, nil) },
		"json": func(b *bytes.Buffer) slog.Handler { return slog.NewJSONHandler(b, nil) },
	} {
		var log bytes.Buffer
		slog.New(h(&log)).Info("connecting", "config", cfg)
		if strings.Contains(log.String(), password) {
			t.Errorf("slog's %s handler prints the password of a Config: %s", name, log.String())
		}
	}

	client, server := net.Pipe()
	t.Cleanup(func() { _ = server.Close() })
	go func() {
		r := reader{br: bufio.NewReader(server)}
		for {
			r.arena = nil
			if _, err := r.read(1 << 20); err != nil {
				return
			}
			if _, err := server.Write([]byte("+OK\r\n")); err != nil {
				return
			}
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	c, err := Connect(ctx, func(context.Context) (net.Conn, error) { return client, nil }, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if bytes.Contains(c.bw[:cap(c.bw)], []byte(password)) {
		t.Error("the Conn still holds the AUTH command after Connect returned")
	}
	// A byte slice under %v is its bytes in decimal, one number a byte.
	nums := make([]string, 0, len(password))
	for _, b := range []byte(password) {
		nums = append(nums, strconv.Itoa(int(b)))
	}
	asBytes := strings.Join(nums, " ")
	for _, verb := range []string{"%v", "%+v"} {
		if got := fmt.Sprintf(verb, c); strings.Contains(got, asBytes) {
			t.Errorf("fmt %s of a *Conn prints the password's bytes", verb)
		}
	}
}
