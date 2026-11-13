package nats

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
// password, and a Conn does not keep the CONNECT line that carried it: the
// write buffer is a field fmt prints with the rest.
func TestThePasswordStaysOutOfPrintsAndLogs(t *testing.T) {
	const password = "correct-horse-battery"
	cfg := Config{User: "app", Password: password, Name: "orders"}

	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		if got := fmt.Sprintf(verb, cfg); strings.Contains(got, password) {
			t.Errorf("fmt %s of a Config prints the password: %s", verb, got)
		}
	}
	if got := cfg.String(); !strings.Contains(got, "orders") || !strings.Contains(got, "<redacted>") {
		t.Errorf("String of a Config: %s, want the name and <redacted>", got)
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
		_, _ = server.Write([]byte("INFO {\"headers\":true}\r\n"))
		br := bufio.NewReader(server)
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			if strings.HasPrefix(line, "PING") {
				_, _ = server.Write([]byte("PONG\r\n"))
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
	if bytes.Contains(c.wb[:cap(c.wb)], []byte(password)) {
		t.Error("the Conn still holds the CONNECT line after Connect returned")
	}
	// A byte slice under %v is its bytes in decimal, one number a byte.
	nums := make([]string, 0, len(password))
	for _, b := range []byte(password) {
		nums = append(nums, strconv.Itoa(int(b)))
	}
	asBytes := strings.Join(nums, " ")
	for _, verb := range []string{"%v", "%+v"} {
		if got := fmt.Sprintf(verb, c); strings.Contains(got, asBytes) {
			t.Errorf("fmt %s of a *Conn prints the password's bytes, kept from its CONNECT line", verb)
		}
	}
}
