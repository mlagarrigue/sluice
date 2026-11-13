package postgres

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// Config.String hides the password from fmt and from slog's text handler;
// the JSON handler calls neither String nor GoString and marshalled the
// exported fields, Password among them. LogValue is what both handlers ask.
func TestTheJSONHandlerDoesNotLogTheConfigsPassword(t *testing.T) {
	const password = "correct-horse-battery"
	cfg := Config{User: "app", Password: password}
	for name, h := range map[string]func(*bytes.Buffer) slog.Handler{
		"text": func(b *bytes.Buffer) slog.Handler { return slog.NewTextHandler(b, nil) },
		"json": func(b *bytes.Buffer) slog.Handler { return slog.NewJSONHandler(b, nil) },
	} {
		var log bytes.Buffer
		slog.New(h(&log)).Info("connecting", "config", cfg)
		if strings.Contains(log.String(), password) {
			t.Errorf("slog's %s handler prints the password of a Config: %s", name, log.String())
		}
		if !strings.Contains(log.String(), "<redacted>") {
			t.Errorf("slog's %s handler: %s, want the Config with <redacted>", name, log.String())
		}
	}
}
