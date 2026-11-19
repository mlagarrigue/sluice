package quic_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice/net/quic"
)

// ListenerConfig's two keys end connections and mint tokens: printed, under
// any verb or either slog handler, they are hidden, as the connection
// configs of postgres, redis and nats hide their passwords. The rest of the
// configuration still prints, and a zero key — a random one per listener —
// shows as zero.
func TestListenerConfigHidesItsKeys(t *testing.T) {
	var k [32]byte
	for i := range k {
		k[i] = byte(200 + i%50) //nolint:gosec // G115: under 250
	}
	cfg := quic.ListenerConfig{RetryTokenKey: k, StatelessResetKey: k, MaxEarlyDataTickets: 77}
	var js, text bytes.Buffer
	slog.New(slog.NewJSONHandler(&js, nil)).Info("start", "listener", cfg)
	slog.New(slog.NewTextHandler(&text, nil)).Info("start", "listener", cfg)
	for name, out := range map[string]string{
		"%v": fmt.Sprintf("[%v]", cfg), "%+v": fmt.Sprintf("%+v", cfg), "%#v": fmt.Sprintf("%#v", cfg),
		"slog JSON": js.String(), "slog text": text.String(),
	} {
		if strings.Contains(out, "200 201 202") || strings.Contains(out, "200,201,202") {
			t.Errorf("%s printed a key: %.200s", name, out)
		}
		if !strings.Contains(out, "77") {
			t.Errorf("%s lost the rest of the configuration: %.200s", name, out)
		}
	}
	if s := fmt.Sprintf("[%v]", quic.ListenerConfig{}); !strings.Contains(s, "TokenKey:[0 0") {
		t.Errorf("a zero key is not shown as zero: %.200s", s)
	}
}
