package quic_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice/net/quic"
)

// ListenerConfig's two keys, under Advanced, end connections and mint tokens: printed, under
// any verb or either slog handler, they are hidden, as the connection
// configs of postgres, redis and nats hide their passwords. The rest of the
// configuration still prints, the advanced settings printed alone hide them
// too, and a zero key — a random one per listener —
// shows as zero.
func TestListenerConfigHidesItsKeys(t *testing.T) {
	var k [32]byte
	for i := range k {
		k[i] = byte(200 + i%50) //nolint:gosec // G115: under 250
	}
	cfg := quic.ListenerConfig{MaxConns: 77, Advanced: quic.AdvancedListenerConfig{RetryTokenKey: k, StatelessResetKey: k, MaxEarlyDataTickets: 77}}
	var js, text, ajs bytes.Buffer
	slog.New(slog.NewJSONHandler(&js, nil)).Info("start", "listener", cfg)
	slog.New(slog.NewTextHandler(&text, nil)).Info("start", "listener", cfg)
	slog.New(slog.NewJSONHandler(&ajs, nil)).Info("start", "advanced", cfg.Advanced)
	for name, out := range map[string]string{
		"%v": fmt.Sprintf("[%v]", cfg), "%+v": fmt.Sprintf("%+v", cfg), "%#v": fmt.Sprintf("%#v", cfg),
		"slog JSON": js.String(), "slog text": text.String(),
		"Advanced %v": fmt.Sprintf("[%v]", cfg.Advanced), "Advanced %#v": fmt.Sprintf("%#v", cfg.Advanced),
		"Advanced slog JSON": ajs.String(),
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
