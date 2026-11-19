package stream

import (
	"net/http"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/web"
)

// h3Config gives HTTP/3 an http.Server's bounds with ServeTLS's defaults,
// line by line as ServeH3's documentation says: a zero takes the default, a
// negative value (net/http's "no bound") the native transport's, and a
// field the H3 configuration already set is left alone. A server with a
// read bound and no idle bound waits between requests for its read bound,
// as net/http does over TCP, not for the transport's two minutes.
func TestH3ConfigMapsTheServersBounds(t *testing.T) {
	const s = time.Second
	for _, tc := range []struct {
		name                string
		srv                 *http.Server
		base                httpstream.Config
		read, write, idle   time.Duration
		maxHeaderBytesWants int
	}{
		{"zero server", &http.Server{}, httpstream.Config{}, web.DefaultReadTimeout, web.DefaultWriteTimeout, web.DefaultIdleTimeout, 0}, //nolint:gosec // G112: a server read for its bounds, never served
		{"own bounds", &http.Server{ReadTimeout: 3 * s, WriteTimeout: 4 * s, IdleTimeout: 5 * s, MaxHeaderBytes: 7000}, httpstream.Config{}, 3 * s, 4 * s, 5 * s, 7000},
		{"read bound only", &http.Server{ReadTimeout: 3 * s}, httpstream.Config{}, 3 * s, web.DefaultWriteTimeout, 3 * s, 0},
		{"opt-outs take the transport's default", &http.Server{ReadTimeout: -1, WriteTimeout: -1, IdleTimeout: -1}, httpstream.Config{}, 0, 0, 0, 0},
		{"read opt-out, no idle bound", &http.Server{ReadTimeout: -1}, httpstream.Config{}, 0, web.DefaultWriteTimeout, web.DefaultIdleTimeout, 0},
		{"idle opt-out only", &http.Server{IdleTimeout: -1}, httpstream.Config{}, web.DefaultReadTimeout, web.DefaultWriteTimeout, 0, 0},                            //nolint:gosec // G112: a server read for its bounds, never served
		{"negative header size", &http.Server{MaxHeaderBytes: -1}, httpstream.Config{}, web.DefaultReadTimeout, web.DefaultWriteTimeout, web.DefaultIdleTimeout, 0}, //nolint:gosec // G112: a server read for its bounds, never served
		{
			"the H3 configuration wins", &http.Server{ReadTimeout: 3 * s, WriteTimeout: 4 * s, IdleTimeout: 5 * s, MaxHeaderBytes: 7000},
			httpstream.Config{ReadTimeout: 6 * s, WriteTimeout: 7 * s, IdleTimeout: 8 * s, MaxHeaderBytes: 9000},
			6 * s, 7 * s, 8 * s, 9000,
		},
	} {
		cfg := h3Config(tc.srv, tc.base)
		if cfg.ReadTimeout != tc.read || cfg.WriteTimeout != tc.write || cfg.IdleTimeout != tc.idle || cfg.MaxHeaderBytes != tc.maxHeaderBytesWants {
			t.Errorf("%s: read/write/idle/header = %v/%v/%v/%d, want %v/%v/%v/%d", tc.name,
				cfg.ReadTimeout, cfg.WriteTimeout, cfg.IdleTimeout, cfg.MaxHeaderBytes,
				tc.read, tc.write, tc.idle, tc.maxHeaderBytesWants)
		}
	}
}
