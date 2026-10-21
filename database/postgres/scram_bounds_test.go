package postgres

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
)

// The SCRAM bounds are a setting with a safe zero: Config{} keeps the
// defaults, a value set is used as is, and a value that makes no sense is
// refused before anything is sent, with the field named. The refusal is the
// rule the configuration follows everywhere: zero stands for the default,
// a negative was typed, and the typo is fixed by reading the message.
func TestConfigSCRAMBounds(t *testing.T) {
	if b := (Config{}).scramBounds(); b != (scramBounds{4096, 1 << 20, 16}) {
		t.Fatalf("Config{} bounds = %+v, want the defaults", b)
	}
	if b := (Config{MinSCRAMIterations: 1000, MaxSCRAMIterations: 2_000_000, MinSCRAMSaltLen: 8}).scramBounds(); b != (scramBounds{1000, 2_000_000, 8}) {
		t.Fatalf("bounds = %+v, want the values set", b)
	}

	for _, tc := range []struct {
		name string
		cfg  Config
		want string // a substring of the error, or empty for accepted
	}{
		{"defaults", Config{}, ""},
		{"a minimum alone", Config{MinSCRAMIterations: 1000}, ""},
		{"a maximum at the minimum", Config{MinSCRAMIterations: 4096, MaxSCRAMIterations: 4096}, ""},
		{"a negative minimum", Config{MinSCRAMIterations: -1}, "Config.MinSCRAMIterations -1 is negative"},
		{"a negative maximum", Config{MaxSCRAMIterations: -1}, "Config.MaxSCRAMIterations -1 is negative"},
		{"a negative salt length", Config{MinSCRAMSaltLen: -1}, "Config.MinSCRAMSaltLen -1 is negative"},
		{"a maximum below the default minimum", Config{MaxSCRAMIterations: 1000}, "Config.MaxSCRAMIterations 1000 is below Config.MinSCRAMIterations 4096"},
		{"a minimum above the default maximum", Config{MinSCRAMIterations: 2_000_000}, "Config.MaxSCRAMIterations 1048576 is below Config.MinSCRAMIterations 2000000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg
			cfg.User = "u"
			_, err := startupBody(cfg)
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("startupBody = %v, want it accepted", err)
			case tc.want == "":
			case err == nil:
				t.Fatalf("startupBody accepted %+v", tc.cfg)
			case !strings.Contains(err.Error(), tc.want):
				t.Fatalf("error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// A server's first message is judged against the configured bounds, not
// the defaults: a count of 1000 is refused under Config{} and accepted once
// MinSCRAMIterations says so; the refusal names the field to set.
func TestParseServerFirstHonoursConfiguredBounds(t *testing.T) {
	const clientNonce = "abcdefgh"
	shortSalt := base64.StdEncoding.EncodeToString([]byte("eightbyt"))
	for _, tc := range []struct {
		name string
		msg  string
		cfg  Config
		want string
	}{
		{"1000 iterations under the defaults", "r=" + clientNonce + "S,s=" + rfcSalt + ",i=1000", Config{}, "below the 4096 this client accepts (Config.MinSCRAMIterations)"},
		{"1000 iterations once allowed", "r=" + clientNonce + "S,s=" + rfcSalt + ",i=1000", Config{MinSCRAMIterations: 1000}, ""},
		{"2,000,000 iterations under the defaults", "r=" + clientNonce + "S,s=" + rfcSalt + ",i=2000000", Config{}, "above the 1048576 this client accepts (Config.MaxSCRAMIterations)"},
		{"2,000,000 iterations once allowed", "r=" + clientNonce + "S,s=" + rfcSalt + ",i=2000000", Config{MaxSCRAMIterations: 2_000_000}, ""},
		{"an 8-byte salt under the defaults", "r=" + clientNonce + "S,s=" + shortSalt + ",i=4096", Config{}, "below the 16 this client accepts (Config.MinSCRAMSaltLen)"},
		{"an 8-byte salt once allowed", "r=" + clientNonce + "S,s=" + shortSalt + ",i=4096", Config{MinSCRAMSaltLen: 8}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, err := parseServerFirst(tc.msg, clientNonce, tc.cfg.scramBounds())
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("parseServerFirst = %v, want it accepted", err)
			case tc.want == "":
			case err == nil:
				t.Fatalf("parseServerFirst accepted %q under %+v", tc.msg, tc.cfg)
			case !strings.Contains(err.Error(), tc.want):
				t.Fatalf("error = %v, want it to mention %q", err, tc.want)
			case !errors.Is(err, ErrAuth):
				t.Fatalf("error = %v, want it to wrap ErrAuth", err)
			}
		})
	}
}
