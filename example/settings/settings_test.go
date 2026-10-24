package settings_test

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/example/settings"
)

// env is a lookup over a map, the test's stand-in for os.LookupEnv.
func env(vars map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		v, ok := vars[name]
		return v, ok
	}
}

// Every variable reaches its field; an unset one keeps the starting value.
func TestFromEnvSetsWhatTheEnvironmentSays(t *testing.T) {
	s, err := settings.FromEnv(env(map[string]string{
		settings.EnvGatewayWithin:    "5ms",
		settings.EnvHTTPReadTimeout:  "3s",
		settings.EnvHTTPWriteTimeout: "4s",
		settings.EnvHTTPIdleTimeout:  "90s",
		settings.EnvHTTPMaxBody:      "65536",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if s.Gateway.Size != 64 || s.Gateway.Within != 5*time.Millisecond {
		t.Errorf("gateway = %+v, want Size 64 (unset) and Within 5ms", s.Gateway)
	}
	if s.HTTP.ReadTimeout != 3*time.Second || s.HTTP.WriteTimeout != 4*time.Second ||
		s.HTTP.IdleTimeout != 90*time.Second || s.HTTP.MaxBodyBytes != 65536 {
		t.Errorf("http = read %v write %v idle %v body %d", s.HTTP.ReadTimeout, s.HTTP.WriteTimeout, s.HTTP.IdleTimeout, s.HTTP.MaxBodyBytes)
	}
}

// A value out of bounds, or not a value at all, is refused by FromEnv with
// the variable's or the field's name — and FromEnv opens nothing, so the
// refusal comes before the program has a socket. The test checks that too:
// the port it would have listened on is still free afterwards.
func TestFromEnvRefusesOutOfBoundsBeforeListening(t *testing.T) {
	for _, tc := range []struct {
		name string
		vars map[string]string
		want string
	}{
		{"a negative read timeout", map[string]string{settings.EnvHTTPReadTimeout: "-1s"}, "Config.ReadTimeout -1s"},
		{"a negative body bound", map[string]string{settings.EnvHTTPMaxBody: "-1"}, "Config.MaxBodyBytes -1"},
		{"a zero gateway window", map[string]string{settings.EnvGatewayWithin: "0s"}, "Config.Within must be positive"},
		{"a negative gateway size", map[string]string{settings.EnvGatewaySize: "-8"}, "Config.Size must be positive"},
		{"a duration with no unit", map[string]string{settings.EnvGatewayWithin: "2"}, settings.EnvGatewayWithin},
		{"a size that is not a number", map[string]string{settings.EnvGatewaySize: "lots"}, settings.EnvGatewaySize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			addr := ln.Addr().String()
			_ = ln.Close()

			if _, err := settings.FromEnv(env(tc.vars)); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("FromEnv = %v, want an error naming %q", err, tc.want)
			}
			again, err := net.Listen("tcp", addr)
			if err != nil {
				t.Fatalf("the port was taken after a refused configuration: %v", err)
			}
			_ = again.Close()
		})
	}
}

// A deployment reads its settings first and listens second: a bad value
// stops it before the port is taken.
func ExampleFromEnv() {
	lookup := env(map[string]string{ // os.LookupEnv in a real program
		settings.EnvGatewayWithin:   "-2ms",
		settings.EnvHTTPReadTimeout: "5s",
	})
	s, err := settings.FromEnv(lookup)
	if err != nil {
		fmt.Println("refused:", err)
		return
	}
	// Only now: ln, _ := net.Listen("tcp", ":8080"); httpstream.Serve(ctx, ln, s.HTTP, handler)
	fmt.Println("serving with", s.Gateway.Within)
	// Output:
	// refused: gateway: Config.Within must be positive — it is the latency ceiling calls are held under
}
