// Package settings shows a deployment setting the library's configuration
// from its environment, with the standard library alone, and refusing a bad
// value before it opens a socket.
//
// The library reads no environment variable and parses no flag: what a
// deployment calls its settings, and where it keeps them, is its own
// business. What the library gives is the configuration types —
// [gateway.Config], [httpstream.Config] — whose zero fields mean "the
// default", and a Validate method on each that refuses what the server
// would refuse, with the field's name. A deployment reads its values (here
// with [os.LookupEnv]; [flag] or [encoding/json] would do as well), puts
// them in the types, and calls Validate before it listens. A value that
// cannot be parsed, or that the library would refuse, stops the program at
// start, naming the variable — never after it has taken its port.
//
// In plain words: the environment says "APP_GATEWAY_WITHIN=2ms"; this
// package turns it into gateway.Config.Within, and refuses "-2ms" before
// anything runs.
package settings

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/mlagarrigue/sluice/gateway"
	"github.com/mlagarrigue/sluice/net/httpstream"
)

// The variables this deployment reads. An unset one keeps the value
// [FromEnv] starts from: the library's default for httpstream (zero), and
// a deliberate choice for the gateway, which has no default.
const (
	EnvGatewaySize      = "APP_GATEWAY_SIZE"
	EnvGatewayWithin    = "APP_GATEWAY_WITHIN"
	EnvHTTPReadTimeout  = "APP_HTTP_READ_TIMEOUT"
	EnvHTTPWriteTimeout = "APP_HTTP_WRITE_TIMEOUT" //nolint:gosec // G101: the name of an environment variable, not a credential
	EnvHTTPIdleTimeout  = "APP_HTTP_IDLE_TIMEOUT"
	EnvHTTPMaxBody      = "APP_HTTP_MAX_BODY_BYTES"
)

// Settings is the configuration this deployment runs with.
type Settings struct {
	Gateway gateway.Config
	HTTP    httpstream.Config
}

// FromEnv reads the APP_* variables through lookup — [os.LookupEnv] in a
// program, a map in a test — over the gateway's starting values (64 calls,
// 2 ms) and httpstream's defaults, then validates both configurations. The
// error names every variable that could not be read and every field the
// library refuses; on error, nothing has been started.
func FromEnv(lookup func(string) (string, bool)) (Settings, error) {
	s := Settings{Gateway: gateway.Config{Size: 64, Within: 2 * time.Millisecond}}
	var errs []error
	readInt(lookup, EnvGatewaySize, &s.Gateway.Size, &errs)
	readDuration(lookup, EnvGatewayWithin, &s.Gateway.Within, &errs)
	readDuration(lookup, EnvHTTPReadTimeout, &s.HTTP.ReadTimeout, &errs)
	readDuration(lookup, EnvHTTPWriteTimeout, &s.HTTP.WriteTimeout, &errs)
	readDuration(lookup, EnvHTTPIdleTimeout, &s.HTTP.IdleTimeout, &errs)
	readInt(lookup, EnvHTTPMaxBody, &s.HTTP.MaxBodyBytes, &errs)
	if len(errs) > 0 {
		return Settings{}, errors.Join(errs...)
	}
	if err := errors.Join(s.Gateway.Validate(), s.HTTP.Validate()); err != nil {
		return Settings{}, err
	}
	return s, nil
}

func readInt(lookup func(string) (string, bool), name string, dst *int, errs *[]error) {
	v, ok := lookup(name)
	if !ok {
		return
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s=%q is not an integer", name, v))
		return
	}
	*dst = n
}

func readDuration(lookup func(string) (string, bool), name string, dst *time.Duration, errs *[]error) {
	v, ok := lookup(name)
	if !ok {
		return
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s=%q is not a duration such as 2ms or 10s", name, v))
		return
	}
	*dst = d
}
