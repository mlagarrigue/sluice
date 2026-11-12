package quic_test

import (
	"crypto/fips140"
	"errors"
	"os"
	"os/exec"
	"testing"

	"github.com/mlagarrigue/sluice/net/quic"
)

// Under GODEBUG=fips140=only the standard library refuses the AES-GCM
// nonces and the Initial keys of RFC 9001: a listener ran, dropped every
// Initial it could not open and accepted no one, without an error anywhere.
// Each entry point now refuses at once, with an error that wraps
// errors.ErrUnsupported. GODEBUG is read at start-up, so the test runs
// itself again in a child process with the mode on.
func TestFIPSOnlyModeIsRefusedAtTheEntryPoints(t *testing.T) {
	if os.Getenv("SLUICE_FIPS_ONLY_CHILD") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestFIPSOnlyModeIsRefusedAtTheEntryPoints$", "-test.v") //nolint:gosec // G204: the test binary itself
		cmd.Env = append(os.Environ(), "GODEBUG=fips140=only", "SLUICE_FIPS_ONLY_CHILD=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("the child under fips140=only failed: %v\n%s", err, out)
		}
		return
	}
	if !fips140.Enforced() {
		t.Fatal("the child runs without fips140=only enforced")
	}
	_, err := quic.NewListener(nil, nil, quic.DefaultConfig(), quic.ListenerConfig{})
	if !errors.Is(err, errors.ErrUnsupported) || !errors.Is(err, quic.ErrQUIC) {
		t.Errorf("NewListener under fips140=only: %v, want ErrUnsupported and ErrQUIC", err)
	}
	for name, dial := range map[string]func() (*quic.Conn, error){
		"Dial":      func() (*quic.Conn, error) { return quic.Dial(t.Context(), nil, nil, nil, quic.DefaultConfig()) },
		"DialEarly": func() (*quic.Conn, error) { return quic.DialEarly(t.Context(), nil, nil, nil, quic.DefaultConfig()) },
	} {
		if _, err := dial(); !errors.Is(err, errors.ErrUnsupported) {
			t.Errorf("%s under fips140=only: %v, want ErrUnsupported", name, err)
		}
	}
}
