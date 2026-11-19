package redis

import (
	"context"
	"errors"
	"net"
	"testing"
)

// Validate refuses what Connect refuses, before any dial: a configuration
// read at deployment is checked without a server, and the two never
// disagree. Connect refuses it the same way without dialling.
func TestValidateIsWhatConnectRefuses(t *testing.T) {
	noDial := func(context.Context) (net.Conn, error) {
		t.Error("Connect dialled with a configuration Validate refuses")
		return nil, errors.New("not dialled")
	}
	for _, bad := range []Config{{Database: -1}, {MaxReplyBytes: -1}} {
		err := bad.Validate()
		if err == nil {
			t.Errorf("%+v: Validate accepted it", bad)
			continue
		}
		if _, cerr := Connect(t.Context(), noDial, bad); cerr == nil || cerr.Error() != err.Error() {
			t.Errorf("%+v: Validate says %v, Connect %v", bad, err, cerr)
		}
	}
	if err := (Config{}).Validate(); err != nil {
		t.Errorf("the zero Config: %v", err)
	}
}
