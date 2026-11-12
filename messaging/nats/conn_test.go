package nats

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

// scripted starts a server that sends info as its INFO line and reads what
// the client sends until it closes; Connect then runs against it.
func scripted(t *testing.T, info string, cfg Config) error {
	t.Helper()
	server, client := net.Pipe()
	go func() {
		defer server.Close()
		_, _ = server.Write([]byte("INFO " + info + "\r\n"))
		_, _ = server.Read(make([]byte, 4096))
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	c, err := Connect(ctx, func(context.Context) (net.Conn, error) { return client, nil }, cfg)
	if err == nil {
		_ = c.Close()
	}
	return err
}

func TestConnectRefusesTLSMismatch(t *testing.T) {
	err := scripted(t, `{"headers":true,"tls_required":true}`, Config{})
	if err == nil || !strings.Contains(err.Error(), "requires TLS") {
		t.Errorf("a server requiring TLS, no Config.TLS: %v", err)
	}
	err = scripted(t, `{"headers":true}`, Config{TLS: &tls.Config{ServerName: "x"}})
	if err == nil || !strings.Contains(err.Error(), "does not offer TLS") {
		t.Errorf("Config.TLS, a server without TLS: %v", err)
	}
}

func TestPubAckReply(t *testing.T) {
	for _, c := range []struct {
		header, data string
		check        func(PubAck) bool
	}{
		{"", `{"stream":"S","seq":42}`, func(a PubAck) bool { return a.Err == nil && a.Stream == "S" && a.Sequence == 42 }},
		{"NATS/1.0 503\r\n\r\n", "", func(a PubAck) bool {
			var se *StatusError
			return errors.As(a.Err, &se) && se.Code == 503
		}},
		{"", `{"error":{"code":503,"err_code":10077,"description":"maximum messages exceeded"}}`, func(a PubAck) bool {
			var ae *APIError
			return errors.As(a.Err, &ae) && ae.Code == 503 && ae.ErrCode == 10077 && a.Stream == ""
		}},
		{"", `not json`, func(a PubAck) bool { return errors.Is(a.Err, ErrProtocol) }},
		{"", `{}`, func(a PubAck) bool { return errors.Is(a.Err, ErrProtocol) }},
	} {
		if a := pubAck(op{kind: opMsg, header: []byte(c.header), data: []byte(c.data)}); !c.check(a) {
			t.Errorf("%q %q: %+v", c.header, c.data, a)
		}
	}
}

func TestPublishRefusesPastMaxPayload(t *testing.T) {
	c := &Conn{maxPub: 4}
	if err := c.Publish(t.Context(), []Message{{Subject: "a", Data: []byte("12345")}}); !errors.Is(err, ErrTooLarge) {
		t.Errorf("Publish: %v", err)
	}
	if _, err := c.PublishAcked(t.Context(), []Message{{Subject: "a", Data: []byte("1234")}, {Subject: "a", Data: []byte("12345")}}); !errors.Is(err, ErrTooLarge) {
		t.Errorf("PublishAcked: %v", err)
	}
	if _, err := c.Request(t.Context(), "a", []byte("12345")); !errors.Is(err, ErrTooLarge) {
		t.Errorf("Request: %v", err)
	}
}
