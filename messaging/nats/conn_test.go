package nats

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// scripted starts a server that sends info as its INFO line and keeps what
// the client sends until it closes; Connect then runs against it. It returns
// the bytes the server received and Connect's error.
func scripted(t *testing.T, info string, cfg Config) ([]byte, error) {
	t.Helper()
	server, client := net.Pipe()
	got := make(chan []byte, 1)
	go func() {
		defer server.Close()
		_, _ = server.Write([]byte("INFO " + info + "\r\n"))
		b, _ := io.ReadAll(server)
		got <- b
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	c, err := Connect(ctx, func(context.Context) (net.Conn, error) { return client, nil }, cfg)
	if err == nil {
		_ = c.Close()
	} else {
		_ = client.Close()
	}
	return <-got, err
}

// A TLS mismatch is refused before anything is sent (Config.TLS): not the
// error's text alone, but nothing on the wire — a CONNECT written before the
// refusal would carry the credentials in clear to a server without TLS, or
// to anything between that struck tls_available from the INFO.
func TestConnectRefusesTLSMismatch(t *testing.T) {
	for _, c := range []struct {
		info, want string
		cfg        Config
	}{
		{`{"headers":true,"tls_required":true}`, "requires TLS", Config{Password: "secret"}},
		{`{"headers":true}`, "does not offer TLS", Config{Password: "secret", TLS: &tls.Config{ServerName: "x"}}},
	} {
		sent, err := scripted(t, c.info, c.cfg)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v", c.info, err)
		}
		if len(sent) != 0 {
			t.Errorf("%s: the client sent %q before refusing", c.info, sent)
		}
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
