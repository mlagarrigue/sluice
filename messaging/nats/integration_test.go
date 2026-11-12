package nats_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/messaging/nats"
)

func dial(tb testing.TB) *nats.Conn {
	tb.Helper()
	addr := os.Getenv("SLUICE_NATS")
	if addr == "" {
		tb.Skip("SLUICE_NATS is not set: no server to run against")
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(tb.Context()), 10*time.Second)
	defer cancel()
	c, err := nats.Connect(ctx, nats.DialTCP(addr), nats.Config{Name: "sluice-test"})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = c.Close() })
	return c
}

// api calls the JetStream API and fails the test on an error it reports.
func api(t *testing.T, c *nats.Conn, subject string, body any) map[string]any {
	t.Helper()
	var b []byte // the API wants an empty payload where it takes none
	if body != nil {
		b, _ = json.Marshal(body)
	}
	m, err := c.Request(t.Context(), subject, b)
	if err != nil {
		t.Fatalf("%s: %v", subject, err)
	}
	var out map[string]any
	if err := json.Unmarshal(m.Data, &out); err != nil || out["error"] != nil {
		t.Fatalf("%s: %s", subject, m.Data)
	}
	return out
}

// jetstream creates a stream and an explicit-ack durable consumer of the
// test's own, holding n messages, and removes the stream afterwards.
func jetstream(t *testing.T, n int, ackWait time.Duration) (*nats.Conn, nats.ConsumerConfig) {
	t.Helper()
	c := dial(t)
	name := "T" + strings.ToUpper(rand.Text())
	api(t, c, "$JS.API.STREAM.CREATE."+name, map[string]any{"name": name, "subjects": []string{name + ".>"}})
	t.Cleanup(func() {
		admin := dial(t)
		_, _ = admin.Request(context.Background(), "$JS.API.STREAM.DELETE."+name, nil)
	})
	api(t, c, "$JS.API.CONSUMER.CREATE."+name+".work", map[string]any{
		"stream_name": name,
		"config":      map[string]any{"durable_name": "work", "ack_policy": "explicit", "ack_wait": ackWait.Nanoseconds()},
	})
	msgs := make([]nats.Message, n)
	for i := range msgs {
		msgs[i] = nats.Message{Subject: name + ".orders", Data: []byte(strconv.Itoa(i))}
	}
	if _, err := c.PublishAcked(t.Context(), msgs); err != nil {
		t.Fatal(err)
	}
	return c, nats.ConsumerConfig{Stream: name, Consumer: "work", BatchMessages: 1000, Expires: 200 * time.Millisecond}
}

func ackPending(t *testing.T, c *nats.Conn, cfg nats.ConsumerConfig) float64 {
	t.Helper()
	info := api(t, c, "$JS.API.CONSUMER.INFO."+cfg.Stream+"."+cfg.Consumer, nil)
	return info["num_ack_pending"].(float64)
}

// 2,500 messages in batches of 1,000; nothing acknowledged until Ack.
func TestConsumerReadsInBatchesAndAcknowledgesOnlyWhatItIsTold(t *testing.T) {
	c, cfg := jetstream(t, 2500, 30*time.Second)
	check := dial(t)
	k, err := c.Consume(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	src := k.Messages(ctx)
	var sizes []int
	seen := 0
	for b := range src.Stream() {
		sizes = append(sizes, len(b.Items))
		for i, m := range b.Items {
			if string(m.Data) != strconv.Itoa(seen+i) {
				t.Fatalf("message %d: %q", seen+i, m.Data)
			}
		}
		seen += len(b.Items)
		if p := ackPending(t, check, cfg); p != float64(len(b.Items)) {
			t.Fatalf("after %d delivered, %v unacknowledged; want this batch's %d", seen, p, len(b.Items))
		}
		for _, m := range b.Items {
			k.Ack(m.Reply)
		}
		if seen == 2500 {
			cancel()
		}
	}
	if err := src.Err(); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(sizes) != "[1000 1000 500]" {
		t.Fatalf("batches %v", sizes)
	}
	time.Sleep(100 * time.Millisecond) // the last acknowledgements, sent without a reply
	if p := ackPending(t, check, cfg); p != 0 {
		t.Fatalf("%v unacknowledged after every batch was", p)
	}
	if _, err := c.Request(t.Context(), "$JS.API.INFO", nil); err != nil {
		t.Fatalf("the connection after the stream stopped: %v", err)
	}
}

// A consumer that stops without acknowledging gets the messages again once
// AckWait has passed.
func TestConsumerRedeliversWhatWasNotAcknowledged(t *testing.T) {
	c, cfg := jetstream(t, 10, 300*time.Millisecond)
	cfg.BatchMessages = 10
	k, _ := c.Consume(cfg)
	var first []string
	for b := range k.Messages(t.Context()).Stream() {
		for _, m := range b.Items {
			first = append(first, string(m.Data))
		}
		k.Ack(b.Items[0].Reply) // one processed before the stop
		break
	}
	if len(first) != 10 {
		t.Fatalf("first read: %v", first)
	}
	time.Sleep(500 * time.Millisecond) // past AckWait
	k, _ = c.Consume(cfg)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var again []string
	src := k.Messages(ctx)
	for b := range src.Stream() {
		for _, m := range b.Items {
			again = append(again, string(m.Data))
			k.Ack(m.Reply)
		}
		if len(again) >= 9 {
			cancel()
		}
	}
	if err := src.Err(); err != nil {
		t.Fatal(err)
	}
	if strings.Join(again, ",") != "1,2,3,4,5,6,7,8,9" {
		t.Fatalf("redelivered %v, want the nine not acknowledged", again)
	}
}

func TestConsumerThatDoesNotExist(t *testing.T) {
	c, cfg := jetstream(t, 0, time.Second)
	cfg.Consumer = "absent"
	k, _ := c.Consume(cfg)
	_, err := k.Messages(t.Context()).Collect()
	var se *nats.StatusError
	if !errors.As(err, &se) {
		t.Fatalf("err %v, want a status", err)
	}
}

// stream creates a stream of the test's own on <name>.>, with extra settings
// merged in, and removes it afterwards.
func stream(t *testing.T, c *nats.Conn, extra map[string]any) string {
	t.Helper()
	name := "T" + strings.ToUpper(rand.Text())
	cfg := map[string]any{"name": name, "subjects": []string{name + ".>"}}
	for k, v := range extra {
		cfg[k] = v
	}
	api(t, c, "$JS.API.STREAM.CREATE."+name, cfg)
	t.Cleanup(func() {
		admin := dial(t)
		_, _ = admin.Request(context.Background(), "$JS.API.STREAM.DELETE."+name, nil)
	})
	return name
}

// 1,000 messages in one write, 1,000 acknowledgements in order.
func TestPublishAckedBatch(t *testing.T) {
	c := dial(t)
	name := stream(t, c, nil)
	msgs := make([]nats.Message, 1000)
	for i := range msgs {
		msgs[i] = nats.Message{Subject: name + ".orders", Data: []byte(strconv.Itoa(i))}
	}
	acks, err := c.PublishAcked(t.Context(), msgs)
	if err != nil {
		t.Fatal(err)
	}
	for i, a := range acks {
		if a.Err != nil || a.Stream != name || a.Sequence != uint64(i+1) {
			t.Fatalf("acknowledgement %d: %+v", i, a)
		}
	}
	if st, _ := api(t, c, "$JS.API.STREAM.INFO."+name, nil)["state"].(map[string]any); st["messages"] != float64(1000) {
		t.Fatalf("the stream holds %v", st["messages"])
	}
}

// A message no stream listens on, and one the stream refuses, each fail in
// their place; the others are stored, and the connection stays usable.
func TestPublishAckedErrorsInTheirPlace(t *testing.T) {
	c := dial(t)
	name := stream(t, c, map[string]any{"max_msgs": 2, "discard": "new"})
	acks, err := c.PublishAcked(t.Context(), []nats.Message{
		{Subject: name + ".a", Data: []byte("0")},
		{Subject: "nowhere." + name, Data: []byte("1")},
		{Subject: name + ".a", Data: []byte("2")},
		{Subject: name + ".a", Data: []byte("3")},
	})
	var se *nats.StatusError
	if !errors.As(err, &se) || se.Code != 503 || !strings.Contains(err.Error(), "2 of 4") {
		t.Fatalf("err %v, want message 1's 503 named, 2 of 4", err)
	}
	if len(acks) != 4 || acks[0].Sequence != 1 || acks[2].Sequence != 2 || !errors.As(acks[1].Err, &se) {
		t.Fatalf("acknowledgements %+v", acks)
	}
	var ae *nats.APIError
	if !errors.As(acks[3].Err, &ae) || ae.ErrCode != 10077 {
		t.Fatalf("past max_msgs with discard new: %+v", acks[3])
	}
	if _, err := c.Request(t.Context(), "$JS.API.INFO", nil); err != nil {
		t.Fatalf("the connection after: %v", err)
	}
}
