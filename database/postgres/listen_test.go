package postgres

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// A notification that lands before the ReadyForQuery closing the LISTEN
// query is kept and delivered first, where Exec would have discarded it; what
// arrives together afterwards is one batch.
func TestListenerKeepsWhatArrivesWhileSubscribing(t *testing.T) {
	var script []byte
	script = append(script, frame(pgwire.BackendCommandComplete, "LISTEN\x00")...)
	script = append(script, beNotification("orders", "early")...)
	script = append(script, frame(pgwire.BackendCommandComplete, "LISTEN\x00")...)
	script = append(script, frame(pgwire.BackendReadyForQuery, "I")...)
	script = append(script, beNotification("orders", "a")...)
	script = append(script, beNotification("audit", "")...)
	rw := &scriptedRW{Reader: bytes.NewReader(script)}
	l := &Listener{c: NewConn(rw, Config{}), batch: 100}
	sql, err := listenSQL(ListenConfig{Channels: []string{"orders", `Au"dit`}})
	if err != nil {
		t.Fatal(err)
	}
	if want := `LISTEN "orders";LISTEN "Au""dit";`; sql != want {
		t.Errorf("sql = %s, want %s", sql, want)
	}
	if err := l.subscribe(sql); err != nil {
		t.Fatal(err)
	}

	var batches [][]Notification
	src := l.Notifications(t.Context())
	src.Stream()(func(b sluice.Batch[Notification]) bool {
		batches = append(batches, append([]Notification(nil), b.Items...))
		return len(batches) < 2
	})
	if err := src.Err(); err != nil {
		t.Fatal(err)
	}
	if len(batches) != 2 || len(batches[0]) != 1 || batches[0][0].Payload != "early" {
		t.Fatalf("batches = %+v, want the early notification alone first", batches)
	}
	if b := batches[1]; len(b) != 2 || b[0] != (Notification{Channel: "orders", Payload: "a", PID: 4242}) || b[1].Channel != "audit" || b[1].Payload != "" {
		t.Errorf("second batch = %+v", b)
	}
}

func TestListenRefuses(t *testing.T) {
	for name, lc := range map[string]ListenConfig{
		"no channel":       {},
		"an empty name":    {Channels: []string{""}},
		"a negative batch": {Channels: []string{"c"}, BatchNotifications: -1},
		"a NUL":            {Channels: []string{"c\x00"}},
	} {
		if _, err := listenSQL(lc); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	for name, body := range map[string][]byte{
		"truncated":  {0, 0, 0, 1, 'c'},
		"trailing":   append(beNotification("c", "p")[5:], 0),
		"no channel": {0, 0},
	} {
		if _, err := parseNotification(body); !errors.Is(err, ErrProtocol) {
			t.Errorf("%s: %v, want ErrProtocol", name, err)
		}
	}
}

// A server that holds back the ReadyForQuery of the LISTEN query and pours
// NotificationResponse used to grow the listener's kept list by one
// Notification — two strings copied, up to MaxMessage each — per message,
// for the life of the subscription. What is kept before ReadyForQuery is
// bounded in bytes, by the package and not by the caller's batch size.
func TestListenBoundsWhatArrivesBeforeReadyForQuery(t *testing.T) {
	payload := strings.Repeat("x", 4000)
	cost := len("orders") + len(payload) + notificationOverhead
	flood := maxPendingNotificationBytes/cost + 10

	var script []byte
	script = append(script, frame(pgwire.BackendCommandComplete, "LISTEN\x00")...)
	for range flood {
		script = append(script, beNotification("orders", payload)...)
	}
	// No ReadyForQuery: the stream simply ends, past the bound.
	rw := &scriptedRW{Reader: bytes.NewReader(script)}
	l := &Listener{c: NewConn(rw, Config{}), batch: 64}
	err := l.subscribe("LISTEN orders;")
	if kept := len(l.pending) * cost; kept > maxPendingNotificationBytes {
		t.Errorf("subscribe kept %d notifications, %d bytes, before ReadyForQuery (err = %v): past the %d-byte bound",
			len(l.pending), kept, err, maxPendingNotificationBytes)
	}
	if !errors.Is(err, ErrProtocol) {
		t.Errorf("subscribe returned %v after %d notifications before ReadyForQuery, want ErrProtocol at the bound", err, flood)
	}
}

// ListenConfig.BatchNotifications says it is "the most notifications one
// batch holds", and what the subscription kept was handed over as a single
// batch however long it was.
func TestListenBatchHoldsNoMoreThanItsBound(t *testing.T) {
	const batch = 4
	l := &Listener{c: NewConn(&scriptedRW{Reader: bytes.NewReader(nil)}, Config{}), batch: batch}
	for i := range batch * 2 {
		l.pending = append(l.pending, Notification{Channel: "orders", Payload: string(rune('a' + i))})
	}
	seen := 0
	for range 2 {
		b, err := l.readBatch()
		if err != nil {
			t.Fatalf("readBatch: %v", err)
		}
		if len(b.Items) > batch {
			t.Fatalf("a batch holds %d notifications, over the bound of %d", len(b.Items), batch)
		}
		for i, n := range b.Items {
			if want := string(rune('a' + seen + i)); n.Payload != want {
				t.Errorf("notification %d of this batch has payload %q, want %q: the order the server sent them in is lost", i, n.Payload, want)
			}
		}
		seen += len(b.Items)
	}
	if seen != batch*2 {
		t.Errorf("%d notifications came back over two batches, want all %d", seen, batch*2)
	}
	if l.pending != nil {
		t.Errorf("%d notifications are still kept after both batches", len(l.pending))
	}
}

// PostgreSQL 18 §54.2.7: a notification may arrive "just before
// ReadyForQuery", and the text bounds neither when nor how many. A server
// that commits notifications between the LISTEN and its answer is
// conformant, and a caller that takes them one at a time
// (BatchNotifications: 1) gets them one at a time — BatchNotifications is
// the size of a batch, not a count past which the server is refused.
func TestListenDeliversWhatArrivesBeforeReadyForQueryInBatches(t *testing.T) {
	var script []byte
	script = append(script, frame(pgwire.BackendCommandComplete, "LISTEN\x00")...)
	script = append(script, beNotification("c", "a")...)
	script = append(script, beNotification("c", "b")...)
	script = append(script, frame(pgwire.BackendReadyForQuery, "I")...)
	l := &Listener{c: NewConn(&scriptedRW{Reader: bytes.NewReader(script)}, Config{}), batch: 1}
	if err := l.subscribe("LISTEN c;"); err != nil {
		t.Fatalf("subscribe refused two notifications before ReadyForQuery under BatchNotifications = 1: %v", err)
	}
	for _, want := range []string{"a", "b"} {
		b, err := l.readBatch()
		if err != nil {
			t.Fatalf("readBatch: %v", err)
		}
		if len(b.Items) != 1 || b.Items[0].Payload != want {
			t.Fatalf("a batch of %v, want the one notification %q", b.Items, want)
		}
	}
}

// A ParameterStatus may arrive at any point of a session, and every read loop
// of this package records it — which is also where the one guard that
// depends on it lives: a client_encoding nobody asked for ends the
// connection, since channel names and payloads would otherwise be read as
// UTF-8 without error. A listening connection used to ignore the message,
// guard and all.
func TestListenEndsAtAnEncodingNobodyAskedFor(t *testing.T) {
	var script []byte
	script = append(script, beNotification("orders", "first")...)
	script = append(script, frame(pgwire.BackendParameterStatus, "client_encoding\x00LATIN1\x00")...)
	script = append(script, beNotification("orders", "caf\xe9")...)
	l := &Listener{c: NewConn(&scriptedRW{Reader: bytes.NewReader(script)}, Config{}), batch: 1}
	var got []string
	src := l.Notifications(t.Context())
	err := src.Consume(func(b sluice.Batch[Notification]) bool {
		for _, n := range b.Items {
			got = append(got, n.Payload)
		}
		return true
	})
	if err == nil || !strings.Contains(err.Error(), "client_encoding") {
		t.Fatalf("listening past a client_encoding of LATIN1 ended with %v, want the encoding named", err)
	}
	if len(got) != 1 || got[0] != "first" {
		t.Fatalf("delivered %q, want only the notification before the announcement", got)
	}
}

// A channel, slot or publication name past 63 bytes is refused, with its
// field and the reason: PostgreSQL truncates a name there, and two channels
// that differ only past it became one subscription in silence — the server's
// NOTICE says so, and the Listener drops notices.
// At 63 bytes a name is taken.
func TestNamesPastTheServersTruncationAreRefused(t *testing.T) {
	long, fits := strings.Repeat("c", 64), strings.Repeat("c", 63)
	for _, tc := range []struct {
		field string
		build func(name string) error
	}{
		{"ListenConfig.Channels", func(n string) error {
			_, err := listenSQL(ListenConfig{Channels: []string{"orders", n}})
			return err
		}},
		{"ReplicationConfig.Slot", func(n string) error {
			_, err := startReplicationSQL(ReplicationConfig{Slot: n, Publications: []string{"p"}})
			return err
		}},
		{"ReplicationConfig.Publications", func(n string) error {
			_, err := startReplicationSQL(ReplicationConfig{Slot: "s", Publications: []string{"p", n}})
			return err
		}},
	} {
		err := tc.build(long)
		if err == nil || !strings.Contains(err.Error(), tc.field) || !strings.Contains(err.Error(), "truncates a name at 63") {
			t.Errorf("%s with a 64-byte name: %v, want a refusal naming the field and the truncation", tc.field, err)
		}
		if err := tc.build(fits); err != nil {
			t.Errorf("%s with a 63-byte name: %v, want it taken", tc.field, err)
		}
	}
}
