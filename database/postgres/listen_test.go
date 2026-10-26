package postgres

import (
	"bytes"
	"errors"
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
	l := &Listener{c: NewConn(rw), batch: 100}
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
// for the life of the subscription. It was the only unbounded list in the
// package: the steady-state loop stops at the batch bound.
func TestListenBoundsWhatArrivesBeforeReadyForQuery(t *testing.T) {
	const batch = 64
	const flood = 20_000

	var script []byte
	script = append(script, frame(pgwire.BackendCommandComplete, "LISTEN\x00")...)
	for range flood {
		script = append(script, beNotification("orders", "x")...)
	}
	// No ReadyForQuery: the stream simply ends.
	rw := &scriptedRW{Reader: bytes.NewReader(script)}
	l := &Listener{c: NewConn(rw), batch: batch}
	sql, err := listenSQL(ListenConfig{Channels: []string{"orders"}, BatchNotifications: batch})
	if err != nil {
		t.Fatal(err)
	}
	err = l.subscribe(sql)
	if len(l.pending) > batch {
		t.Errorf("subscribe kept %d notifications before ReadyForQuery with BatchNotifications = %d (err = %v); nothing bounds it",
			len(l.pending), batch, err)
	}
	if err == nil {
		t.Error("subscribe returned nil although the stream ended before ReadyForQuery")
	}
}

// ListenConfig.BatchNotifications says it is "the most notifications one
// batch holds", and what the subscription kept was handed over as a single
// batch however long it was.
func TestListenBatchHoldsNoMoreThanItsBound(t *testing.T) {
	const batch = 4
	l := &Listener{c: NewConn(&scriptedRW{Reader: bytes.NewReader(nil)}), batch: batch}
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
