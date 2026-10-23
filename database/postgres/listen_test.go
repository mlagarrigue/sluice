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
