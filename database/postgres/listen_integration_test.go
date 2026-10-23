package postgres_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
)

// Three notifications sent by another session, each in its own transaction,
// reach the listener in the order they were committed, with the sender's PID.
func TestIntegrationListenReceivesNotifications(t *testing.T) {
	addr, cfg := pgTarget(t)
	sender := dialWith(t, addr, cfg)
	channel := "sluice_listen_" + strconv.FormatInt(time.Now().UnixNano()%1_000_000_000, 10)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	l, err := postgres.Listen(ctx, postgres.DialTCP(addr), cfg, postgres.ListenConfig{Channels: []string{channel}})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer l.Close()

	pid := queryText(t, sender, "SELECT pg_backend_pid()::text")
	for _, p := range []string{"one", "two", "three"} {
		if err := sender.Exec(ctx, "NOTIFY "+channel+", '"+p+"'"); err != nil {
			t.Fatal(err)
		}
	}

	var got []string
	src := l.Notifications(ctx)
	src.Stream()(func(b sluice.Batch[postgres.Notification]) bool {
		for _, n := range b.Items {
			if n.Channel != channel || strconv.FormatUint(uint64(n.PID), 10) != pid {
				t.Errorf("notification %+v, want channel %s from PID %s", n, channel, pid)
			}
			got = append(got, n.Payload)
		}
		return len(got) < 3
	})
	if err := src.Err(); err != nil {
		t.Fatalf("stream: %v", err)
	}
	if len(got) != 3 || got[0] != "one" || got[1] != "two" || got[2] != "three" {
		t.Errorf("payloads = %v, want [one two three]", got)
	}
}
