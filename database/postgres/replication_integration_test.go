package postgres_test

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
)

// queryText runs a query whose single column is text and returns the first
// row's value.
func queryText(t *testing.T, conn *postgres.Conn, sql string) string {
	t.Helper()
	var out string
	src := conn.Query(t.Context(), sql, nil, postgres.QueryConfig{})
	src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
		if len(b.Items) > 0 && out == "" {
			v, _ := b.Items[0].Value(0)
			out = string(v) // copied: the row is valid for this call only
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return out
}

// A slot is read end to end: a transaction of three inserts arrives as Begin,
// three Inserts in order and a Commit, the values decode with pgtype, and the
// position acknowledged after the commit is the one the server holds for the
// slot once the stream has stopped.
func TestIntegrationReplicationReadsASlot(t *testing.T) {
	addr, cfg := pgTarget(t)
	conn := dialWith(t, addr, cfg)
	if lvl := queryText(t, conn, "SELECT current_setting('wal_level')"); lvl != "logical" {
		t.Skipf("wal_level is %q, not logical: the server cannot replicate logically", lvl)
	}
	if queryText(t, conn, "SELECT rolreplication::text FROM pg_roles WHERE rolname = current_user") != "true" {
		t.Skip("the role lacks the REPLICATION attribute")
	}

	suffix := strconv.FormatInt(time.Now().UnixNano()%1_000_000_000, 10)
	table, pub, slot := "sluice_repl_"+suffix, "sluice_repl_pub_"+suffix, "sluice_repl_slot_"+suffix
	ctx := t.Context()
	for _, sql := range []string{
		"CREATE TABLE " + table + " (id bigint PRIMARY KEY, note text)",
		"CREATE PUBLICATION " + pub + " FOR TABLE " + table,
	} {
		if err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	queryText(t, conn, "SELECT pg_create_logical_replication_slot('"+slot+"', 'pgoutput')::text")
	t.Cleanup(func() {
		ctx := context.Background()
		_ = conn.Exec(ctx, "SELECT pg_drop_replication_slot('"+slot+"')")
		_ = conn.Exec(ctx, "DROP PUBLICATION "+pub)
		_ = conn.Exec(ctx, "DROP TABLE "+table)
	})
	if err := conn.Exec(ctx, "INSERT INTO "+table+" VALUES (1, 'one'), (2, 'two'), (3, NULL)"); err != nil {
		t.Fatal(err)
	}

	rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	repl, err := postgres.Replicate(rctx, postgres.DialTCP(addr), cfg, postgres.ReplicationConfig{
		Slot: slot, Publications: []string{pub},
	})
	if err != nil {
		t.Fatalf("Replicate: %v", err)
	}
	defer repl.Close()

	var kinds []postgres.ChangeKind
	var ids []int64
	var notes []string
	var lastLSN, end postgres.LSN
	src := repl.Changes(rctx)
	src.Stream()(func(b sluice.Batch[postgres.Change]) bool {
		for _, c := range b.Items {
			kinds = append(kinds, c.Kind)
			switch c.Kind {
			case postgres.ChangeInsert:
				if c.Relation == nil || c.Relation.Name != table || len(c.New) != 2 {
					t.Fatalf("Insert on %+v with %d values", c.Relation, len(c.New))
				}
				if c.LSN < lastLSN {
					t.Errorf("Insert at %v after one at %v", c.LSN, lastLSN)
				}
				lastLSN = c.LSN
				id, err := pgtype.DecodeInt8(c.New[0].Data)
				if err != nil || c.New[0].Status != postgres.ValueBinary {
					t.Fatalf("id: %v, status %v", err, c.New[0].Status)
				}
				ids = append(ids, id)
				if c.New[1].Status == postgres.ValueNull {
					notes = append(notes, "NULL")
				} else {
					notes = append(notes, pgtype.DecodeText(c.New[1].Data))
				}
			case postgres.ChangeCommit:
				end = c.EndLSN
				repl.Ack(end)
				return false
			}
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("stream: %v", err)
	}
	if want := []postgres.ChangeKind{postgres.ChangeBegin, postgres.ChangeInsert, postgres.ChangeInsert, postgres.ChangeInsert, postgres.ChangeCommit}; !slices.Equal(kinds, want) {
		t.Errorf("kinds = %v, want %v", kinds, want)
	}
	if fmt.Sprint(ids) != "[1 2 3]" || strings.Join(notes, ",") != "one,two,NULL" {
		t.Errorf("rows = %v %v, want [1 2 3] [one two NULL]", ids, notes)
	}
	if err := repl.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}

	// The slot's confirmed position is the acknowledged one, read once the
	// walsender has let go of the slot.
	var confirmed postgres.LSN
	for range 50 {
		s := queryText(t, conn, "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = '"+slot+"' AND NOT active")
		if s != "" {
			if confirmed, err = postgres.ParseLSN(s); err != nil {
				t.Fatal(err)
			}
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if confirmed < end {
		t.Errorf("confirmed_flush_lsn = %v after acknowledging %v", confirmed, end)
	}
}

// A Type and an Origin come from a real server as decoded, not only as a
// scripted stream encodes them: a column of an enum the publishing database
// defined is preceded by a Type naming it, and a transaction marked with a
// replication origin is followed, after its Begin, by an Origin naming the
// node and the commit's position there (pgoutput's 'Y' and 'O').
func TestIntegrationReplicationDeliversTypeAndOrigin(t *testing.T) {
	addr, cfg := pgTarget(t)
	conn := dialWith(t, addr, cfg)
	if lvl := queryText(t, conn, "SELECT current_setting('wal_level')"); lvl != "logical" {
		t.Skipf("wal_level is %q, not logical: the server cannot replicate logically", lvl)
	}
	if queryText(t, conn, "SELECT rolreplication::text FROM pg_roles WHERE rolname = current_user") != "true" {
		t.Skip("the role lacks the REPLICATION attribute")
	}

	suffix := strconv.FormatInt(time.Now().UnixNano()%1_000_000_000, 10)
	mood, table, pub, slot, origin := "sluice_repl_mood_"+suffix, "sluice_repl_"+suffix, "sluice_repl_pub_"+suffix, "sluice_repl_slot_"+suffix, "sluice_repl_origin_"+suffix
	ctx := t.Context()
	for _, sql := range []string{
		"CREATE TYPE " + mood + " AS ENUM ('calm', 'busy')",
		"CREATE TABLE " + table + " (id bigint PRIMARY KEY, m " + mood + ")",
		"CREATE PUBLICATION " + pub + " FOR TABLE " + table,
	} {
		if err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	queryText(t, conn, "SELECT pg_create_logical_replication_slot('"+slot+"', 'pgoutput')::text")
	queryText(t, conn, "SELECT pg_replication_origin_create('"+origin+"')::text")
	t.Cleanup(func() {
		ctx := context.Background()
		_ = conn.Exec(ctx, "SELECT pg_replication_origin_session_reset()")
		_ = conn.Exec(ctx, "SELECT pg_replication_origin_drop('"+origin+"')")
		_ = conn.Exec(ctx, "SELECT pg_drop_replication_slot('"+slot+"')")
		_ = conn.Exec(ctx, "DROP PUBLICATION "+pub)
		_ = conn.Exec(ctx, "DROP TABLE "+table)
		_ = conn.Exec(ctx, "DROP TYPE "+mood)
	})
	// The session replays as the origin; the transaction carries the
	// position the origin committed it at, which the Origin message reports.
	const originLSN = "0/2A00"
	queryText(t, conn, "SELECT pg_replication_origin_session_setup('"+origin+"')::text")
	tx, err := conn.Begin(ctx, postgres.TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		"SELECT pg_replication_origin_xact_setup('" + originLSN + "', now())",
		"INSERT INTO " + table + " VALUES (1, 'busy')",
	} {
		if err := tx.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	repl, err := postgres.Replicate(rctx, postgres.DialTCP(addr), cfg, postgres.ReplicationConfig{
		Slot: slot, Publications: []string{pub},
	})
	if err != nil {
		t.Fatalf("Replicate: %v", err)
	}
	defer repl.Close()

	var kinds []postgres.ChangeKind
	var typ postgres.DataType
	var from string
	var fromLSN postgres.LSN
	src := repl.Changes(rctx)
	src.Stream()(func(b sluice.Batch[postgres.Change]) bool {
		for _, c := range b.Items {
			kinds = append(kinds, c.Kind)
			switch c.Kind {
			case postgres.ChangeType:
				typ = c.Type
			case postgres.ChangeOrigin:
				from, fromLSN = c.Origin, c.LSN
			case postgres.ChangeCommit:
				repl.Ack(c.EndLSN)
				return false
			}
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("stream: %v", err)
	}
	if !slices.Contains(kinds, postgres.ChangeType) || typ.Name != mood || typ.OID == 0 {
		t.Errorf("Type = %+v in %v, want the enum %s", typ, kinds, mood)
	}
	want, err := postgres.ParseLSN(originLSN)
	if err != nil {
		t.Fatal(err)
	}
	if len(kinds) < 2 || kinds[1] != postgres.ChangeOrigin || from != origin || fromLSN != want {
		t.Errorf("Origin = %q at %v in %v, want %q at %v right after the Begin", from, fromLSN, kinds, origin, want)
	}
}
