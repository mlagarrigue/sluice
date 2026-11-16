package postgres_test

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/phase"
)

func phaseStore(t *testing.T) (*postgres.PhaseStore, *postgres.Conn) {
	t.Helper()
	addr, cfg := pgTarget(t)
	admin := dialWith(t, addr, cfg)
	schema := testSchema(t, admin)
	return &postgres.PhaseStore{Dial: postgres.DialTCP(addr), Config: cfg, Table: schema + ".etl_phases"}, admin
}

// TestIntegrationAPhaseStoreResumesAJob: a job whose second phase fails is
// resumed at it, its first phase skipped once verified, the record kept in
// the application's table.
func TestIntegrationAPhaseStoreResumesAJob(t *testing.T) {
	store, admin := phaseStore(t)
	rows, broken := int64(0), true
	job := phase.Job{
		Name:  "orders-nightly",
		Input: phase.Input{"day": "2026-11-15"},
		Store: store,
		Phases: []phase.Phase{
			{
				Name: "extract",
				Run:  func(context.Context) (phase.Counts, error) { rows = 10; return phase.Counts{"rows": 10}, nil },
				Verify: func(_ context.Context, c phase.Counts) error {
					if rows != c["rows"] {
						return errors.New("staging changed")
					}
					return nil
				},
			},
			{Name: "load", Run: func(context.Context) (phase.Counts, error) {
				if broken {
					return nil, errors.New("the warehouse is down")
				}
				return phase.Counts{"rows": rows}, nil
			}},
		},
	}
	if _, err := job.Run(t.Context()); err == nil || !strings.Contains(err.Error(), "the warehouse is down") {
		t.Fatalf("first run: %v", err)
	}
	broken = false
	rep, err := job.Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, p := range rep.Phases {
		got = append(got, p.Name+"="+string(p.Outcome))
	}
	if !slices.Equal(got, []string{"extract=skipped", "load=ran"}) {
		t.Errorf("resume: %v", got)
	}
	if status := queryText(t, admin, `SELECT record->'phases'->1->>'status' FROM `+store.Table+` WHERE job = 'orders-nightly'`); status != "done" {
		t.Errorf("the record in the table says %q", status)
	}
}

// TestIntegrationAPhaseStoreLockIsTheServers: a second session is refused
// while the first holds the job; when the holder's session is terminated,
// its next write fails with ErrLeaseLost, and the job can be taken again.
func TestIntegrationAPhaseStoreLockIsTheServers(t *testing.T) {
	store, admin := phaseStore(t)
	held, err := store.Lock(t.Context(), "nightly")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Lock(t.Context(), "nightly"); !errors.Is(err, phase.ErrBusy) {
		t.Errorf("a second session: %v", err)
	}
	other, err := store.Lock(t.Context(), "weekly")
	if err != nil {
		t.Fatalf("another job of the same table: %v", err)
	}
	if err := other.Release(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := held.Save(t.Context(), phase.Record{Fingerprint: "f"}); err != nil {
		t.Fatal(err)
	}
	if rec, found, err := held.Load(t.Context()); err != nil || !found || rec.Fingerprint != "f" {
		t.Fatalf("Load: %+v, %v, %v", rec, found, err)
	}
	// The holder's session ends, as when its process dies or its network
	// goes: the server drops the lock with it.
	// Only that session: its last query named this test's own schema.
	schema, _, _ := strings.Cut(store.Table, ".")
	if got := queryText(t, admin, `SELECT count(pg_terminate_backend(pid))::text FROM pg_stat_activity
		WHERE pid <> pg_backend_pid() AND query LIKE '%`+schema+`%'`); got != "1" {
		t.Fatalf("terminated %s sessions, want the holder's alone", got)
	}
	if err := held.Save(t.Context(), phase.Record{Fingerprint: "g"}); !errors.Is(err, phase.ErrLeaseLost) {
		t.Errorf("a write after the session ended: %v", err)
	}
	if err := held.Release(t.Context()); err != nil {
		t.Errorf("Release of a lost lease: %v", err)
	}
	again, err := store.Lock(t.Context(), "nightly")
	if err != nil {
		t.Fatalf("the job taken again: %v", err)
	}
	if rec, _, err := again.Load(t.Context()); err != nil || rec.Fingerprint != "f" {
		t.Errorf("the record kept: %+v, %v", rec, err)
	}
	if err := again.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestPhaseStoreTableIsAnIdentifier(t *testing.T) {
	for _, table := range []string{"", "a.b.c", "bad name", `x"; DROP TABLE y; --`} {
		s := &postgres.PhaseStore{Table: table}
		if _, err := s.Lock(t.Context(), "job"); err == nil || !strings.Contains(err.Error(), "PhaseStore.Table") {
			t.Errorf("%q: %v", table, err)
		}
	}
	refused := errors.New("refused")
	s := &postgres.PhaseStore{Table: "etl.phases", Config: postgres.Config{User: "etl"}, Dial: func(context.Context) (io.ReadWriteCloser, error) { return nil, refused }}
	if _, err := s.Lock(t.Context(), "job"); !errors.Is(err, refused) {
		t.Errorf("a server that cannot be reached: %v", err)
	}
}
