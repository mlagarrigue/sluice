package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// TestIntegrationJobsLockingAFreshTableAtOnce: jobs of different names take
// different locks, and nothing on the server serialises two CREATE TABLE IF
// NOT EXISTS of the same name — the loser saw 23505, 42P07 or 42710, and Lock
// failed for a job nothing else held. Sixteen jobs lock a fresh table at
// once, forty times over: the race is not certain on any one round.
func TestIntegrationJobsLockingAFreshTableAtOnce(t *testing.T) {
	addr, cfg := pgTarget(t)
	admin := dialWith(t, addr, cfg)
	schema := testSchema(t, admin)
	const sessions, rounds = 16, 40
	for round := range rounds {
		store := &postgres.PhaseStore{
			Dial: postgres.DialTCP(addr), Config: cfg,
			Table: fmt.Sprintf("%s.phases_%d", schema, round),
		}
		errs := make([]error, sessions)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range sessions {
			wg.Go(func() {
				<-start
				lease, err := store.Lock(t.Context(), fmt.Sprintf("job-%d", i))
				if err != nil {
					errs[i] = err
					return
				}
				errs[i] = lease.Release(t.Context())
			})
		}
		close(start)
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("round %d: job-%d, which no other session locks, could not take its lock on a table being created: %v", round, i, err)
			}
		}
	}
}

// muteConn is a connection to the real server that can stop delivering what
// the server answers: once muted, a read waits until the connection is
// closed. Writes still reach the server. It is the state the constat names —
// a server that no longer answers — installed on the client's side of a real
// PostgreSQL rather than forged.
type muteConn struct {
	io.ReadWriteCloser
	muted  *atomic.Bool
	closed chan struct{}
	once   sync.Once
}

func (m *muteConn) Read(p []byte) (int, error) {
	if m.muted.Load() {
		<-m.closed
		return 0, io.ErrClosedPipe
	}
	return m.ReadWriteCloser.Read(p)
}

func (m *muteConn) Close() error {
	m.once.Do(func() { close(m.closed) })
	return m.ReadWriteCloser.Close()
}

// TestIntegrationARunAgainstAMuteServerReturns: the phase ends with its
// caller's deadline while the server has stopped answering. Job.Run then
// writes the failure and releases the lock, a statement each on a connection
// with no deadline of its own, under a context that ends: the write is
// cancelled, the connection closed past CancelTimeout, and Run returns. Under
// context.WithoutCancel alone it waited for as long as the socket lived.
func TestIntegrationARunAgainstAMuteServerReturns(t *testing.T) {
	store, _ := phaseStore(t)
	var (
		muted atomic.Bool
		mu    sync.Mutex
		conns []*muteConn
	)
	realDial := store.Dial
	store.Dial = func(ctx context.Context) (io.ReadWriteCloser, error) {
		rw, err := realDial(ctx)
		if err != nil {
			return nil, err
		}
		m := &muteConn{ReadWriteCloser: rw, muted: &muted, closed: make(chan struct{})}
		mu.Lock()
		conns = append(conns, m)
		mu.Unlock()
		return m, nil
	}
	job := func(name string, mute bool) *phase.Job {
		return &phase.Job{
			Name:  name,
			Store: store,
			Phases: []phase.Phase{{
				Name: "load",
				Run: func(ctx context.Context) (phase.Counts, error) {
					if mute {
						muted.Store(true)
					}
					<-ctx.Done() // the phase ends by its caller's deadline
					return nil, ctx.Err()
				},
			}},
		}
	}
	run := func(name string, mute bool) (returned bool, err error) {
		ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, rerr := job(name, mute).Run(ctx)
			done <- rerr
		}()
		select {
		case err = <-done:
			return true, err
		case <-time.After(15 * time.Second): // a guard, not a measure
			// Release the run: closing the connections ends the reads.
			mu.Lock()
			for _, c := range conns {
				_ = c.Close()
			}
			mu.Unlock()
			<-done
			return false, nil
		}
	}

	// Control: the server answers. The phase fails by its deadline, the
	// failure is recorded, the lock released, and Run returns.
	returned, err := run("mute-control", false)
	if !returned || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("control: returned %v with %v, want the phase's deadline", returned, err)
	}

	// The server stops answering while the phase runs: the failure's write
	// ends by its own bound, the connection past CancelTimeout.
	returned, err = run("mute", true)
	if !returned {
		t.Errorf("fifteen seconds past a 300 ms deadline, Run had not returned: the record of the failed phase is being written under a context nothing ends")
	} else if err == nil {
		t.Errorf("Run returned no error against a server that stopped answering")
	}
}
