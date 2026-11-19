package phase_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/phase"
)

// world is what a test's phases write and read back: a working table per
// phase, as a count of rows, and the order in which phases ran.
type world struct {
	rows map[string]int64
	ran  []string
	fail map[string]error // a phase that returns this error
}

func newWorld() *world { return &world{rows: map[string]int64{}, fail: map[string]error{}} }

// phase is a phase that writes n rows to its table, and whose Verify
// checks the table still holds what was recorded.
func (w *world) phase(name string, n int64) phase.Phase {
	return phase.Phase{
		Name: name,
		Run: func(context.Context) (phase.Counts, error) {
			w.ran = append(w.ran, name)
			if err := w.fail[name]; err != nil {
				return nil, err
			}
			w.rows[name] = n // emptied, then filled: it can run again
			return phase.Counts{"rows": n}, nil
		},
		Verify: func(_ context.Context, recorded phase.Counts) error {
			if w.rows[name] != recorded["rows"] {
				return fmt.Errorf("%s holds %d rows, %d recorded", name, w.rows[name], recorded["rows"])
			}
			return nil
		},
	}
}

func (w *world) job(dir string, day string) *phase.Job {
	return &phase.Job{
		Name:         "orders-nightly",
		Input:        phase.Input{"day": day},
		LogicVersion: 1,
		Store:        phase.Dir(dir),
		Phases:       []phase.Phase{w.phase("extract", 10), w.phase("transform", 8), w.phase("load", 8)},
	}
}

func outcomes(r phase.Report) []string {
	var out []string
	for _, p := range r.Phases {
		out = append(out, p.Name+"="+string(p.Outcome))
	}
	return out
}

// TestAJobResumesWhereItDied: the second phase fails; the next run skips
// the first, verified, and runs the second and the third.
func TestAJobResumesWhereItDied(t *testing.T) {
	dir, w := t.TempDir(), newWorld()
	w.fail["transform"] = errors.New("the source went away")
	rep, err := w.job(dir, "2026-11-15").Run(t.Context())
	if err == nil || !strings.Contains(err.Error(), `phase "transform": the source went away`) {
		t.Fatalf("err = %v", err)
	}
	if got := outcomes(rep); !slices.Equal(got, []string{"extract=ran"}) {
		t.Errorf("first run: %v", got)
	}

	delete(w.fail, "transform")
	w.ran = nil
	rep, err = w.job(dir, "2026-11-15").Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got := outcomes(rep); !slices.Equal(got, []string{"extract=skipped", "transform=ran", "load=ran"}) || rep.RestartReason != "" {
		t.Errorf("resume: %v, restarted %q", got, rep.RestartReason)
	}
	if !slices.Equal(w.ran, []string{"transform", "load"}) {
		t.Errorf("ran %v", w.ran)
	}
	if _, err := os.Stat(filepath.Join(dir, "orders-nightly.lease")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the lease outlived the run: %v", err)
	}
}

// TestADoneJobRunsAgainOnlyWhatItCannotVerify: a job run again after it
// finished skips the phases whose Verify passes and runs the one without.
func TestADoneJobRunsAgainOnlyWhatItCannotVerify(t *testing.T) {
	dir, w := t.TempDir(), newWorld()
	job := w.job(dir, "2026-11-15")
	job.Phases[2].Verify = nil
	if _, err := job.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	rep, err := job.Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got := outcomes(rep); !slices.Equal(got, []string{"extract=skipped", "transform=skipped", "load=ran"}) ||
		rep.Phases[2].Why != "it has no Verify" {
		t.Errorf("%v, %+v", got, rep.Phases[2])
	}
}

// TestAFailedVerifyRunsThatPhaseAndEveryLaterOne: an unlogged table
// emptied by a crash — the record says done, the table says otherwise.
func TestAFailedVerifyRunsThatPhaseAndEveryLaterOne(t *testing.T) {
	dir, w := t.TempDir(), newWorld()
	if _, err := w.job(dir, "2026-11-15").Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	w.rows["transform"] = 0
	rep, err := w.job(dir, "2026-11-15").Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got := outcomes(rep); !slices.Equal(got, []string{"extract=skipped", "transform=ran", "load=ran"}) {
		t.Errorf("%v", got)
	}
	if !strings.Contains(rep.Phases[1].Why, "its Verify failed: transform holds 0 rows, 8 recorded") ||
		rep.Phases[2].Why != "an earlier phase, transform, ran again" {
		t.Errorf("%+v", rep.Phases)
	}
}

// TestAChangedInputStartsOverAndSaysWhy: the next day, a new logic
// version, a phase added — each discards the record and names what
// changed; Fresh does with the same input.
func TestAChangedInputStartsOverAndSaysWhy(t *testing.T) {
	dir, w := t.TempDir(), newWorld()
	if _, err := w.job(dir, "2026-11-14").Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		edit func(*phase.Job)
		want string
	}{
		"next day":    {func(j *phase.Job) { j.Input["day"] = "2026-11-15" }, `day was "2026-11-14", is "2026-11-15"`},
		"logic":       {func(j *phase.Job) { j.LogicVersion = 2 }, "LogicVersion 1 → 2"},
		"key added":   {func(j *phase.Job) { j.Input["region"] = "eu" }, `region was absent, is "eu"`},
		"phase added": {func(j *phase.Job) { j.Phases = append(j.Phases, w.phase("index", 1)) }, "phases were [extract transform load], are [extract transform load index]"},
		"fresh":       {func(j *phase.Job) { j.Fresh = true }, "Fresh was set"},
	} {
		job := w.job(dir, "2026-11-14")
		tc.edit(job)
		rep, err := job.Run(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(rep.RestartReason, tc.want) || rep.Phases[0].Outcome != phase.Ran {
			t.Errorf("%s: restarted %q, %v", name, rep.RestartReason, outcomes(rep))
		}
		// Back to the first input, for the next case to change from.
		if _, err := w.job(dir, "2026-11-14").Run(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
}

// TestAPhaseLeftRunningRunsAgain: a process killed in the middle of a
// phase leaves it recorded as running, which counts as not done; a panic
// propagates and still releases the lease.
func TestAPhaseLeftRunningRunsAgain(t *testing.T) {
	dir, w := t.TempDir(), newWorld()
	job := w.job(dir, "2026-11-15")
	job.Phases[1].Run = func(context.Context) (phase.Counts, error) { panic("killed") }
	func() {
		defer func() {
			if r := recover(); r != "killed" {
				t.Fatalf("recovered %v, want the phase's panic", r)
			}
		}()
		_, _ = job.Run(t.Context())
	}()
	if _, err := os.Stat(filepath.Join(dir, "orders-nightly.lease")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a panic left the lease held: %v", err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "orders-nightly.json")) //nolint:gosec // G304: the test's own directory
	if err != nil || !strings.Contains(string(b), `"name":"transform","status":"running"`) {
		t.Fatalf("record %s, %v", b, err)
	}
	rep, err := w.job(dir, "2026-11-15").Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got := outcomes(rep); !slices.Equal(got, []string{"extract=skipped", "transform=ran", "load=ran"}) {
		t.Errorf("%v", got)
	}
}

// TestASecondProcessIsRefused: while one run holds the job, another fails
// at once with ErrBusy, naming the holder.
func TestASecondProcessIsRefused(t *testing.T) {
	dir, w := t.TempDir(), newWorld()
	job := w.job(dir, "2026-11-15")
	var second error
	job.Phases[0].Run = func(ctx context.Context) (phase.Counts, error) {
		_, second = w.job(dir, "2026-11-15").Run(ctx)
		return phase.Counts{"rows": 10}, nil
	}
	if _, err := job.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(second, phase.ErrBusy) || !strings.Contains(second.Error(), fmt.Sprintf("pid %d", os.Getpid())) {
		t.Errorf("second run: %v", second)
	}
}

func TestAJobIsCheckedBeforeItRuns(t *testing.T) {
	ok := func(context.Context) (phase.Counts, error) { return nil, nil }
	store := phase.Dir(t.TempDir())
	for want, job := range map[string]phase.Job{
		"job name":             {Name: "a/b", Store: store, Phases: []phase.Phase{{Name: "x", Run: ok}}},
		"has no Store":         {Name: "a", Phases: []phase.Phase{{Name: "x", Run: ok}}},
		"has no phase":         {Name: "a", Store: store},
		"phase 0 has no name":  {Name: "a", Store: store, Phases: []phase.Phase{{Run: ok}}},
		`two phases named "x"`: {Name: "a", Store: store, Phases: []phase.Phase{{Name: "x", Run: ok}, {Name: "x", Run: ok}}},
		`phase "x" has no Run`: {Name: "a", Store: store, Phases: []phase.Phase{{Name: "x"}}},
	} {
		if _, err := job.Run(t.Context()); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v", want, err)
		}
	}
}

// TestARecordThatIsNotTheJobsIsRefusedOrReplaced: a record whose phases do
// not match its own fingerprint is refused; one whose fingerprint is not
// this job's although nothing readable differs is replaced, and says so.
func TestARecordThatIsNotTheJobsIsRefusedOrReplaced(t *testing.T) {
	dir, w := t.TempDir(), newWorld()
	if _, err := w.job(dir, "2026-11-15").Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "orders-nightly.json")
	b, err := os.ReadFile(path) //nolint:gosec // G304: the test's own directory
	if err != nil {
		t.Fatal(err)
	}
	cut := strings.Replace(string(b), `,{"name":"load"`, `]}{"x":[{"name":"load"`, 1)
	cut, _, _ = strings.Cut(cut, `{"x"`)
	if err := os.WriteFile(path, []byte(cut), 0o600); err != nil { //nolint:gosec // G703: the test's own directory
		t.Fatal(err)
	}
	if _, err := w.job(dir, "2026-11-15").Run(t.Context()); err == nil || !strings.Contains(err.Error(), "the record lists 2 phases") {
		t.Errorf("a record cut short: %v", err)
	}

	forged := strings.Replace(string(b), `"fingerprint":"`, `"fingerprint":"00`, 1)
	if err := os.WriteFile(path, []byte(forged), 0o600); err != nil { //nolint:gosec // G703: the test's own directory
		t.Fatal(err)
	}
	rep, err := w.job(dir, "2026-11-15").Run(t.Context())
	if err != nil || rep.RestartReason != "the record's fingerprint is not this job's" {
		t.Errorf("a forged fingerprint: %q, %v", rep.RestartReason, err)
	}

	if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := w.job(dir, "2026-11-15").Run(t.Context()); err == nil || !strings.Contains(err.Error(), "orders-nightly.json") {
		t.Errorf("a record that is not JSON: %v", err)
	}
}

// failing is a store whose lease fails the n-th Save, or its Release.
type failing struct {
	saves, failAt int
	release       error
}

func (f *failing) Lock(context.Context, string) (phase.Lease, error) { return f, nil }
func (f *failing) Load(context.Context) (phase.Record, bool, error) {
	return phase.Record{}, false, nil
}
func (f *failing) Release(context.Context) error { return f.release }
func (f *failing) Lost() <-chan struct{}         { return nil }

func (f *failing) Save(context.Context, phase.Record) error {
	f.saves++
	if f.saves == f.failAt {
		return phase.ErrLeaseLost
	}
	return nil
}

// TestAWriteThatFailsStopsTheJob: the record written before a phase runs,
// after it finishes, after it fails; and a Release that fails.
func TestAWriteThatFailsStopsTheJob(t *testing.T) {
	w := newWorld()
	run := func(store phase.Store) error {
		_, err := (&phase.Job{Name: "j", Store: store, Phases: []phase.Phase{w.phase("a", 1)}}).Run(t.Context())
		return err
	}
	if err := run(&failing{failAt: 1}); !errors.Is(err, phase.ErrLeaseLost) {
		t.Errorf("before the phase: %v", err)
	}
	if err := run(&failing{failAt: 2}); !errors.Is(err, phase.ErrLeaseLost) {
		t.Errorf("after the phase: %v", err)
	}
	w.fail["a"] = errors.New("broken")
	if err := run(&failing{failAt: 2}); !errors.Is(err, phase.ErrLeaseLost) || !strings.Contains(err.Error(), "broken") {
		t.Errorf("after the phase failed: %v", err)
	}
	delete(w.fail, "a")
	released := errors.New("release")
	if err := run(&failing{release: released}); !errors.Is(err, released) {
		t.Errorf("Release: %v", err)
	}
}

// A failed Verify invalidates that phase and every phase after it (ADR 0027,
// "A skip is re-validated"), and the record says so: when the phase whose
// Verify failed then fails in its Run — or the process dies in it — the next
// run still runs the later phases. The invalidation lived in a local of
// Job.Run and died with it, and the next run skipped them on their own
// Verify, though they were built from what the invalidated phase wrote.
func TestAnInvalidationOutlivesTheRunThatMadeIt(t *testing.T) {
	dir := t.TempDir()
	rows := map[string]int64{}
	var extractErr error

	step := func(name string, n int64, fail *error) phase.Phase {
		return phase.Phase{
			Name: name,
			Run: func(context.Context) (phase.Counts, error) {
				if fail != nil && *fail != nil {
					return nil, *fail
				}
				rows[name] = n
				return phase.Counts{"rows": n}, nil
			},
			Verify: func(_ context.Context, recorded phase.Counts) error {
				if rows[name] != recorded["rows"] {
					return errors.New(name + " does not hold what was recorded")
				}
				return nil
			},
		}
	}
	job := func() *phase.Job {
		return &phase.Job{
			Name:   "invalidation",
			Input:  phase.Input{"day": "2026-11-17"},
			Store:  phase.Dir(dir),
			Phases: []phase.Phase{step("extract", 10, &extractErr), step("load", 10, nil)},
		}
	}
	outcomes := func(r phase.Report) []string {
		var out []string
		for _, p := range r.Phases {
			out = append(out, p.Name+"="+string(p.Outcome))
		}
		return out
	}

	// Run 1: both phases done.
	if _, err := job().Run(t.Context()); err != nil {
		t.Fatal(err)
	}

	// Control: extract's Verify fails and its Run succeeds — load runs again,
	// as the ADR says.
	rows["extract"] = 0
	rep, err := job().Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got := outcomes(rep); !slices.Equal(got, []string{"extract=ran", "load=ran"}) {
		t.Fatalf("control: %v, want extract and load both ran", got)
	}

	// Run 3: extract's Verify fails again, and this time its Run fails too.
	rows["extract"] = 0
	extractErr = errors.New("the source is down")
	if _, err := job().Run(t.Context()); err == nil {
		t.Fatal("run 3: the failing phase returned no error")
	}

	// Run 4: the source is back. extract runs again from its start; load read
	// what the earlier extract wrote, and was invalidated by run 3.
	extractErr = nil
	rep, err = job().Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got := outcomes(rep); !slices.Equal(got, []string{"extract=ran", "load=ran"}) {
		t.Errorf("after an invalidating run that failed: %v, want [extract=ran load=ran] — the phase after the one whose Verify failed is skipped", got)
	}
}

// boundStore is a Store that records the contexts its lease is called under.
type boundStore struct {
	saves   []context.Context
	release context.Context
}

func (s *boundStore) Lock(context.Context, string) (phase.Lease, error) { return s, nil }
func (s *boundStore) Load(context.Context) (phase.Record, bool, error) {
	return phase.Record{}, false, nil
}

func (s *boundStore) Save(ctx context.Context, _ phase.Record) error {
	s.saves = append(s.saves, ctx)
	return nil
}
func (s *boundStore) Release(ctx context.Context) error { s.release = ctx; return nil }
func (s *boundStore) Lost() <-chan struct{}             { return nil }

// canEnd reports whether anything can end a call made under ctx.
func canEnd(ctx context.Context) bool {
	_, deadline := ctx.Deadline()
	return deadline || ctx.Done() != nil
}

// The failed phase's record and the lock's release are written past the end
// of the caller's context, but under one that ends (the architecture's S5:
// the connectors are bounded by their context). They were written under
// context.WithoutCancel alone, which nothing ends: on postgres.PhaseStore a
// server that stopped answering held Run for as long as the socket lived.
func TestTheLastWritesOfARunAreUnderAContextThatEnds(t *testing.T) {
	s := &boundStore{}
	job := &phase.Job{
		Name:  "bounded",
		Store: s,
		Phases: []phase.Phase{{
			Name: "load",
			Run:  func(context.Context) (phase.Counts, error) { return nil, errors.New("the server stopped answering") },
		}},
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if _, err := job.Run(ctx); err == nil {
		t.Fatal("control: the failing phase returned no error")
	}
	// Control: the first write, before the phase ran, is under the caller's
	// context.
	if len(s.saves) != 2 || !canEnd(s.saves[0]) {
		t.Fatalf("control: %d writes, the first under a context that can end: %v", len(s.saves), len(s.saves) > 0 && canEnd(s.saves[0]))
	}
	if !canEnd(s.saves[1]) {
		t.Error("the record of the failed phase is written under a context with no deadline and no cancellation")
	}
	if s.release == nil || !canEnd(s.release) {
		t.Error("the lock is released under a context with no deadline and no cancellation")
	}
}
