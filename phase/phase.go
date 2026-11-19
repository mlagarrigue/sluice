// Package phase resumes a long job where its process died.
//
// A job is a sequence of phases. Each phase that finishes is recorded in a
// store the application chooses — a directory ([Dir]), a table in its own
// database — and a job run again skips the phases already done, once each
// has been checked against what it left behind, runs the rest, and refuses
// to start beside another process running the same job. The library keeps
// none of that state: it brings the mechanism, the application owns the
// record.
//
// Three properties make a resumption trustworthy rather than merely
// possible (docs/design/adr/0027-phase-runner.md):
//
//   - A phase recorded as done is skipped only when its [Phase.Verify]
//     passes, and a phase without one is never skipped: a working table
//     emptied by a crash while its record survived saying "done" is the
//     case a trusting default produces.
//   - A resume is valid only against the same input. The job's name, its
//     [Input], its [Job.Logic] version and the names of its phases are
//     fingerprinted; a run whose fingerprint differs from the record starts
//     over, and its [Report] names what changed. Settings that do not change
//     the output — worker counts, verbosity — have no place in Input: they
//     live in the phases' functions, so they apply to the phases that
//     remain.
//   - A second process on the same job fails at once with [ErrBusy].
//
// A phase is the grain of resumption: nothing resumes inside one, so a
// phase must run correctly again from its start — empty its working table
// before filling it.
//
// The guide: docs/guide/long-jobs.md.
package phase

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"
)

// Input is what a job's output depends on, as strings the application
// formats itself (a date in ISO 8601), so that its canonical form is
// unambiguous. It is fingerprinted.
type Input map[string]string

// Counts is what a phase produced, by name — rows loaded, files read —
// recorded when it finishes and handed back to its Verify.
type Counts map[string]int64

// A Phase is one unit of restart.
type Phase struct {
	// Name identifies the phase in the record; it must be unique in its
	// job.
	Name string

	// Run does the phase's work and returns what it produced. It must be
	// able to run again from its start.
	Run func(ctx context.Context) (Counts, error)

	// Verify reads back what the phase left and checks it against the
	// counts recorded when it finished. A phase recorded as done is skipped
	// only when Verify returns nil; nil Verify means the phase always runs.
	Verify func(ctx context.Context, recorded Counts) error
}

// A Job is a named sequence of phases and the store its record lives in.
type Job struct {
	// Name identifies the job in its store: letters, digits, '.', '_' and
	// '-'. Two processes running the same name exclude each other.
	Name string

	// Input is what the job's output depends on; part of the fingerprint.
	Input Input

	// Logic is bumped if and only if the same input can now produce
	// different output; part of the fingerprint.
	Logic int

	// Store holds the job's lock and record.
	Store Store

	// Phases run in order, each after the previous one finished.
	Phases []Phase

	// Fresh discards the record and runs every phase, even with the same
	// fingerprint. It is not part of the fingerprint.
	Fresh bool
}

// Outcome is what a run did with a phase.
type Outcome string

const (
	// Skipped: the phase was recorded as done and its Verify passed.
	Skipped Outcome = "skipped"
	// Ran: the phase ran, for the first time or again.
	Ran Outcome = "ran"
)

// PhaseReport is what a run did with one phase.
type PhaseReport struct {
	Name    string
	Outcome Outcome
	// Why says why a phase recorded as done ran again: its Verify failed,
	// it has none, an earlier phase ran again. Empty otherwise.
	Why    string
	Counts Counts
}

// Report is what a run did: the phases it reached, in order, and why the
// record was discarded if it was.
type Report struct {
	// Restarted names what changed when the record was discarded — the
	// input keys, the logic version, the phases — or says Fresh was set.
	// Empty on a first run and on a resume.
	Restarted string
	Phases    []PhaseReport
}

var jobName = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

func (j *Job) validate() error {
	switch {
	case !jobName.MatchString(j.Name):
		return fmt.Errorf("phase: job name %q: want letters, digits, '.', '_' or '-'", j.Name)
	case j.Store == nil:
		return fmt.Errorf("phase: job %q has no Store", j.Name)
	case len(j.Phases) == 0:
		return fmt.Errorf("phase: job %q has no phase", j.Name)
	}
	seen := map[string]bool{}
	for i, p := range j.Phases {
		switch {
		case p.Name == "":
			return fmt.Errorf("phase: job %q: phase %d has no name", j.Name, i)
		case seen[p.Name]:
			return fmt.Errorf("phase: job %q: two phases named %q", j.Name, p.Name)
		case p.Run == nil:
			return fmt.Errorf("phase: job %q: phase %q has no Run", j.Name, p.Name)
		}
		seen[p.Name] = true
	}
	return nil
}

// fresh is the record of a run that starts from zero.
func (j *Job) fresh() Record {
	// A nil Input and an empty one are the same input: one fingerprint.
	r := Record{Input: Input{}, Logic: j.Logic}
	maps.Copy(r.Input, j.Input)
	names := make([]string, len(j.Phases))
	for i, p := range j.Phases {
		names[i] = p.Name
		r.Phases = append(r.Phases, PhaseRecord{Name: p.Name})
	}
	// encoding/json writes a map's keys sorted: the canonical form. Of
	// strings and an int, Marshal cannot fail.
	b, _ := json.Marshal(struct {
		Name   string
		Input  Input
		Logic  int
		Phases []string
	}{j.Name, r.Input, j.Logic, names})
	sum := sha256.Sum256(b)
	r.Fingerprint = hex.EncodeToString(sum[:])
	return r
}

// changes names what differs between a record and the run that discards
// it.
func changes(was, is Record) string {
	var out []string
	keys := map[string]bool{}
	for k := range was.Input {
		keys[k] = true
	}
	for k := range is.Input {
		keys[k] = true
	}
	quote := func(m Input, k string) string {
		if v, ok := m[k]; ok {
			return fmt.Sprintf("%q", v)
		}
		return "absent"
	}
	for _, k := range slices.Sorted(maps.Keys(keys)) {
		if a, b := quote(was.Input, k), quote(is.Input, k); a != b {
			out = append(out, fmt.Sprintf("%s was %s, is %s", k, a, b))
		}
	}
	if was.Logic != is.Logic {
		out = append(out, fmt.Sprintf("Logic %d → %d", was.Logic, is.Logic))
	}
	if a, b := phaseNames(was), phaseNames(is); !slices.Equal(a, b) {
		out = append(out, fmt.Sprintf("phases were %v, are %v", a, b))
	}
	if len(out) == 0 {
		return "the record's fingerprint is not this job's"
	}
	return strings.Join(out, "; ")
}

func phaseNames(r Record) []string {
	names := make([]string, len(r.Phases))
	for i, p := range r.Phases {
		names[i] = p.Name
	}
	return names
}

// afterEndBound bounds what Run writes once ctx may have ended.
const afterEndBound = 5 * time.Second

// afterEnd is the context of a write Run makes whatever ctx's state: ctx's
// values, not its end, and a deadline of its own.
func afterEnd(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), afterEndBound)
}

// Run runs the job: it takes the job's lock, reads its record, skips the
// phases done and verified, runs the others in order, records each as it
// finishes, and releases the lock. A phase's error is recorded and
// returned; the next run resumes at that phase. A panic in a phase
// propagates; the lock is released and the phase stays recorded as
// running, which counts as not done.
//
// A lock found lost while a phase runs ([Lease.Lost]) cancels the context
// the phase runs under, its cause ErrLeaseLost; Run then returns an error
// wrapping ErrLeaseLost, as the next Save fails with it.
//
// The failed phase's record and the lock's release are written even once
// ctx has ended, so that its end neither loses the failure nor keeps the
// lock: each under ctx's values, bounded by five seconds of its own rather
// than by ctx, so that a store that stopped answering cannot hold Run.
func (j *Job) Run(ctx context.Context) (rep Report, err error) {
	if err := j.validate(); err != nil {
		return rep, err
	}
	lease, err := j.Store.Lock(ctx, j.Name)
	if err != nil {
		return rep, err
	}
	defer func() {
		actx, cancel := afterEnd(ctx)
		defer cancel()
		if rerr := lease.Release(actx); rerr != nil && err == nil {
			err = rerr
		}
	}()
	// A lock lost while a phase runs ends that phase's context, with
	// ErrLeaseLost as its cause: the process that took the lock over is
	// running the same phase, and the two must not fill the same working
	// tables at once. Release, deferred above, runs after this cancel.
	ctx, lost := context.WithCancelCause(ctx)
	defer lost(nil)
	go func() {
		select {
		case <-lease.Lost():
			lost(ErrLeaseLost)
		case <-ctx.Done():
		}
	}()
	rec, found, err := lease.Load(ctx)
	if err != nil {
		return rep, err
	}
	fresh := j.fresh()
	switch {
	case !found:
		rec = fresh
	case j.Fresh:
		rep.Restarted, rec = "Fresh was set", fresh
	case rec.Fingerprint != fresh.Fingerprint:
		rep.Restarted, rec = changes(rec, fresh), fresh
	case len(rec.Phases) != len(j.Phases):
		return rep, fmt.Errorf("phase: job %q: the record lists %d phases under this job's fingerprint, the job %d",
			j.Name, len(rec.Phases), len(j.Phases))
	}

	again := "" // why every phase from here on runs again, once one has
	for i, p := range j.Phases {
		st := &rec.Phases[i]
		why := again
		if st.Status == Done && again == "" {
			if p.Verify == nil {
				why = "it has no Verify"
			} else {
				verr := p.Verify(ctx, st.Counts)
				if verr == nil {
					rep.Phases = append(rep.Phases, PhaseReport{Name: p.Name, Outcome: Skipped, Counts: st.Counts})
					continue
				}
				why = "its Verify failed: " + verr.Error()
				again = "an earlier phase, " + p.Name + ", ran again"
				// Written with this phase's Running below, not kept in
				// again alone: a Run that fails, or a process that dies in
				// it, would leave the later phases Done in the record, and
				// the next run would skip them on their own Verify — built
				// from what this phase wrote before it was invalidated.
				for k := i + 1; k < len(rec.Phases); k++ {
					rec.Phases[k] = PhaseRecord{Name: rec.Phases[k].Name}
				}
			}
		}
		*st = PhaseRecord{Name: p.Name, Status: Running, At: time.Now()}
		if err := lease.Save(ctx, rec); err != nil {
			return rep, err
		}
		counts, rerr := p.Run(ctx)
		if rerr != nil {
			st.Status, st.Error, st.At = Failed, rerr.Error(), time.Now()
			rerr = fmt.Errorf("phase: job %q, phase %q: %w", j.Name, p.Name, rerr)
			actx, cancel := afterEnd(ctx)
			serr := lease.Save(actx, rec)
			cancel()
			if serr != nil {
				return rep, errors.Join(rerr, serr)
			}
			return rep, rerr
		}
		st.Status, st.Counts, st.At = Done, counts, time.Now()
		if err := lease.Save(ctx, rec); err != nil {
			return rep, err
		}
		rep.Phases = append(rep.Phases, PhaseReport{Name: p.Name, Outcome: Ran, Why: why, Counts: counts})
	}
	return rep, nil
}
