package diagnostics

import (
	"runtime"
	"testing"
	"time"
)

func diag(sev Severity, code string) Diagnostic {
	return NewDiagnostic(sev, code, Field(code))
}

// trackedError is an error whose collection can be observed with a cleanup.
type trackedError struct{ msg string }

func (e *trackedError) Error() string { return e.msg }

func TestDiagnosticsCeiling(t *testing.T) {
	tests := []struct {
		name          string
		limit         int
		add           int
		wantKept      int
		wantTruncated int
	}{
		{"under the ceiling", 10, 3, 3, 0},
		{"exactly at the ceiling", 3, 3, 3, 0},
		{"over the ceiling", 3, 10, 3, 7},
		{"zero limit counts everything", 0, 5, 0, 5},
		{"negative limit counts everything", -1, 5, 0, 5},
		{"nothing added", 10, 0, 0, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewCollector(tc.limit)
			for i := range tc.add {
				d.Add(diag(Warning, "code"+string(rune('a'+i))))
			}
			if d.Len() != tc.wantKept {
				t.Errorf("Len = %d, want %d", d.Len(), tc.wantKept)
			}
			if d.Truncated() != tc.wantTruncated {
				t.Errorf("Truncated = %d, want %d", d.Truncated(), tc.wantTruncated)
			}
		})
	}
}

// Add reports whether it kept the diagnostic, so a producer can stop rather
// than ask twice.
func TestDiagnosticsAddReportsAcceptance(t *testing.T) {
	d := NewCollector(2)

	if !d.Add(diag(Info, "a")) || !d.Add(diag(Info, "b")) {
		t.Error("Add must report true while under the ceiling")
	}
	if d.Add(diag(Info, "c")) {
		t.Error("Add must report false once the ceiling is reached")
	}
}

// Stable ordering is what makes a report reproducible: the first n, in arrival
// order, not the most severe and not the last.
func TestDiagnosticsKeepsFirstInOrder(t *testing.T) {
	d := NewCollector(3)
	d.Add(diag(Info, "first"))
	d.Add(diag(Critical, "second"))
	d.Add(diag(Warning, "third"))
	d.Add(diag(Critical, "dropped-even-though-critical"))

	got := d.All()
	want := []string{"first", "second", "third"}
	if len(got) != len(want) {
		t.Fatalf("kept %d diagnostics, want %d", len(got), len(want))
	}
	for i, w := range want {
		if got[i].Code != w {
			t.Errorf("kept[%d] = %q, want %q", i, got[i].Code, w)
		}
	}
	if d.Truncated() != 1 {
		t.Errorf("Truncated = %d, want 1", d.Truncated())
	}
}

func TestDiagnosticsWorst(t *testing.T) {
	tests := []struct {
		name     string
		add      []Severity
		want     Severity
		wantAny  bool
		useLimit int
	}{
		{"nothing recorded", nil, Info, false, 10},
		{"one info", []Severity{Info}, Info, true, 10},
		{"warning beats info", []Severity{Info, Warning}, Warning, true, 10},
		{"critical beats all", []Severity{Warning, Critical, Error}, Critical, true, 10},
		{"worst is first", []Severity{Critical, Info}, Critical, true, 10},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewCollector(tc.useLimit)
			for _, s := range tc.add {
				d.Add(diag(s, "c"))
			}
			got, found := d.Worst()
			if found != tc.wantAny {
				t.Errorf("Worst reported found=%v, want %v", found, tc.wantAny)
			}
			if found && got != tc.want {
				t.Errorf("Worst = %v, want %v", got, tc.want)
			}
		})
	}
}

// Worst reflects only what was kept. A truncated report may hide something
// more severe, and the contract says so rather than pretending otherwise.
func TestDiagnosticsWorstIgnoresTruncated(t *testing.T) {
	d := NewCollector(1)
	d.Add(diag(Info, "kept"))
	d.Add(diag(Critical, "dropped"))

	got, _ := d.Worst()
	if got != Info {
		t.Errorf("Worst = %v, want Info: it must not inspect what it did not keep", got)
	}
	if d.Truncated() != 1 {
		t.Errorf("Truncated = %d, want 1: the caller needs this to know the answer is partial", d.Truncated())
	}
}

func TestDiagnosticsReset(t *testing.T) {
	d := NewCollector(2)
	d.Add(diag(Critical, "a"))
	d.Add(diag(Critical, "b"))
	d.Add(diag(Critical, "dropped"))

	d.Reset()

	if d.Len() != 0 {
		t.Errorf("Len after Reset = %d, want 0", d.Len())
	}
	if d.Truncated() != 0 {
		t.Errorf("Truncated after Reset = %d, want 0: the count describes one batch", d.Truncated())
	}
	// The ceiling survives: a reset collector is still bounded.
	d.Add(diag(Info, "x"))
	d.Add(diag(Info, "y"))
	d.Add(diag(Info, "z"))
	if d.Len() != 2 || d.Truncated() != 1 {
		t.Errorf("after Reset: Len = %d, Truncated = %d, want 2 and 1", d.Len(), d.Truncated())
	}
}

// Reset must release what it dropped. A Diagnostic holds an Args map and a
// wrapped cause; leaving them live in the backing array would pin the first
// batch's memory for the life of a reused collector — the unbounded retention
// S1 exists to prevent.
func TestDiagnosticsResetReleasesReferences(t *testing.T) {
	d := NewCollector(4)

	// The cleanup runs on a runtime goroutine, so the signal crosses a channel
	// rather than a bare bool: reading one written by the collector is a data
	// race, and -race fails the test for the wrong reason.
	collected := make(chan struct{})
	func() {
		// A cause whose liveness is observable. AddCleanup needs a pointer, so
		// the concrete type is used rather than the error interface.
		cause := &trackedError{msg: "held by the diagnostic"}
		runtime.AddCleanup(cause, func(bool) { close(collected) }, true)
		d.Add(NewDiagnostic(Error, "code", Path{}).WithCause(cause))
	}()

	d.Reset()

	// The collector must stay reachable past the GC, or the backing array dies
	// with it and the test passes whether or not Reset cleared anything. This
	// is what makes the assertion falsifiable: without the clear in Reset, the
	// diagnostic is still live here and the cleanup never runs.
	runtime.GC()
	runtime.GC()
	runtime.KeepAlive(&d)

	select {
	case <-collected:
	case <-time.After(time.Second):
		t.Error("Reset left a dropped diagnostic live in the backing array: " +
			"a reused collector would pin every batch it ever saw")
	}
}

// The zero value keeps nothing but still counts, so a collector nobody sized
// reports that diagnostics happened rather than swallowing them.
func TestDiagnosticsZeroValue(t *testing.T) {
	var d Collector

	if d.Add(diag(Critical, "a")) {
		t.Error("the zero collector must keep nothing")
	}
	if d.Len() != 0 {
		t.Errorf("Len = %d, want 0", d.Len())
	}
	if d.Truncated() != 1 {
		t.Errorf("Truncated = %d, want 1: a dropped diagnostic must still be counted", d.Truncated())
	}
	if _, found := d.Worst(); found {
		t.Error("Worst must report no diagnostic on an empty collector")
	}
}

// The storage is claimed on first use: a large configured ceiling must not
// allocate for the batches that report nothing, which is most of them.
func TestDiagnosticsDoesNotPreallocate(t *testing.T) {
	const huge = 1 << 20

	allocs := testing.AllocsPerRun(100, func() {
		d := NewCollector(huge)
		_ = d.Len()
	})
	if allocs != 0 {
		t.Errorf("an unused collector allocated %.0f times, want 0", allocs)
	}
}

// The first Add must not claim the whole ceiling. A limit set high "just in
// case" is a configuration value, and turning it into a 125 MB allocation the
// moment one diagnostic appears is the failure this cap exists to prevent.
func TestDiagnosticsFirstAddDoesNotClaimTheCeiling(t *testing.T) {
	const huge = 1 << 20

	d := NewCollector(huge)
	d.Add(diag(Warning, "one"))

	if got := cap(d.All()); got > initialDiagnostics {
		t.Errorf("one diagnostic under a %d ceiling reserved %d entries, want at most %d",
			huge, got, initialDiagnostics)
	}
	// The ceiling still bounds what is kept, which is the point of the type.
	if d.Len() != 1 {
		t.Errorf("Len = %d, want 1", d.Len())
	}
}

// Capping the initial reservation must not break the ceiling: a report that
// fills past the cap grows into its limit and stops there.
func TestDiagnosticsGrowsPastTheInitialReservation(t *testing.T) {
	const limit = initialDiagnostics * 4

	d := NewCollector(limit)
	for range limit + 10 {
		d.Add(diag(Info, "c"))
	}

	if d.Len() != limit {
		t.Errorf("Len = %d, want %d: the ceiling must still bound the report", d.Len(), limit)
	}
	if d.Truncated() != 10 {
		t.Errorf("Truncated = %d, want 10", d.Truncated())
	}
}

// A small ceiling reserves only what it can hold, rather than the cap.
func TestDiagnosticsSmallLimitReservesLess(t *testing.T) {
	d := NewCollector(2)
	d.Add(diag(Info, "a"))

	if got := cap(d.All()); got != 2 {
		t.Errorf("a ceiling of 2 reserved %d entries, want 2", got)
	}
}
