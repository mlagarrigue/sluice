package phase

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func lockDir(t *testing.T, d *DirStore) *dirLease {
	t.Helper()
	l, err := d.Lock(t.Context(), "job")
	if err != nil {
		t.Fatal(err)
	}
	return l.(*dirLease)
}

// TestALiveLeaseIsRenewedAndAnExpiredOneTakenOver: the holder pushes its
// expiry back every third of the lease and writes long after the lease
// would have run out; a lease left by a dead process is taken over once it
// expires, and not before.
func TestALiveLeaseIsRenewedAndAnExpiredOneTakenOver(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := &DirStore{Path: t.TempDir(), LeaseTTL: 3 * time.Second}
		l := lockDir(t, d)
		time.Sleep(10 * time.Second)
		synctest.Wait()
		if err := l.Save(t.Context(), Record{Fingerprint: "f"}); err != nil {
			t.Fatalf("a renewed lease refused a write: %v", err)
		}
		if err := l.Release(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := l.Release(t.Context()); err != nil {
			t.Errorf("a second Release: %v", err)
		}
		if err := l.Save(t.Context(), Record{}); !errors.Is(err, ErrLeaseLost) {
			t.Errorf("a write after Release: %v", err)
		}

		// A dead process's lease: written, never renewed.
		dead := leaseFile{Token: "dead", Expires: time.Now().Add(3 * time.Second), PID: 1, Host: "gone"}
		if err := writeFile(filepath.Join(d.Path, "job.lease"), dead); err != nil {
			t.Fatal(err)
		}
		if _, err := d.Lock(t.Context(), "job"); !errors.Is(err, ErrBusy) || !strings.Contains(err.Error(), "pid 1 on gone") {
			t.Errorf("a live lease: %v", err)
		}
		time.Sleep(4 * time.Second)
		taken := lockDir(t, d)
		if err := taken.Save(t.Context(), Record{Fingerprint: "g"}); err != nil {
			t.Errorf("the lease taken over: %v", err)
		}
		if err := taken.Release(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
}

// TestALeaseLostStopsTheWrites: a holder stopped longer than its lease is
// never renewed and writes nothing more; one whose lease another process
// took writes nothing more either, and does not remove the other's lease.
func TestALeaseLostStopsTheWrites(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := &DirStore{Path: t.TempDir(), LeaseTTL: 3 * time.Second}
		l := lockDir(t, d)
		l.mu.Lock()
		l.mine.Expires = time.Now().Add(-time.Second) // as if stopped past its lease
		l.mu.Unlock()
		if err := l.Save(t.Context(), Record{}); !errors.Is(err, ErrLeaseLost) || !strings.Contains(err.Error(), "expired") {
			t.Errorf("an expired lease wrote: %v", err)
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if err := l.Release(t.Context()); err != nil {
			t.Fatal(err)
		}
		// Not removed: a taker may be replacing it. It runs out instead.
		time.Sleep(2 * time.Second)

		l = lockDir(t, d)
		thief := leaseFile{Token: "thief", Expires: time.Now().Add(time.Hour), PID: 2, Host: "other"}
		if err := writeFile(l.path, thief); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second) // the renewal finds it
		synctest.Wait()
		if err := l.Save(t.Context(), Record{}); !errors.Is(err, ErrLeaseLost) || !strings.Contains(err.Error(), "pid 2 on other") {
			t.Errorf("a lease taken by another wrote: %v", err)
		}
		if err := l.Release(t.Context()); err != nil {
			t.Fatal(err)
		}
		var cur leaseFile
		if err := readFile(l.path, &cur); err != nil || cur.Token != "thief" {
			t.Errorf("Release removed another's lease: %+v, %v", cur, err)
		}
	})
}

// TestTakingOverIsExclusive: a taker finds another's guard and is refused;
// a guard older than a lease, left by a taker that died, is removed for
// the next run; a lease file that does not parse is a dead writer's.
func TestTakingOverIsExclusive(t *testing.T) {
	d := &DirStore{Path: t.TempDir(), LeaseTTL: time.Second}
	path := filepath.Join(d.Path, "job.lease")
	if err := writeFile(path, leaseFile{Token: "dead", Expires: time.Now().Add(-time.Minute)}); err != nil {
		t.Fatal(err)
	}
	guard := path + ".takeover"
	if err := os.WriteFile(guard, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Lock(t.Context(), "job"); !errors.Is(err, ErrBusy) || !strings.Contains(err.Error(), "taking the expired lease") {
		t.Errorf("a guard held: %v", err)
	}
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(guard, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Lock(t.Context(), "job"); !errors.Is(err, ErrBusy) {
		t.Errorf("a stale guard: %v", err)
	}
	if _, err := os.Stat(guard); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the stale guard stayed: %v", err)
	}

	if err := os.WriteFile(path, []byte("{half"), 0o600); err != nil {
		t.Fatal(err)
	}
	l := lockDir(t, d)
	if err := l.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// TestADirectoryThatCannotHoldTheJobFails: no directory, a job name that
// is not a file name, a directory gone under a held lease.
func TestADirectoryThatCannotHoldTheJobFails(t *testing.T) {
	if _, err := Dir(filepath.Join(t.TempDir(), "absent")).Lock(t.Context(), "job"); err == nil {
		t.Error("a lease in a directory that does not exist")
	}
	if _, err := Dir(t.TempDir()).Lock(t.Context(), "../job"); err == nil {
		t.Error("a job name with a path in it")
	}
	if err := writeFile(filepath.Join(t.TempDir(), "absent", "x.json"), Record{}); err == nil {
		t.Error("a record written into a directory that does not exist")
	}
	if Dir("x").lease() != 30*time.Second {
		t.Error("the default lease")
	}

	dir := t.TempDir()
	l := lockDir(t, Dir(dir))
	if err := os.Remove(l.path); err != nil {
		t.Fatal(err)
	}
	if err := l.Save(t.Context(), Record{}); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("a lease file removed: %v", err)
	}
	if err := l.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// TestAFailedWriteLeavesNothingHalfDone: a record that cannot replace its
// file leaves no temporary file behind; a directory the taker may not
// write to refuses the takeover with the system's error, not ErrBusy.
func TestAFailedWriteLeavesNothingHalfDone(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "x.json"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := writeFile(filepath.Join(dir, "x.json"), Record{}); err == nil {
		t.Error("a record renamed over a directory")
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		t.Errorf("left behind: %v", entries)
	}

	if os.Getuid() == 0 {
		t.Skip("root writes to a read-only directory")
	}
	d := Dir(t.TempDir())
	if err := writeFile(filepath.Join(d.Path, "job.lease"), leaseFile{Token: "dead"}); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(d.Path, 0o500); err != nil { //nolint:gosec // G302: a test directory made read-only on purpose
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(d.Path, 0o700) }) //nolint:gosec // G302: restored for the cleanup
	if _, err := d.Lock(t.Context(), "job"); err == nil || errors.Is(err, ErrBusy) {
		t.Errorf("a takeover in a read-only directory: %v", err)
	}
}
