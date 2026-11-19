package phase

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

// DirStore keeps each job's record in a file of one directory, and its lock
// in a lease file beside it: for a job that runs on one machine, with no
// database to hold its state.
//
// The lease names its holder (a random token, the process and the host) and
// an expiry the holder pushes back every third of the lease. A lease that
// has expired — its process died, or stopped longer than the lease — may be
// taken over; a live one refuses. A record is written only under a live
// lease that is still this process's, so a process resumed after its lease
// expired stops at its next write instead of writing beside its successor.
// The check and the write are two steps: a process stopped between them,
// for longer than the lease, can still write once — the limit of a lock
// kept in files rather than by a server.
type DirStore struct {
	// Path is the directory; it must exist.
	Path string

	// LeaseTTL is how long a lease holds without being renewed; zero means
	// 30 seconds. It bounds how long a dead process keeps its job locked.
	LeaseTTL time.Duration
}

// Dir is a [DirStore] over a directory, with the default lease.
func Dir(path string) *DirStore { return &DirStore{Path: path} }

func (d *DirStore) lease() time.Duration {
	if d.LeaseTTL > 0 {
		return d.LeaseTTL
	}
	return 30 * time.Second
}

// leaseFile is what a lease file holds.
type leaseFile struct {
	Token   string    `json:"token"`
	Expires time.Time `json:"expires"`
	PID     int       `json:"pid"`
	Host    string    `json:"host"`
}

// dirLease is a lease of a DirStore, held.
type dirLease struct {
	store     *DirStore
	path, rec string
	mu        sync.Mutex
	mine      leaseFile
	stop      chan struct{}
	done      sync.WaitGroup
	released  bool
	lost      chan struct{} // closed by markLost (Lease.Lost)
	lostOnce  sync.Once
}

// Lost is closed once renew or Save finds the lease lost.
func (l *dirLease) Lost() <-chan struct{} { return l.lost }

// markLost closes lost, once.
func (l *dirLease) markLost() { l.lostOnce.Do(func() { close(l.lost) }) }

// Lock takes the job's lease: by creating its file, or by taking over an
// expired one. Taking over goes through a second file created exclusively,
// so that of two processes finding the same expired lease, one takes it
// and the other is refused.
func (d *DirStore) Lock(_ context.Context, job string) (Lease, error) {
	if !jobName.MatchString(job) {
		return nil, fmt.Errorf("phase: job name %q: want letters, digits, '.', '_' or '-'", job)
	}
	host, _ := os.Hostname()
	l := &dirLease{
		store: d,
		path:  filepath.Join(d.Path, job+".lease"),
		rec:   filepath.Join(d.Path, job+".json"),
		mine:  leaseFile{Token: rand.Text(), PID: os.Getpid(), Host: host},
		stop:  make(chan struct{}),
		lost:  make(chan struct{}),
	}
	l.mine.Expires = time.Now().Add(d.lease())
	f, err := os.OpenFile(l.path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	switch {
	case err == nil:
		werr := json.NewEncoder(f).Encode(l.mine)
		if cerr := f.Close(); werr == nil {
			werr = cerr
		}
		if werr != nil {
			return nil, errors.Join(werr, os.Remove(l.path))
		}
	case errors.Is(err, fs.ErrExist):
		if err := l.takeOver(); err != nil {
			return nil, err
		}
	default:
		return nil, err
	}
	l.done.Add(1)
	go l.renew()
	return l, nil
}

// takeOver replaces an expired lease with this one. The lease is read
// under the guard, so that of two takers the second finds the first's.
func (l *dirLease) takeOver() error {
	guard := l.path + ".takeover"
	g, err := os.OpenFile(guard, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) //nolint:gosec // G304: a file of the store's own directory
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			// A taker that died inside this short section leaves its
			// guard: older than a lease, it is removed, and the next run
			// takes the lease.
			if info, serr := os.Stat(guard); serr == nil && time.Since(info.ModTime()) > l.store.lease() {
				_ = os.Remove(guard)
			}
			return fmt.Errorf("%w: another process is taking the expired lease of %s over", ErrBusy, l.path)
		}
		return err
	}
	defer func() { _ = g.Close(); _ = os.Remove(guard) }()
	if err := l.refuseLive(); err != nil {
		return err
	}
	return writeFile(l.path, l.mine)
}

// refuseLive fails with ErrBusy when the lease file holds a live lease. A
// file gone (its holder released it) or unreadable (half written by a
// process that died) holds none.
func (l *dirLease) refuseLive() error {
	var cur leaseFile
	readable := readFile(l.path, &cur) == nil
	if !readable || !time.Now().Before(cur.Expires) {
		return nil
	}
	return fmt.Errorf("%w: pid %d on %s holds %s until %s", ErrBusy, cur.PID, cur.Host, l.path,
		cur.Expires.Format(time.RFC3339))
}

// renew pushes the lease's expiry back every third of the lease, until
// Release, or until the lease is found expired or another's.
func (l *dirLease) renew() {
	defer l.done.Done()
	t := time.NewTicker(l.store.lease() / 3)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
			l.mu.Lock()
			if err := l.checkLocked(); err != nil {
				if !l.released {
					l.markLost() // expired or another's: the phase running is told
				}
				l.mu.Unlock()
				return
			}
			next := l.mine
			next.Expires = time.Now().Add(l.store.lease())
			if writeFile(l.path, next) == nil {
				l.mine = next
			}
			l.mu.Unlock()
		}
	}
}

// checkLocked fails with ErrLeaseLost unless the lease file still holds
// this lease, unexpired. An expired lease is never renewed: another
// process may be taking it over.
func (l *dirLease) checkLocked() error {
	var cur leaseFile
	if l.released {
		return ErrLeaseLost
	}
	if err := readFile(l.path, &cur); err != nil {
		return fmt.Errorf("%w: %s: %w", ErrLeaseLost, l.path, err)
	}
	if cur.Token != l.mine.Token {
		return fmt.Errorf("%w: %s is held by pid %d on %s", ErrLeaseLost, l.path, cur.PID, cur.Host)
	}
	if !time.Now().Before(l.mine.Expires) {
		return fmt.Errorf("%w: %s expired at %s", ErrLeaseLost, l.path, l.mine.Expires.Format(time.RFC3339))
	}
	return nil
}

func (l *dirLease) Load(context.Context) (Record, bool, error) {
	var r Record
	err := readFile(l.rec, &r)
	if errors.Is(err, fs.ErrNotExist) {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, err
	}
	return r, true, nil
}

func (l *dirLease) Save(_ context.Context, rec Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.checkLocked(); err != nil {
		if !l.released {
			l.markLost()
		}
		return err
	}
	return writeFile(l.rec, rec)
}

// Release stops the renewal and removes the lease file when it is still
// this lease's.
func (l *dirLease) Release(context.Context) error {
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return nil
	}
	// Expired or taken over, the file is another's, or anybody's: only a
	// live lease of this process's is removed.
	live := l.checkLocked() == nil
	l.released = true
	l.mu.Unlock()
	close(l.stop)
	l.done.Wait()
	if !live {
		return nil
	}
	return os.Remove(l.path)
}

func readFile(path string, v any) error {
	b, err := os.ReadFile(path) //nolint:gosec // G304: a file of the store's own directory
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("phase: %s: %w", path, err)
	}
	return nil
}

// writeFile replaces a file whole: a temporary file beside it, synced,
// renamed over it, and the directory synced, so that a crash leaves the
// old file or the new one, never half of either.
func writeFile(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	_, werr := f.Write(b)
	if werr == nil {
		werr = f.Sync()
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr == nil {
		werr = os.Rename(tmp, path)
	}
	if werr != nil {
		return errors.Join(werr, os.Remove(tmp))
	}
	if runtime.GOOS == "windows" {
		return nil // a directory cannot be opened for syncing there; the rename is the write
	}
	d, err := os.Open(dir) //nolint:gosec // G304: the store's own directory
	if err != nil {
		return err
	}
	serr := d.Sync()
	if cerr := d.Close(); serr == nil {
		serr = cerr
	}
	return serr
}
