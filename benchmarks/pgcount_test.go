package benchmarks

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"
)

// countConn counts, on a benchmark connection, what the kernel sees: read and
// write calls, the reads and writes that found the socket not ready (EAGAIN,
// each one a park on the poller and a wakeup later), the time spent inside
// write, and the time a read waited between its first EAGAIN and its data.
// It reads and writes through the RawConn, which is the loop net.Conn runs
// internally, so the counts are exact rather than inferred. Enabled by
// SLUICE_PG_COUNT=1; otherwise the benchmarks dial a plain connection.
type countConn struct {
	*net.TCPConn
	rc syscall.RawConn
	ioStats
}

type ioStats struct {
	reads, readBytes, readEAGAIN, readParked int64
	writes, writeBytes, writeEAGAIN          int64
	writeNS, readWaitNS                      int64
}

func countingEnabled() bool { return os.Getenv("SLUICE_PG_COUNT") == "1" }

func dialCount(ctx context.Context, network, addr string) (net.Conn, error) {
	var d net.Dialer
	nc, err := d.DialContext(ctx, network, addr)
	if err != nil || !countingEnabled() {
		return nc, err
	}
	tc := nc.(*net.TCPConn)
	rc, err := tc.SyscallConn()
	if err != nil {
		return nil, err
	}
	c := &countConn{TCPConn: tc, rc: rc}
	lastCount = c
	return c, nil
}

// lastCount is the counting connection the running benchmark dialled last.
var lastCount *countConn

func (c *countConn) Read(p []byte) (int, error) {
	var n int
	var serr error
	var blockedAt time.Time
	err := c.rc.Read(func(fd uintptr) bool {
		n, serr = syscall.Read(int(fd), p)
		if errors.Is(serr, syscall.EAGAIN) {
			c.readEAGAIN++
			if blockedAt.IsZero() {
				blockedAt = time.Now()
			}
			return false
		}
		return true
	})
	c.reads++
	if !blockedAt.IsZero() {
		c.readParked++
		c.readWaitNS += int64(time.Since(blockedAt))
	}
	if err != nil {
		return 0, err
	}
	if serr != nil {
		return 0, serr
	}
	if n == 0 && len(p) > 0 {
		return 0, io.EOF
	}
	c.readBytes += int64(n)
	return n, nil
}

func (c *countConn) Write(p []byte) (int, error) {
	done := 0
	var serr error
	err := c.rc.Write(func(fd uintptr) bool {
		for done < len(p) {
			t := time.Now()
			n, e := syscall.Write(int(fd), p[done:])
			c.writeNS += int64(time.Since(t))
			c.writes++
			if errors.Is(e, syscall.EAGAIN) {
				c.writeEAGAIN++
				return false
			}
			if e != nil {
				serr = e
				return true
			}
			done += n
		}
		return true
	})
	c.writeBytes += int64(done)
	if err != nil {
		return done, err
	}
	return done, serr
}

// countReport starts the counters for one benchmark and returns the function
// that reports them per op. Voluntary context switches of the whole process
// stand for wakeups: with GOMAXPROCS=1 a parked read puts the only P's thread
// to sleep in epoll_wait, and its return is one switch.
func countReport(b *testing.B) func() {
	b.Helper()
	if !countingEnabled() || lastCount == nil {
		return func() {}
	}
	c := lastCount
	c.ioStats = ioStats{}
	var ru0 syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru0)
	return func() {
		var ru1 syscall.Rusage
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru1)
		n := float64(b.N)
		s := c.ioStats
		b.ReportMetric(float64(s.reads)/n, "reads/op")
		b.ReportMetric(float64(s.readBytes)/float64(max(s.reads, 1)), "B/read")
		b.ReportMetric(float64(s.readEAGAIN)/n, "rEAGAIN/op")
		b.ReportMetric(float64(s.readParked)/n, "parked/op")
		b.ReportMetric(float64(s.readWaitNS)/n/1e3, "waitµs/op")
		b.ReportMetric(float64(s.writes)/n, "writes/op")
		b.ReportMetric(float64(s.writeBytes)/n, "wB/op")
		b.ReportMetric(float64(s.writeEAGAIN)/n, "wEAGAIN/op")
		b.ReportMetric(float64(s.writeNS)/n/1e3, "writeµs/op")
		b.ReportMetric(float64(ru1.Nvcsw-ru0.Nvcsw)/n, "nvcsw/op")
		b.ReportMetric(float64(ru1.Nivcsw-ru0.Nivcsw)/n, "nivcsw/op")
		b.ReportMetric(float64((ru1.Utime.Nano()+ru1.Stime.Nano())-(ru0.Utime.Nano()+ru0.Stime.Nano()))/n/1e3, "cpuµs/op")
		b.ReportMetric(float64(ru1.Stime.Nano()-ru0.Stime.Nano())/n/1e3, "sysµs/op")
	}
}

// spinBatch burns SLUICE_PG_SPIN microseconds of CPU per batch, standing for
// a consumer that works on what it receives.
var spinPerBatch = func() time.Duration {
	d, _ := time.ParseDuration(os.Getenv("SLUICE_PG_SPIN"))
	return d
}()

func spinBatch() {
	if spinPerBatch == 0 {
		return
	}
	for t := time.Now(); time.Since(t) < spinPerBatch; {
	}
}
