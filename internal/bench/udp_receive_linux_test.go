package bench

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/net/quic/udp"
)

// What a QUIC receive socket loses in a burst, at the kernel's default
// receive buffer and at the 7 MiB that the udp-rmem check of package audit recommends.
//
// A sender writes a burst of datagrams the size of a QUIC packet as fast as
// the loopback takes them; a reader drains the socket concurrently, as a
// connection's read loop does: one read call per datagram, then either
// nothing (work=0, a lower bound on any real receive path) or 2µs of work,
// the order of one datagram through net/quic's receive path
// (BenchmarkConnReceivePacket: header, Open, frames, delivery). The unit is
// datagrams lost per burst, sent minus received, and the kernel's own count of them
// (RcvbufErrors in /proc/net/snmp, which counts every UDP socket of the
// machine: run on a quiet one). Counts, not nanoseconds: they need no paired
// timing to compare, only several rounds.
func BenchmarkUDPReceiveBurst(b *testing.B) {
	const datagram = 1200
	for _, work := range []time.Duration{0, 2 * time.Microsecond} {
		for _, burst := range []int{1024, 8192} {
			for _, buf := range []int{0, 7 << 20} {
				name := fmt.Sprintf("work=%v/burst=%d/buffer=default", work, burst)
				if buf != 0 {
					name = fmt.Sprintf("work=%v/burst=%d/buffer=%dMiB", work, burst, buf>>20)
				}
				b.Run(name, func(b *testing.B) {
					recv, err := udp.Listen("udp4", "127.0.0.1:0")
					if err != nil {
						b.Fatal(err)
					}
					defer func() { _ = recv.Close() }()
					if buf != 0 {
						if err := recv.SetReadBuffer(buf); err != nil {
							b.Fatal(err)
						}
					}
					granted := rcvbuf(b, recv)
					send, err := net.DialUDP("udp4", nil, recv.LocalAddr().(*net.UDPAddr))
					if err != nil {
						b.Fatal(err)
					}
					defer func() { _ = send.Close() }()
					payload := make([]byte, datagram)
					var lost, kernel int
					for b.Loop() {
						before := rcvbufErrors(b)
						got := make(chan int, 1)
						go func() {
							n, tmp := 0, make([]byte, 2048)
							for {
								// The burst is over once the socket stays empty
								// for this long: loopback delivers within it.
								_ = recv.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
								if _, err := recv.Read(tmp); err != nil {
									got <- n
									return
								}
								n++
								for start := time.Now(); time.Since(start) < work; {
								}
							}
						}()
						for range burst {
							if _, err := send.Write(payload); err != nil {
								b.Fatal(err)
							}
						}
						lost += burst - <-got
						kernel += rcvbufErrors(b) - before
					}
					b.ReportMetric(float64(lost)/float64(b.N), "lost/burst")
					b.ReportMetric(float64(kernel)/float64(b.N), "rcvbuferrors/burst")
					b.ReportMetric(float64(granted), "SO_RCVBUF")
				})
			}
		}
	}
}

// rcvbuf is the receive buffer the kernel granted c: twice what was asked,
// capped at net.core.rmem_max (socket(7)).
func rcvbuf(tb testing.TB, c *net.UDPConn) int {
	tb.Helper()
	rc, err := c.SyscallConn()
	if err != nil {
		tb.Fatal(err)
	}
	var v int
	var opErr error
	if err := rc.Control(func(fd uintptr) {
		v, opErr = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF)
	}); err != nil {
		tb.Fatal(err)
	}
	if opErr != nil {
		tb.Fatal(opErr)
	}
	return v
}

// rcvbufErrors reads the kernel's count of UDP datagrams dropped for a full
// receive buffer, machine-wide.
func rcvbufErrors(tb testing.TB) int {
	tb.Helper()
	f, err := os.Open("/proc/net/snmp")
	if err != nil {
		tb.Skipf("no /proc/net/snmp: %v", err)
	}
	defer func() { _ = f.Close() }()
	var head []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 || fields[0] != "Udp:" {
			continue
		}
		if head == nil {
			head = fields
			continue
		}
		for i, h := range head {
			if h == "RcvbufErrors" && i < len(fields) {
				n, err := strconv.Atoi(fields[i])
				if err != nil {
					tb.Fatal(err)
				}
				return n
			}
		}
	}
	tb.Fatal("no RcvbufErrors in /proc/net/snmp")
	return 0
}
