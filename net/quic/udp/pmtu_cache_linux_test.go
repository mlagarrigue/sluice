package udp_test

import (
	"errors"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/mlagarrigue/sluice/net/quic/udp"
)

// Set in the re-executed binary to say it is already inside the namespace
// the parent made.
const netnsEnvVar = "SLUICE_UDP_PMTU_NETNS"

// Addresses of the throwaway namespace: a veth pair carries 10.99.0.0/24,
// and 10.200.0.0/24 is routed through it with a low MTU. The destination is
// deliberately a host nobody answers for — the kernel decides EMSGSIZE from
// the route alone, before any neighbour is resolved — and it must not be a
// local address, or the traffic would leave through the local table and the
// route's MTU would never apply.
const (
	nsLocalAddr = "10.99.0.1"
	nsDestAddr  = "10.200.0.5"
	nsRouteMTU  = 600
)

// A path MTU of 600 cached for the peer must not stop the 1200-byte
// datagram RFC 9000 §14.1 guarantees. Under IP_PMTUDISC_DO the kernel
// enforces that cache and refuses the datagram with EMSGSIZE, which
// §14.2.1 forbids acting on below 1200; udp.Configure asks for
// IP_PMTUDISC_PROBE so the cache is ignored.
//
// The cache is set here by a route MTU, which is the state an ICMP
// Fragmentation Needed installs, so no packet is crafted. That makes the
// probe silent on one thing it does not test: whether the kernel accepts
// such an ICMP from an off-path sender in the first place.
//
// The whole thing runs in a throwaway user and network namespace, so no
// route of the machine is touched. Without one it skips; the test job of
// ci.yml gives its runner one and refuses the skip.
func TestPathMTUCacheDoesNotRefuseMinimumDatagram(t *testing.T) {
	if os.Getenv(netnsEnvVar) == "" {
		reExecInNamespace(t)
		return
	}
	buildNamespaceTopology(t)

	// The control, and the reason a pass here means anything: a socket in
	// the DO mode must be refused, which proves the route's MTU is in force.
	// Without it a green test could mean the datagram never met the route.
	if err := sendMinimumDatagram(t, syscall.IP_PMTUDISC_DO); err == nil {
		t.Fatalf("inert probe: a %d-byte datagram left a DO-mode socket although the route to %s holds MTU %d",
			minDatagram, nsDestAddr, nsRouteMTU)
	} else if !errors.Is(err, syscall.EMSGSIZE) {
		t.Fatalf("inert probe: DO-mode socket failed with %v, want EMSGSIZE", err)
	}

	// What udp.Configure asks for: the cache is ignored and the datagram
	// leaves. This is the assertion that fails if the mode goes back to DO.
	if err := sendMinimumDatagram(t, 0); err != nil {
		t.Fatalf("a %d-byte datagram was refused although the kernel path-MTU cache must be ignored: %v",
			minDatagram, err)
	}
}

// QUIC's smallest allowed maximum datagram size (RFC 9000 §14.1).
const minDatagram = 1200

// sendMinimumDatagram sends one minDatagram-sized datagram to nsDestAddr. A
// mode of 0 means the socket udp.Listen configures; any other value is set
// over a plain socket instead, for the control above.
func sendMinimumDatagram(t *testing.T, mode int) error {
	t.Helper()
	var c *net.UDPConn
	var err error
	if mode == 0 {
		c, err = udp.Listen("udp4", net.JoinHostPort(nsLocalAddr, "0"))
	} else {
		c, err = net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP(nsLocalAddr)})
	}
	if err != nil {
		t.Fatalf("listen on %s: %v", nsLocalAddr, err)
	}
	defer c.Close()
	if mode != 0 {
		rc, err := c.SyscallConn()
		if err != nil {
			t.Fatal(err)
		}
		var setErr error
		if err := rc.Control(func(fd uintptr) {
			setErr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_MTU_DISCOVER, mode)
		}); err != nil {
			t.Fatal(err)
		}
		if setErr != nil {
			t.Fatalf("set IP_MTU_DISCOVER=%d: %v", mode, setErr)
		}
	}
	dst := &net.UDPAddr{IP: net.ParseIP(nsDestAddr), Port: 4433}
	_, err = c.WriteTo(make([]byte, minDatagram), dst)
	return err
}

// buildNamespaceTopology wires the namespace the parent created: a veth
// pair, an address on each end, and a route to nsDestAddr's network whose
// MTU stands in for a poisoned path-MTU cache entry.
func buildNamespaceTopology(t *testing.T) {
	t.Helper()
	for _, args := range [][]string{
		{"link", "set", "lo", "up"},
		{"link", "add", "veth0", "type", "veth", "peer", "name", "veth1"},
		{"addr", "add", nsLocalAddr + "/24", "dev", "veth0"},
		{"addr", "add", "10.99.0.2/24", "dev", "veth1"},
		{"link", "set", "veth0", "up"},
		{"link", "set", "veth1", "up"},
		{"route", "add", "10.200.0.0/24", "dev", "veth0", "mtu", strconv.Itoa(nsRouteMTU)},
	} {
		//nolint:gosec // G204: args is the literal table above, not input.
		if out, err := exec.Command("ip", args...).CombinedOutput(); err != nil {
			t.Fatalf("ip %s: %v: %s", strings.Join(args, " "), err, out)
		}
	}
}

// reExecInNamespace runs this one test again, inside a user and network
// namespace of its own, and reports what it said. The namespace has to come
// from outside the process: unshare of a network namespace needs a
// single-threaded caller, which a Go test never is.
func reExecInNamespace(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("ip"); err != nil {
		t.Skipf("iproute2 absent, no way to build the topology: %v", err)
	}
	// Whether a namespace can be had at all, asked cheaply, so that a
	// refusal here skips while a failure of the test below is a failure.
	if out, err := exec.Command("unshare", "-rn", "true").CombinedOutput(); err != nil {
		t.Skipf("no usable network namespace here: %v: %s", err, out)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Skipf("no test binary to re-execute: %v", err)
	}
	//nolint:gosec // G204: exe is this test binary, and the name is its own.
	cmd := exec.Command("unshare", "-rn", exe, "-test.run=^"+t.Name()+"$", "-test.v")
	cmd.Env = append(os.Environ(), netnsEnvVar+"=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("in the network namespace: %v\n%s", err, out)
	}
	t.Logf("in the network namespace:\n%s", out)
}
