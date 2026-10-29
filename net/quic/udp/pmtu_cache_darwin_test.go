package udp_test

import (
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice/net/quic/udp"
)

// Set by the CI step that installs a host route of MTU 600 toward this
// address; the test skips without it, and that step refuses the skip.
const pmtuDestEnvVar = "SLUICE_UDP_PMTU_DEST"

// A path MTU of 600 the kernel holds for the peer must not stop the
// 1200-byte datagram RFC 9000 §14.1 guarantees (§14.2.1 forbids acting on
// a path MTU below it). On macOS the path MTU an ICMP Fragmentation Needed
// teaches the kernel lives in the destination's host route, as its MTU;
// the CI step installs that route with `route add -host … -mtu 600`, so no
// packet is crafted, and the test sends from the socket udp.Listen
// configures — IP_DONTFRAG set — toward that destination.
//
// The control, and the reason a pass here means anything: the kernel is
// asked for the route it would send by and must report the MTU of 600.
// Without it a green test could mean the route was never in force.
func TestPathMTUCacheDoesNotRefuseMinimumDatagram(t *testing.T) {
	dest := os.Getenv(pmtuDestEnvVar)
	if dest == "" {
		t.Skipf("%s is not set: no host route of MTU %d to send by", pmtuDestEnvVar, routeMTU)
	}
	out, err := exec.Command("route", "-n", "get", dest).CombinedOutput()
	if err != nil {
		t.Fatalf("route -n get %s: %v: %s", dest, err, out)
	}
	if mtu, ok := routeGetMTU(string(out)); !ok || mtu != routeMTU {
		t.Fatalf("inert probe: the route to %s reports MTU %d (found %v), want %d:\n%s", dest, mtu, ok, routeMTU, out)
	}
	t.Logf("route -n get %s:\n%s", dest, out)

	c, err := udp.Listen("udp4", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	dst := &net.UDPAddr{IP: net.ParseIP(dest), Port: 4433}
	if _, err := c.WriteTo(make([]byte, minDatagram), dst); err != nil {
		t.Fatalf("a %d-byte datagram was refused toward a destination whose route holds MTU %d: %v",
			minDatagram, routeMTU, err)
	}
}

// The MTU the CI step gives the host route.
const routeMTU = 600

// QUIC's smallest allowed maximum datagram size (RFC 9000 §14.1).
const minDatagram = 1200

// routeGetMTU reads the mtu column of `route -n get`: a header line naming
// the metrics, then a line of their values.
func routeGetMTU(out string) (int, bool) {
	lines := strings.Split(out, "\n")
	for i, l := range lines {
		head := strings.Fields(l)
		col := -1
		for j, h := range head {
			if h == "mtu" {
				col = j
			}
		}
		if col < 0 || i+1 >= len(lines) {
			continue
		}
		vals := strings.Fields(lines[i+1])
		if col >= len(vals) {
			return 0, false
		}
		v, err := strconv.Atoi(vals[col])
		return v, err == nil
	}
	return 0, false
}
