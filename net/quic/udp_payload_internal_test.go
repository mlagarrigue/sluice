package quic

import (
	"strconv"
	"testing"
)

// RFC 9000 §18.2 on this endpoint's own announcement: max_udp_payload_size
// below 1200 is invalid, and a conforming peer would close the handshake
// with TRANSPORT_PARAMETER_ERROR — so check refuses it before any peer
// sees it, and a field left at zero goes on the wire as 1452, never as 0.
func TestMaxUDPPayloadSizeIsAnnouncedWithin18_2(t *testing.T) {
	for _, n := range []uint64{1, 1199, 65528} {
		p := DefaultParameters()
		p.MaxUDPPayloadSize = n
		if err := p.check(); err == nil {
			t.Errorf("MaxUDPPayloadSize %d passed check; §18.2 bounds it to 1200..65527", n)
		}
	}
	for _, n := range []uint64{1200, 65527} {
		p := DefaultParameters()
		p.MaxUDPPayloadSize = n
		if err := p.check(); err != nil {
			t.Errorf("MaxUDPPayloadSize %d was refused: %v", n, err)
		}
	}

	p := DefaultParameters()
	p.MaxUDPPayloadSize = 0
	if err := p.check(); err != nil {
		t.Fatalf("MaxUDPPayloadSize 0 (meaning 1452) was refused: %v", err)
	}
	again, err := parseParameters(appendParameters(nil, p))
	if err != nil {
		t.Fatalf("a zero MaxUDPPayloadSize was announced as something a peer refuses: %v", err)
	}
	if again.MaxUDPPayloadSize != pmtuCommonSize {
		t.Errorf("a zero MaxUDPPayloadSize was announced as %d, want %d", again.MaxUDPPayloadSize, pmtuCommonSize)
	}
}

// RFC 9000 §18.2 on the peer's announcement: 0 is a value below 1200, not
// the absence the 65527 default stands in for.
func TestMaxUDPPayloadSizeZeroIsAParameterError(t *testing.T) {
	for _, n := range []uint64{0, 1199} {
		_, err := parseParameters(appendParam(nil, paramMaxUDPPayloadSize, AppendVarint(nil, n)))
		wantCode(t, err, transportParameterError, "max_udp_payload_size announced at "+strconv.FormatUint(n, 10))
	}
	p, err := parseParameters(appendParam(nil, paramMaxUDPPayloadSize, AppendVarint(nil, 1200)))
	if err != nil || p.MaxUDPPayloadSize != 1200 {
		t.Errorf("max_udp_payload_size 1200 parsed as %d, %v; want 1200 accepted", p.MaxUDPPayloadSize, err)
	}
}

// RFC 9000 §18.2: "The default for this parameter is the maximum permitted
// UDP payload of 65527." A peer that omits it reads everything, and the
// path search's ceiling is this end's own, not 0.
func TestAbsentMaxUDPPayloadSizeDefaultsTo65527(t *testing.T) {
	p, err := parseParameters(nil)
	if err != nil {
		t.Fatal(err)
	}
	if p.MaxUDPPayloadSize != pmtuMaxSize {
		t.Errorf("absent max_udp_payload_size read as %d, want the default %d", p.MaxUDPPayloadSize, pmtuMaxSize)
	}

	// A client's block without the parameter, as the server conn expects
	// it (initial_source_connection_id is what handlePeerParameters checks).
	raw := appendParam(nil, paramInitialSourceCID, []byte{1, 2, 3, 4})
	raw = appendParam(raw, paramInitialMaxData, AppendVarint(nil, 1<<20))
	c := boundsConn(t, false)
	if err := c.handlePeerParameters(raw); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	ceiling := c.pmtuCeilingLocked()
	c.mu.Unlock()
	if ceiling != pmtuCommonSize {
		t.Errorf("path search ceiling = %d facing a peer that omits max_udp_payload_size, want this end's own %d", ceiling, pmtuCommonSize)
	}
}
