package quic

import (
	"errors"
	"fmt"
	"net"
	"testing"
	"time"
)

// Every violation RFC 9000 names leaves with its own code, not
// INTERNAL_ERROR (0x1): a conforming peer and an interop tool read the code
// to learn who was wrong. Each row provokes one violation on a server
// connection, aborts on the error exactly as the read path does, and reads
// the code of the CONNECTION_CLOSE that actually went out — the frame, not
// the Go error, because the two disagreed: closeCodeFor asserted the
// outermost error's type, and a transportError wrapped with %w by the
// stream or parameter layer lost its code on the way to the wire.
func TestProtocolViolationsCarryTheirNormativeCodes(t *testing.T) {
	appendParam := func(dst []byte, id uint64, val []byte) []byte {
		dst = AppendVarint(dst, id)
		dst = AppendVarint(dst, uint64(len(val)))
		return append(dst, val...)
	}
	// What a well-behaved client announces to the server under test, which
	// each parameter row then spoils in one way.
	clientParams := func(spoil func(p *TransportParameters, raw []byte) []byte) []byte {
		p := DefaultParameters()
		p.initialSourceCID = []byte("cscid000")
		raw := appendParameters(nil, p)
		return spoil(&p, raw)
	}
	stream := func(offset uint64, data []byte, fin bool) []byte {
		typ := uint64(FrameStream | streamOFF | streamLEN)
		if fin {
			typ |= streamFIN
		}
		b := AppendVarint(nil, typ)
		b = AppendVarint(b, 0)
		b = AppendVarint(b, offset)
		b = AppendVarint(b, uint64(len(data)))
		return append(b, data...)
	}
	resetStream := func(finalSize uint64) []byte {
		b := AppendVarint(nil, FrameResetStream)
		b = AppendVarint(b, 0)
		b = AppendVarint(b, 0)
		return AppendVarint(b, finalSize)
	}

	cases := []struct {
		name    string
		section string
		want    uint64
		// wrapped rows return the code below a %w, the shape a direct
		// assertion on the outermost error used to turn into INTERNAL_ERROR.
		wrapped bool
		provoke func(c *Conn) error
	}{
		{
			name: "a STREAM frame past the stream's window", section: "§4.1", want: transportFlowControl, wrapped: true,
			provoke: func(c *Conn) error {
				window := DefaultParameters().InitialMaxStreamDataBidiRemote
				err := c.frames(spaceApplication, stream(window, []byte("x"), false), time.Now())
				return err
			},
		},
		{
			name: "a RESET_STREAM that moves a known final size", section: "§4.5", want: transportFinalSize,
			provoke: func(c *Conn) error {
				if err := c.frames(spaceApplication, stream(0, []byte("hello"), true), time.Now()); err != nil {
					return fmt.Errorf("the FIN itself was refused: %w", err)
				}
				err := c.frames(spaceApplication, resetStream(7), time.Now())
				return err
			},
		},
		{
			name: "an initial_source_connection_id that is not the packets'", section: "§7.3", want: transportParameterError,
			provoke: func(c *Conn) error {
				return c.handlePeerParameters(clientParams(func(p *TransportParameters, _ []byte) []byte {
					p.initialSourceCID = []byte("spliced0")
					return appendParameters(nil, *p)
				}))
			},
		},
		{
			name: "a client that sends original_destination_connection_id", section: "§18.2", want: transportParameterError,
			provoke: func(c *Conn) error {
				return c.handlePeerParameters(clientParams(func(_ *TransportParameters, raw []byte) []byte {
					return appendParam(raw, paramOriginalDCID, []byte("odcid000"))
				}))
			},
		},
		{
			name: "a transport parameter announced twice", section: "§7.4", want: transportParameterError,
			provoke: func(c *Conn) error {
				return c.handlePeerParameters(clientParams(func(_ *TransportParameters, raw []byte) []byte {
					return appendParam(raw, paramInitialMaxData, AppendVarint(nil, 1<<20))
				}))
			},
		},
		{
			name: "a max_udp_payload_size below 1200", section: "§18.2", want: transportParameterError,
			provoke: func(c *Conn) error {
				return c.handlePeerParameters(clientParams(func(p *TransportParameters, _ []byte) []byte {
					p.MaxUDPPayloadSize = 1199
					return appendParameters(nil, *p)
				}))
			},
		},
		{
			name: "a transport parameter longer than what remains", section: "§7.4", want: transportParameterError,
			provoke: func(c *Conn) error {
				return c.handlePeerParameters(clientParams(func(_ *TransportParameters, raw []byte) []byte {
					raw = AppendVarint(raw, paramInitialMaxData)
					return AppendVarint(raw, 40) // forty bytes announced, none follow
				}))
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pcA, pcB := newLoopbackPair(t)
			dcid := []byte("dcid0000")
			c := newConn(pcA, pcB.LocalAddr(), dcid, []byte("scid0000"), false, DefaultParameters())
			defer c.Close()
			if err := c.installInitial(dcid); err != nil {
				t.Fatal(err)
			}
			c.OnStreamFrames(CreditOnDelivery, func([]Frame) error { return nil })

			err := tc.provoke(c)
			if err == nil {
				t.Fatalf("%s: the violation was accepted", tc.section)
			}
			if tc.wrapped {
				if _, direct := err.(*transportError); direct { //nolint:errorlint // the point: the code sits below a %w
					t.Fatalf("the error reaches abort unwrapped; this row no longer proves the chain is searched: %v", err)
				}
			}
			c.abort(err)

			got := initialCloseCode(t, pcB, dcid)
			if got != tc.want {
				t.Errorf("%s: the CONNECTION_CLOSE carries %#x, want %#x (error: %v)", tc.section, got, tc.want, err)
			}
			if !errors.Is(err, ErrQUIC) {
				t.Errorf("the error does not wrap ErrQUIC: %v", err)
			}
		})
	}
}

// initialCloseCode reads the next packet the server under test sent, opens
// it under the client's Initial keys — public by construction — and
// returns the code of the one CONNECTION_CLOSE it carries.
func initialCloseCode(t *testing.T, pc net.PacketConn, dcid []byte) uint64 {
	t.Helper()
	if err := pc.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2048)
	n, _, err := pc.ReadFrom(buf)
	if err != nil {
		t.Fatalf("no CONNECTION_CLOSE went out: %v", err)
	}
	h, _, err := parseLongHeader(buf[:n])
	if err != nil || h.Type != packetInitial {
		t.Fatalf("the close is not an Initial (%v)", err)
	}
	secrets, err := initialSecrets(dcid)
	if err != nil {
		t.Fatal(err)
	}
	opener, err := newPacketOpener(secrets.Server)
	if err != nil {
		t.Fatal(err)
	}
	payload, _, err := opener.Open(h.Raw, h.PNOffset, 0)
	if err != nil {
		t.Fatalf("the close does not open under the client's Initial keys: %v", err)
	}
	frames, err := parseFrames(nil, payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 || frames[0].Type != frameConnectionClose {
		t.Fatalf("frames %+v, want one transport CONNECTION_CLOSE", frames)
	}
	return frames[0].value
}

// The closing period is three probe timeouts (RFC 9000 §10.2) on any real
// path, and ten seconds at most: the peer chooses most of a PTO — its
// max_ack_delay, legal up to 2^14 ms, and a smoothed RTT its delayed
// acknowledgements inflate — and a Listener's connection holds its MaxConns
// slots for the whole period, so an uncapped one let a peer keep a slot for
// a minute or more after each close.
func TestClosingPeriodIsThreePTOsWithACeiling(t *testing.T) {
	c := &Conn{}
	c.rtt.sample(80*time.Millisecond, 0)
	if got, want := c.closingPeriodLocked(), 3*c.rtt.pto(25*time.Millisecond); got != want {
		t.Errorf("an 80 ms path closes over %v, want three PTOs, %v", got, want)
	}

	// The legal extremes a peer can reach: max_ack_delay at 2^14 - 1 ms
	// and acknowledgements held back for seconds.
	c.hasPeerParams = true
	c.peerParams.sawMaxAckDelay = true
	c.peerParams.MaxAckDelay = (1<<14 - 1) * time.Millisecond
	for range 8 {
		c.rtt.sample(20*time.Second, 0)
	}
	if uncapped := 3 * c.rtt.pto(c.peerMaxAckDelayLocked()); uncapped < time.Minute {
		t.Fatalf("the extremes give an uncapped period of %v; the case no longer tests the ceiling", uncapped)
	}
	if got := c.closingPeriodLocked(); got != 10*time.Second {
		t.Errorf("the extremes close over %v, want the %v ceiling", got, 10*time.Second)
	}
}
