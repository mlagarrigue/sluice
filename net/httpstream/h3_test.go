package httpstream

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/dgram"
	"github.com/mlagarrigue/sluice/net/quic"
)

// h3Request builds the bytes one request stream carries: an HTTP/3 HEADERS
// frame holding a QPACK field section of literals.
func h3Request(method, path string, extra ...[2]string) []byte {
	fields := []Header{
		{Name: []byte(":method"), Value: []byte(method)},
		{Name: []byte(":path"), Value: []byte(path)},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("h")},
	}
	for _, kv := range extra {
		fields = append(fields, Header{Name: []byte(kv[0]), Value: []byte(kv[1])})
	}
	return appendH3Frame(nil, h3Headers, appendQPACK(nil, fields))
}

// streamFrame wraps stream bytes as the QUIC frame that would carry them.
func streamFrame(id uint64, data []byte, fin bool) quic.Frame {
	return quic.Frame{Type: quic.FrameStream, StreamID: id, Data: data, Fin: fin}
}

// serveH3Datagram is the request path in one call, as these tests drive it:
// the frames of a datagram in, the handler's answers out, keyed by the stream
// each belongs on.
//
// [ServeH3] does this across two goroutines — assembly on the connection's
// read loop, the handler off it — which is what a server must do and what a
// test of the assembly alone does not need.
func serveH3Datagram(a *h3Assembler, frames []quic.Frame, handle Handler) (
	heads map[uint64][]byte, bodies map[uint64]sluice.Stream[[]byte], failed []h3StreamError, err error,
) {
	batch, ids, failed, err := a.Frames(frames)
	if err != nil || batch.Len() == 0 {
		return nil, nil, failed, err
	}
	res, err := apply(handle, batch)
	if err != nil {
		return nil, nil, failed, err
	}
	heads = make(map[uint64][]byte, len(ids))
	bodies = make(map[uint64]sluice.Stream[[]byte], len(ids))
	for i, id := range ids {
		if heads[id], err = appendH3ResponseBasic(nil, res[i]); err != nil {
			return nil, nil, failed, err
		}
		if res[i].streamed() {
			bodies[id] = res[i].Stream
		}
	}
	return heads, bodies, failed, nil
}

// The thesis at HTTP/3, and the transport where it costs nothing at all: one
// datagram carries frames for several streams, so the requests are grouped
// before this layer sees them. No pipelining to ask for, no multiplexing to
// design around — the datagram was always a batch.
func TestH3RequestsFromOneDatagramAreOneBatch(t *testing.T) {
	a := newH3Assembler(Config{})
	var frames []quic.Frame
	for i := range 4 {
		id := uint64(4 * i) // client-initiated bidirectional
		frames = append(frames, streamFrame(id, h3Request("GET", fmt.Sprintf("/r%d", i)), true))
	}

	var sizes []int
	out, _, _, err := serveH3Datagram(a, frames, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		sizes = append(sizes, b.Len())
		res := make([]Response, 0, b.Len())
		for _, r := range b.Items {
			res = append(res, Response{Status: 200, Body: append([]byte(nil), r.Target...)})
		}
		return sluice.Batch[Response]{Items: res}
	})
	if err != nil {
		t.Fatalf("serveH3Datagram returned %v", err)
	}
	if len(sizes) != 1 || sizes[0] != 4 {
		t.Fatalf("batch sizes %v, want one batch of 4", sizes)
	}
	if len(out) != 4 {
		t.Fatalf("%d streams answered, want 4", len(out))
	}
	for i := range 4 {
		id := uint64(4 * i)
		if len(out[id]) == 0 {
			t.Errorf("stream %d got no answer", id)
		}
	}
	t.Logf("one datagram: %d requests in one batch", sizes[0])
}

// A QUIC stream is a byte stream, so a frame split across datagrams is
// ordinary. The assembler must hold the remainder rather than refuse it.
func TestH3FrameSplitAcrossDatagrams(t *testing.T) {
	a := newH3Assembler(Config{})
	whole := h3Request("POST", "/split")
	cut := len(whole) / 2

	batch, _, _, err := a.Frames([]quic.Frame{streamFrame(0, whole[:cut], false)})
	if err != nil {
		t.Fatalf("the first half returned %v", err)
	}
	if batch.Len() != 0 {
		t.Fatal("a half-arrived request completed")
	}

	batch, ids, _, err := a.Frames([]quic.Frame{streamFrame(0, whole[cut:], true)})
	if err != nil {
		t.Fatalf("the second half returned %v", err)
	}
	if batch.Len() != 1 || len(ids) != 1 {
		t.Fatalf("the reassembled request did not complete: %d", batch.Len())
	}
	if string(batch.Items[0].Target) != "/split" {
		t.Errorf("target = %q", batch.Items[0].Target)
	}
}

// A request with a body: HEADERS then DATA, ended by the stream's FIN.
func TestH3AssemblesABody(t *testing.T) {
	a := newH3Assembler(Config{})
	stream := h3Request("POST", "/orders")
	stream = appendH3Frame(stream, h3Data, []byte(`{"qty":2}`))

	batch, _, _, err := a.Frames([]quic.Frame{streamFrame(0, stream, true)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Len() != 1 {
		t.Fatalf("%d requests", batch.Len())
	}
	if got := string(batch.Items[0].Body); got != `{"qty":2}` {
		t.Errorf("body = %q", got)
	}
}

// Unidirectional streams carry control, QPACK and push — never requests — so
// they are skipped rather than misread.
func TestH3IgnoresUnidirectionalStreams(t *testing.T) {
	a := newH3Assembler(Config{})
	batch, _, _, err := a.Frames([]quic.Frame{
		streamFrame(2, []byte{0x00}, false), // client-initiated unidirectional
		streamFrame(3, []byte{0x00}, false), // server-initiated unidirectional
		streamFrame(1, []byte{0x00}, false), // server-initiated bidirectional
	})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Len() != 0 {
		t.Errorf("%d requests came off streams that carry none", batch.Len())
	}
}

// The same refusals HTTP/2 makes, for the same reason: a request that reaches
// an HTTP/1.1 hop unchecked is how a smuggled request is written, and the hop
// does not care which version delivered it.
func TestH3RefusesMalformedRequests(t *testing.T) {
	tests := []struct {
		name   string
		fields []Header
	}{
		{"no :method", []Header{{[]byte(":path"), []byte("/x")}, {[]byte(":scheme"), []byte("https")}}},
		{"a repeated :path", []Header{
			{[]byte(":method"), []byte("GET")},
			{[]byte(":path"), []byte("/a")},
			{[]byte(":path"), []byte("/b")},
			{[]byte(":scheme"), []byte("https")},
		}},
		{"a connection-specific field", []Header{
			{[]byte(":method"), []byte("GET")},
			{[]byte(":path"), []byte("/x")},
			{[]byte(":scheme"), []byte("https")},
			{[]byte("connection"), []byte("keep-alive")},
		}},
		{"an uppercase field name", []Header{
			{[]byte(":method"), []byte("GET")},
			{[]byte(":path"), []byte("/x")},
			{[]byte(":scheme"), []byte("https")},
			{[]byte("X-Bad"), []byte("v")},
		}},
		{"a pseudo-header after a regular field", []Header{
			{[]byte(":method"), []byte("GET")},
			{[]byte("x-ok"), []byte("v")},
			{[]byte(":path"), []byte("/x")},
			{[]byte(":scheme"), []byte("https")},
		}},
		{"a :path that is not origin-form", []Header{
			{[]byte(":method"), []byte("GET")},
			{[]byte(":path"), []byte("http://elsewhere/x")},
			{[]byte(":scheme"), []byte("https")},
		}},
		// RFC 9114 §4.1.2/§10.3: QPACK can carry any byte, so the h1
		// parser's octet rules apply to what it decoded — these are the
		// CR/LF/NUL and token refusals that close the smuggling family at
		// any h3→h1 hop.
		{"a CRLF in a field value", []Header{
			{[]byte(":method"), []byte("GET")},
			{[]byte(":path"), []byte("/x")},
			{[]byte(":scheme"), []byte("https")},
			{[]byte("x-note"), []byte("a\r\nx-smuggled: b")},
		}},
		{"a NUL in a field value", []Header{
			{[]byte(":method"), []byte("GET")},
			{[]byte(":path"), []byte("/x")},
			{[]byte(":scheme"), []byte("https")},
			{[]byte("x-note"), []byte("a\x00b")},
		}},
		{"a field name that is not a token", []Header{
			{[]byte(":method"), []byte("GET")},
			{[]byte(":path"), []byte("/x")},
			{[]byte(":scheme"), []byte("https")},
			{[]byte("x note"), []byte("v")},
		}},
		{"a :method that is not a token", []Header{
			{[]byte(":method"), []byte("GET T")},
			{[]byte(":path"), []byte("/x")},
			{[]byte(":scheme"), []byte("https")},
		}},
		{"a :scheme that is not a token", []Header{
			{[]byte(":method"), []byte("GET")},
			{[]byte(":path"), []byte("/x")},
			{[]byte(":scheme"), []byte("ht\rtps")},
		}},
		{"a :path with a control character", []Header{
			{[]byte(":method"), []byte("GET")},
			{[]byte(":path"), []byte("/x\ry")},
			{[]byte(":scheme"), []byte("https")},
		}},
		{"an :authority with a control character", []Header{
			{[]byte(":method"), []byte("GET")},
			{[]byte(":path"), []byte("/x")},
			{[]byte(":scheme"), []byte("https")},
			{[]byte(":authority"), []byte("h\x00st")},
		}},
		// RFC 9114 §4.3.1: an authority that names nothing is malformed, not
		// a request delivered to the handler with an empty host.
		{"an empty :authority", []Header{
			{[]byte(":method"), []byte("GET")},
			{[]byte(":path"), []byte("/x")},
			{[]byte(":scheme"), []byte("https")},
			{[]byte(":authority"), []byte("")},
		}},
		{"an empty host", []Header{
			{[]byte(":method"), []byte("GET")},
			{[]byte(":path"), []byte("/x")},
			{[]byte(":scheme"), []byte("https")},
			{[]byte("host"), []byte("")},
		}},
		// RFC 9110 §5.5 keeps every C0 control but HTAB, and DEL, out of
		// field content — not only NUL, CR and LF.
		{"a field value with a C0 control", []Header{
			{[]byte(":method"), []byte("GET")},
			{[]byte(":path"), []byte("/x")},
			{[]byte(":scheme"), []byte("https")},
			{[]byte("x-note"), []byte("a\x0cb")},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := newH3Assembler(Config{})
			stream := appendH3Frame(nil, h3Headers, appendQPACK(nil, tt.fields))
			// A malformed request is one stream's failure, not the
			// connection's: the peer is told on the stream it used, and
			// whatever else the datagram carried is still served.
			_, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, stream, true)})
			if err != nil {
				t.Fatalf("Frames failed the connection over one stream: %v", err)
			}
			if len(failed) != 1 || !errors.Is(failed[0].Err, ErrH3Protocol) {
				t.Fatalf("Frames reported %v, want one stream error wrapping ErrH3Protocol", failed)
			}
			if failed[0].Code != h3MessageError {
				t.Errorf("stream reset with code %#x, want H3_MESSAGE_ERROR (%#x)", failed[0].Code, h3MessageError)
			}
		})
	}
}

// An empty override table turns every indexed field line into a refusal with
// a reason — the stance the package used to hold by default, still available
// to a caller who wants it.
func TestQPACKEmptyOverrideRefusesIndexedFieldLines(t *testing.T) {
	d := newQPACKDecoder([]Header{}, 0)
	for _, block := range [][]byte{
		{0x00, 0x00, 0xc1},            // an indexed field line, static table
		{0x00, 0x00, 0x51, 0x01, 'v'}, // a literal with a static name reference
	} {
		if _, err := d.Decode(nil, block); !errors.Is(err, errQPACKIndexed) {
			t.Errorf("Decode(%x) returned %v, want ErrQPACKIndexed", block, err)
		}
	}
}

// Literals round trip through what this package writes and reads, Huffman
// included — the decoder is HPACK's, over the code QPACK shares with it.
func TestQPACKLiteralRoundTrip(t *testing.T) {
	fields := []Header{
		{[]byte(":method"), []byte("GET")},
		{[]byte("x-trace"), []byte("abc-123")},
	}
	d := newQPACKDecoder(nil, 0)
	got, err := d.Decode(nil, appendQPACK(nil, fields))
	if err != nil {
		t.Fatalf("Decode returned %v", err)
	}
	if len(got) != len(fields) {
		t.Fatalf("%d fields, want %d", len(got), len(fields))
	}
	for i := range got {
		if string(got[i].Name) != string(fields[i].Name) || string(got[i].Value) != string(fields[i].Value) {
			t.Errorf("field %d = %q: %q", i, got[i].Name, got[i].Value)
		}
	}
}

// Reserved frame types exist so that a receiver refusing the unknown is found
// out early (RFC 9114 §7.2.8). Ignoring them is what keeps the protocol able
// to change.
func TestH3IgnoresReservedFrameTypes(t *testing.T) {
	stream := appendH3Frame(nil, 0x21, []byte("greasing"))
	stream = append(stream, h3Request("GET", "/x")...)

	a := newH3Assembler(Config{})
	batch, _, _, err := a.Frames([]quic.Frame{streamFrame(0, stream, true)})
	if err != nil {
		t.Fatalf("a reserved frame type was refused: %v", err)
	}
	if batch.Len() != 1 {
		t.Fatalf("%d requests", batch.Len())
	}
}

// End to end over a real QUIC connection: a TLS 1.3 handshake on UDP, a
// request on a stream, the handler's answer back on the same stream.
//
// This is what separates "testable" from "servable", and it is why the
// connection had to exist: everything before it fed the assembler frames a
// connection would have produced, and this produces them.
func TestH3OverARealConnection(t *testing.T) {
	// An in-memory pair rather than loopback UDP. There is no loss recovery
	// under this connection, so a datagram the kernel drops is dropped for
	// good — which made this test fail about one run in five under a loaded
	// -race suite, with a message about a handler that was never reached. The
	// handshake, the packet protection and the two endpoints are unchanged;
	// only the transport's right to lose a packet is gone.
	serverPC, clientPC := dgram.Pair()
	defer func() { _ = serverPC.Close() }()
	defer func() { _ = clientPC.Close() }()

	sizes := make(chan int, 4)
	serverUp := make(chan error, 1)
	serverDone := make(chan struct{})
	// Reaped before the next test starts, as in startH3.
	t.Cleanup(func() { <-serverDone })
	go func() {
		defer close(serverDone)
		buf := make([]byte, 2048)
		if err := serverPC.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			serverUp <- err
			return
		}
		n, peer, err := serverPC.ReadFrom(buf)
		if err != nil {
			serverUp <- err
			return
		}
		conn, err := quic.Accept(serverPC, peer, buf[:n], h3ServerTLS(t), quic.DefaultParameters())
		if err != nil {
			serverUp <- err
			return
		}
		serverUp <- nil
		_ = ServeH3(t.Context(), conn, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
			sizes <- b.Len()
			res := make([]Response, 0, b.Len())
			for _, r := range b.Items {
				res = append(res, Response{Status: 200, Body: append([]byte("answering "), r.Target...)})
			}
			return sluice.Batch[Response]{Items: res}
		})
	}()

	conn, err := quic.Dial(clientPC, serverPC.LocalAddr(), h3ClientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := <-serverUp; err != nil {
		t.Fatalf("the server: %v", err)
	}

	s, err := conn.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(h3Request("GET", "/orders/7")); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseWrite(); err != nil {
		t.Fatal(err)
	}

	select {
	case n := <-sizes:
		if n != 1 {
			t.Errorf("the handler saw a batch of %d", n)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the handler was never reached over a real connection")
	}

	// And the answer comes back on the same stream.
	reply := make([]byte, 512)
	n, err := s.Read(reply)
	if err != nil {
		t.Fatalf("reading the answer: %v", err)
	}
	frames, _, err := parseH3Frames(nil, reply[:n], 1<<20)
	if err != nil {
		t.Fatalf("the answer did not parse: %v", err)
	}
	var body string
	for _, f := range frames {
		if f.Type == h3Data {
			body = string(f.Payload)
		}
	}
	if body != "answering /orders/7" {
		t.Errorf("the answer carried %q", body)
	}
}

func h3ServerTLS(t *testing.T) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		MinVersion:   tls.VersionTLS13,
		CipherSuites: []uint16{tls.TLS_AES_128_GCM_SHA256},
		NextProtos:   []string{"h3"},
	}
}

func h3ClientTLS() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // G402: a certificate the test minted
		MinVersion:         tls.VersionTLS13,
		CipherSuites:       []uint16{tls.TLS_AES_128_GCM_SHA256},
		NextProtos:         []string{"h3"},
		ServerName:         "127.0.0.1",
	}
}

// A supplied static table resolves indexed field lines, which is what a real
// client sends and what this package refuses without one.
func TestQPACKResolvesIndexedFieldLinesWithASuppliedTable(t *testing.T) {
	// A stand-in for RFC 9204 Appendix A: the shape matters here, not the
	// contents, and the contents are the caller's to supply from the RFC.
	static := []Header{
		{Name: []byte(":authority"), Value: nil},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte("age"), Value: []byte("0")},
	}
	d := newQPACKDecoder(static, 0)

	// An indexed field line naming static entry 1, then a literal with a
	// static name reference to entry 2.
	block := []byte{0x00, 0x00} // the section prefix
	block = append(block, 0xc0|1)
	block = append(block, 0x50|2, 0x02, 'v', '1')

	got, err := d.Decode(nil, block)
	if err != nil {
		t.Fatalf("Decode returned %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("%d fields, want 2", len(got))
	}
	if string(got[0].Name) != ":path" || string(got[0].Value) != "/" {
		t.Errorf("field 0 = %q: %q", got[0].Name, got[0].Value)
	}
	if string(got[1].Name) != "age" || string(got[1].Value) != "v1" {
		t.Errorf("field 1 = %q: %q", got[1].Name, got[1].Value)
	}
}

// With no table given, the shipped RFC 9204 Appendix A resolves the same
// block: index 1 is :path with value "/".
func TestQPACKDefaultsToTheShippedTable(t *testing.T) {
	d := newQPACKDecoder(nil, 0)
	got, err := d.Decode(nil, []byte{0x00, 0x00, 0xc1})
	if err != nil {
		t.Fatalf("Decode returned %v", err)
	}
	if len(got) != 1 || string(got[0].Name) != ":path" || string(got[0].Value) != "/" {
		t.Fatalf("fields = %v, want :path /", got)
	}
}

// The field-section prefix is RFC 7541 prefixed integers, not QUIC varints
// (RFC 9204 §4.5.1). The two encodings agree on single bytes up to 0x3F, so
// only a Delta Base of 0x40 or more tells them apart: a QUIC-varint reader
// takes 0x64 as the first byte of a two-byte integer and eats the field line
// behind it.
func TestQPACKPrefixIsPrefixedIntegersNotVarints(t *testing.T) {
	d := newQPACKDecoder(nil, 0)
	// RIC = 0, S = 0, Delta Base = 100 — legal with RIC 0 (§4.5.1.2) — then
	// one indexed field line, static 1 (:path /).
	got, err := d.Decode(nil, []byte{0x00, 0x64, 0xc1})
	if err != nil {
		t.Fatalf("a conformant prefix with Delta Base 100 was refused: %v", err)
	}
	if len(got) != 1 || string(got[0].Name) != ":path" || string(got[0].Value) != "/" {
		t.Fatalf("fields = %v, want :path /", got)
	}
}

// S=1 says Base = RIC − Delta Base − 1, negative whenever RIC is zero — and
// zero is the only insert count this decoder accepts, so a set sign bit is
// always an invalid field block (RFC 9204 §4.5.1.2).
func TestQPACKRefusesANegativeBase(t *testing.T) {
	d := newQPACKDecoder(nil, 0)
	// S=1, Delta Base 1, then an indexed field line. The old QUIC-varint
	// reader took 0x81 as the first of four bytes and swallowed the three
	// behind it as the "integer" — this block decoded cleanly instead of
	// being the invalid field block §4.5.1.2 says it is.
	if _, err := d.Decode(nil, []byte{0x00, 0x81, 0x00, 0x00, 0xc1}); !errors.Is(err, ErrH3Protocol) {
		t.Fatalf("Decode returned %v, want a refusal of S=1 with an insert count of zero", err)
	}
}

// A literal field line with a literal name is 001NHxxx (RFC 9204 §4.5.6):
// N=1 marks a never-indexed field — what encoders set on authorization and
// cookie — and MUST decode exactly like N=0. A 0xf0 mask used to drop the
// whole 0x30–0x3F half to the default refusal.
func TestQPACKDecodesNeverIndexedLiterals(t *testing.T) {
	d := newQPACKDecoder(nil, 0)
	block := []byte{0x00, 0x00}
	block = append(block, 0x30|0x07, 0x01) // N=1, plain name of 8 bytes (7 + 1)
	block = append(block, []byte("x-secret")...)
	block = append(block, 0x06) // plain value of 6 bytes
	block = append(block, []byte("s3cr3t")...)

	got, err := d.Decode(nil, block)
	if err != nil {
		t.Fatalf("a never-indexed literal was refused: %v", err)
	}
	if len(got) != 1 || string(got[0].Name) != "x-secret" || string(got[0].Value) != "s3cr3t" {
		t.Fatalf("fields = %v, want x-secret: s3cr3t", got)
	}
}

// An oversized literal is refused on its declared length, before any Huffman
// decoding: the list-size check after the fact already refused the section,
// but by then the peer had bought a full decode — ~50× the advertised header
// budget per attempt. The literal is all 0xff, thirty ones of which are EOS:
// decoding it at all fails with an HPACK Huffman error, so a refusal that is
// ErrH3Protocol and not that is what proves the budget check came first.
func TestQPACKRefusesOversizedLiteralsBeforeDecoding(t *testing.T) {
	const maxList = 1 << 10
	d := newQPACKDecoder(nil, maxList)

	huge := bytes.Repeat([]byte{0xff}, 5000) // declared well past maxList, Huffman floor included
	block := []byte{0x00, 0x00}
	block = append(block, 0x37, 0x01) // literal name, 8 bytes
	block = append(block, []byte("x-padded")...)
	block = append(block, 0xff, 0x89, 0x26) // H=1, length 5000 (7-bit prefix + 2 octets)
	block = append(block, huge...)

	_, err := d.Decode(nil, block)
	if !errors.Is(err, ErrH3Protocol) {
		t.Fatalf("Decode returned %v, want a refusal", err)
	}
	if errors.Is(err, ErrHPACK) {
		t.Fatalf("Decode returned %v: the literal was Huffman-decoded before the budget check", err)
	}
}

// A capacity of zero was advertised, so a dynamic-table reference is a peer
// ignoring what it was told.
func TestQPACKRefusesDynamicReferences(t *testing.T) {
	d := newQPACKDecoder([]Header{{Name: []byte("x")}}, 0)
	for _, block := range [][]byte{
		{0x00, 0x00, 0x81},       // indexed, T clear: dynamic
		{0x00, 0x00, 0x41, 0x00}, // literal with a dynamic name reference
		{0x00, 0x00, 0x10},       // post-base reference
	} {
		if _, err := d.Decode(nil, block); !errors.Is(err, ErrH3Protocol) {
			t.Errorf("Decode(%x) returned %v, want a refusal", block, err)
		}
	}
}

// The control stream carries what RFC 9114 §6.2.1 requires, and a peer is
// entitled to wait for it.
func TestH3ControlStreamCarriesSettings(t *testing.T) {
	raw := appendH3Control(nil, 1<<16)

	typ, rest, err := quic.Varint(raw)
	if err != nil {
		t.Fatal(err)
	}
	if typ != h3StreamControl {
		t.Fatalf("the stream type is %#x, want the control stream", typ)
	}
	frames, _, err := parseH3Frames(nil, rest, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 || frames[0].Type != h3Settings {
		t.Fatalf("the control stream opens with %+v, want SETTINGS", frames)
	}
	settings, err := parseH3Settings(frames[0].Payload)
	if err != nil {
		t.Fatal(err)
	}
	// A capacity of zero is what binds the peer not to use a dynamic table,
	// which is what makes refusing those references conformant.
	if settings[h3SettingQPACKMaxTableCapacity] != 0 {
		t.Errorf("QPACK capacity = %d, want 0", settings[h3SettingQPACKMaxTableCapacity])
	}
	if settings[h3SettingMaxFieldSection] != 1<<16 {
		t.Errorf("max field section = %d", settings[h3SettingMaxFieldSection])
	}
	// §7.2.4.1 SHOULD: at least one setting from the reserved 0x1f*N+0x21
	// space, so peers that refuse unknown identifiers are found out early.
	grease := false
	for id := range settings {
		if id >= 0x21 && (id-0x21)%0x1f == 0 {
			grease = true
		}
	}
	if !grease {
		t.Error("the SETTINGS carry no reserved (GREASE) identifier")
	}
}

func TestParseH3SettingsRefusesReservedH2Identifiers(t *testing.T) {
	// RFC 9114 §7.2.4.1: HTTP/2's setting identifiers with no HTTP/3 meaning
	// MUST NOT appear, and receiving one is H3_SETTINGS_ERROR, not a skip.
	for _, id := range []uint64{0x00, 0x02, 0x03, 0x04, 0x05} {
		payload := quic.AppendVarint(nil, id)
		payload = quic.AppendVarint(payload, 1)
		if _, err := parseH3Settings(payload); !errors.Is(err, ErrH3Protocol) {
			t.Errorf("reserved setting %#x was accepted: %v", id, err)
		}
	}
}

// RFC 9114 §4.2.2: a response's field section SHOULD stay under the peer's
// advertised MAX_FIELD_SECTION_SIZE — the size being the uncompressed names
// and values plus 32 per field — and one that cannot is refused rather than
// sent to a client that said it will not take it.
func TestH3ResponseHonoursPeerFieldSectionLimit(t *testing.T) {
	res := Response{Status: 200, Headers: []Header{
		{Name: []byte("x-big"), Value: bytes.Repeat([]byte("v"), 100)},
	}}
	if _, err := appendH3Response(nil, res, nil, 64, &h3Scratch{}); err == nil {
		t.Error("a field section over the peer's limit was encoded")
	}
	if _, err := appendH3Response(nil, res, nil, 4096, &h3Scratch{}); err != nil {
		t.Errorf("a field section under the limit was refused: %v", err)
	}
	if _, err := appendH3Response(nil, res, nil, 0, &h3Scratch{}); err != nil {
		t.Errorf("no advertised limit still refused the response: %v", err)
	}
}

// The h1/h2 field-count bound, applied to what QPACK decodes: the byte bound
// is not this bound — near-empty fields fit the bytes while making every
// Request.Get walk hundreds of entries.
func TestH3RefusesABlockOfTooManyFields(t *testing.T) {
	fields := []Header{
		{[]byte(":method"), []byte("GET")},
		{[]byte(":path"), []byte("/x")},
		{[]byte(":scheme"), []byte("https")},
	}
	for i := range 70 {
		fields = append(fields, Header{fmt.Appendf(nil, "x-f%d", i), []byte("v")})
	}
	a := newH3Assembler(Config{}) // default MaxHeaders: 64
	stream := appendH3Frame(nil, h3Headers, appendQPACK(nil, fields))
	_, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, stream, true)})
	if err != nil {
		t.Fatalf("Frames failed the connection over one stream: %v", err)
	}
	if len(failed) != 1 || !errors.Is(failed[0].Err, ErrTooLarge) {
		t.Fatalf("Frames reported %v, want one stream error wrapping ErrTooLarge", failed)
	}
	if failed[0].Code != h3ExcessiveLoad {
		t.Errorf("stream reset with code %#x, want H3_EXCESSIVE_LOAD (%#x)", failed[0].Code, h3ExcessiveLoad)
	}
}

// The output half of the octet rules: a handler cannot emit a field value
// carrying a C0 control (HTAB excepted) or DEL, same as h1 and h2 refuse.
func TestH3ResponseRefusesControlBytesInFieldValues(t *testing.T) {
	res := Response{Status: 200, Headers: []Header{
		{Name: []byte("x-note"), Value: []byte("a\x00b")},
	}}
	if _, err := appendH3Response(nil, res, nil, 0, &h3Scratch{}); err == nil {
		t.Error("a response field value with a NUL was encoded")
	}
	res.Headers[0].Value = []byte("a\tb")
	if _, err := appendH3Response(nil, res, nil, 0, &h3Scratch{}); err != nil {
		t.Errorf("a response field value with an HTAB was refused: %v", err)
	}
}

// RFC 9114 §8.1: a graceful close SHOULD sometimes carry a reserved error
// code instead of H3_NO_ERROR, and must never carry anything else.
func TestH3GracefulCodeIsNoErrorOrReserved(t *testing.T) {
	reserved := 0
	for range 1024 {
		switch code := h3GracefulCode(); {
		case code == h3NoError:
		case code >= 0x21 && (code-0x21)%0x1f == 0:
			reserved++
		default:
			t.Fatalf("graceful close code %#x is neither H3_NO_ERROR nor reserved", code)
		}
	}
	if reserved == 0 {
		t.Error("1024 graceful closes never greased the error code")
	}
}

func TestParseH3SettingsRefusesARepeatedIdentifier(t *testing.T) {
	var payload []byte
	payload = quic.AppendVarint(payload, h3SettingMaxFieldSection)
	payload = quic.AppendVarint(payload, 1)
	payload = quic.AppendVarint(payload, h3SettingMaxFieldSection)
	payload = quic.AppendVarint(payload, 2)
	if _, err := parseH3Settings(payload); !errors.Is(err, ErrH3Protocol) {
		t.Fatalf("a repeated setting was accepted: %v", err)
	}
}

// And over HTTP/3, closing the set: the same Response, streamed, on all three.
func TestH3StreamedResponse(t *testing.T) {
	a := newH3Assembler(Config{})
	frames := []quic.Frame{streamFrame(0, h3Request("GET", "/export"), true)}

	heads, bodies, _, err := serveH3Datagram(a, frames, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		return sluice.Batch[Response]{Items: []Response{{
			Status: 200,
			Stream: func(yield func(sluice.Batch[[]byte]) bool) {
				for i := range 3 {
					if !yield(sluice.Batch[[]byte]{Items: [][]byte{fmt.Appendf(nil, "row%d;", i)}}) {
						return
					}
				}
			},
		}}}
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(heads) != 1 || len(bodies) != 1 {
		t.Fatalf("%d heads and %d bodies, want one of each", len(heads), len(bodies))
	}

	// The headers carry no content-length, because the length is not known.
	parsed, _, err := parseH3Frames(nil, heads[0], 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	d := newQPACKDecoder(nil, 0)
	for _, f := range parsed {
		if f.Type == h3Data {
			t.Error("a streamed response put its payload in the head")
			continue
		}
		if f.Type != h3Headers {
			continue
		}
		fields, err := d.Decode(nil, f.Payload)
		if err != nil {
			t.Fatal(err)
		}
		for _, h := range fields {
			if string(h.Name) == "content-length" {
				t.Errorf("a streamed response stated content-length: %q", h.Value)
			}
		}
	}

	// And the body arrives as DATA frames, one batch at a time.
	var got strings.Builder
	bodies[0](func(b sluice.Batch[[]byte]) bool {
		out := appendH3StreamedBody(nil, b.Items)
		frames, _, err := parseH3Frames(nil, out, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range frames {
			if f.Type != h3Data {
				t.Errorf("a streamed body carried frame %#x", f.Type)
			}
			got.WriteString(string(f.Payload))
		}
		return true
	})
	if got.String() != "row0;row1;row2;" {
		t.Errorf("the streamed body was %q", got.String())
	}
}

// The authority reaches the handler under the same name it does on the other
// two protocols. HTTP/3 used to deliver it as a pseudo-header left in the
// regular list, so a handler had to know which transport it was talking to in
// order to find the host — which is what taking a batch of requests was for.
func TestH3DeliversTheAuthorityAsHost(t *testing.T) {
	a := newH3Assembler(Config{})
	batch, _, _, err := a.Frames([]quic.Frame{
		streamFrame(0, h3Request("GET", "/a"), true),
	})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Len() != 1 {
		t.Fatalf("%d requests", batch.Len())
	}
	if got := string(batch.Items[0].Get("host")); got != "h" {
		t.Errorf("the handler reads host %q, want %q", got, "h")
	}
	if got := batch.Items[0].Get(":authority"); got != nil {
		t.Errorf("the pseudo-header survived into the field list as %q", got)
	}
}

// Two hops reading a different host out of one request is how a request is
// routed somewhere it was not addressed (RFC 9114 §4.3.1).
func TestH3RefusesHostDisagreeingWithAuthority(t *testing.T) {
	a := newH3Assembler(Config{})
	_, _, failed, err := a.Frames([]quic.Frame{
		streamFrame(0, h3Request("GET", "/a", [2]string{"host", "other.test"}), true),
	})
	if err != nil {
		t.Fatalf("one malformed request failed the connection: %v", err)
	}
	if len(failed) != 1 || !errors.Is(failed[0].Err, ErrH3Protocol) {
		t.Errorf("a request naming two hosts returned %v, want one stream error wrapping ErrH3Protocol", failed)
	}
}

// A handler bug must not become a malformed :status on the wire, on any of
// the three protocols.
func TestH3RefusesAStatusThatIsNotOne(t *testing.T) {
	// Zero is not in the list: it means 200 and is the documented default.
	for _, status := range []int{-1, 99, 600, 700} {
		if _, err := appendH3ResponseBasic(nil, Response{Status: status}); !errors.Is(err, ErrMalformed) {
			t.Errorf("status %d returned %v, want ErrMalformed", status, err)
		}
	}
}

// One malformed request on a connection costs that request, over a real QUIC
// connection and not only in the assembler.
//
// It is the same criterion the HTTP/2 side is held to, and the same failure it
// used to have: every refusal a server could express ended the connection, so
// a client that sent one bad request lost the good ones with it. HTTP/3's
// answer is RESET_STREAM, which reaches the client as an error on that stream
// alone.
func TestH3ResetsOneStreamAndServesTheRest(t *testing.T) {
	// An in-memory pair rather than loopback UDP. There is no loss recovery
	// under this connection, so a datagram the kernel drops is dropped for
	// good — which made this test fail about one run in five under a loaded
	// -race suite, with a message about a handler that was never reached. The
	// handshake, the packet protection and the two endpoints are unchanged;
	// only the transport's right to lose a packet is gone.
	serverPC, clientPC := dgram.Pair()
	defer func() { _ = serverPC.Close() }()
	defer func() { _ = clientPC.Close() }()

	serverUp := make(chan error, 1)
	serverDone := make(chan struct{})
	// Reaped before the next test starts, as in startH3.
	t.Cleanup(func() { <-serverDone })
	go func() {
		defer close(serverDone)
		buf := make([]byte, 2048)
		if err := serverPC.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			serverUp <- err
			return
		}
		n, peer, err := serverPC.ReadFrom(buf)
		if err != nil {
			serverUp <- err
			return
		}
		conn, err := quic.Accept(serverPC, peer, buf[:n], h3ServerTLS(t), quic.DefaultParameters())
		if err != nil {
			serverUp <- err
			return
		}
		serverUp <- nil
		_ = ServeH3(t.Context(), conn, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
			res := make([]Response, 0, b.Len())
			for _, r := range b.Items {
				res = append(res, Response{Status: 200, Body: append([]byte("answering "), r.Target...)})
			}
			return sluice.Batch[Response]{Items: res}
		})
	}()

	conn, err := quic.Dial(clientPC, serverPC.LocalAddr(), h3ClientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := <-serverUp; err != nil {
		t.Fatalf("the server: %v", err)
	}

	// An uppercase field name, which RFC 9114 §4.2 makes malformed for the
	// same reason HTTP/2 does: a name normalised rather than refused means one
	// thing to this hop and another to the next.
	bad, err := conn.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bad.Write(h3Request("GET", "/bad", [2]string{"X-Uppercase", "1"})); err != nil {
		t.Fatal(err)
	}
	if err := bad.CloseWrite(); err != nil {
		t.Fatal(err)
	}

	good, err := conn.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := good.Write(h3Request("GET", "/good")); err != nil {
		t.Fatal(err)
	}
	if err := good.CloseWrite(); err != nil {
		t.Fatal(err)
	}

	reply := make([]byte, 512)
	n, err := good.Read(reply)
	if err != nil {
		t.Fatalf("the healthy request went unanswered: %v (conn: %v)", err, conn.Err())
	}
	frames, _, err := parseH3Frames(nil, reply[:n], 1<<20)
	if err != nil {
		t.Fatalf("the answer did not parse: %v", err)
	}
	var body string
	for _, f := range frames {
		if f.Type == h3Data {
			body = string(f.Payload)
		}
	}
	if body != "answering /good" {
		t.Errorf("the healthy request answered %q", body)
	}

	if code, reset := bad.WasReset(); !reset {
		t.Error("the malformed request's stream was not reset")
	} else if code != h3MessageError {
		t.Errorf("the stream was reset with %#x, want H3_MESSAGE_ERROR (%#x)", code, h3MessageError)
	}
	if err := conn.Err(); err != nil {
		t.Errorf("the connection ended over one malformed request: %v", err)
	}
}

// ServeH3's responder hand-off is provably deadlock-free only while the
// transport admits no more request streams than the hand-off channel holds.
// A connection whose announced stream limit exceeds that cap is refused
// outright, because serving it would trade a visible error for a latent
// read-loop deadlock.
func TestServeH3RefusesAStreamLimitPastTheHandoffCap(t *testing.T) {
	serverPC, clientPC := dgram.Pair()
	defer func() { _ = serverPC.Close() }()
	defer func() { _ = clientPC.Close() }()

	params := quic.DefaultParameters()
	params.InitialMaxStreamsBidi = h3MaxServeStreams + 1

	served := make(chan error, 1)
	go func() {
		buf := make([]byte, 2048)
		if err := serverPC.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			served <- err
			return
		}
		n, peer, err := serverPC.ReadFrom(buf)
		if err != nil {
			served <- err
			return
		}
		conn, err := quic.Accept(serverPC, peer, buf[:n], h3ServerTLS(t), params)
		if err != nil {
			served <- err
			return
		}
		served <- ServeH3(t.Context(), conn, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
			return sluice.Batch[Response]{}
		})
	}()

	conn, err := quic.Dial(clientPC, serverPC.LocalAddr(), h3ClientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	select {
	case err := <-served:
		if err == nil || !strings.Contains(err.Error(), "InitialMaxStreamsBidi") {
			t.Fatalf("ServeH3 returned %v, want a refusal naming InitialMaxStreamsBidi", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("ServeH3 accepted a stream limit that breaks the hand-off proof")
	}
	// The refusal reaches the client as a close, not as a server gone quiet.
	select {
	case <-conn.Done():
		if code, _, ok := conn.PeerClosed(); !ok || code != h3InternalError {
			t.Errorf("the client saw close code %#x (application close %v), want H3_INTERNAL_ERROR", code, ok)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the refused connection was left open")
	}
}

// TestH3AssemblerManualCreditBalances proves the release arithmetic the
// manual-credit transport depends on: across every path a frame can take —
// absorbed into assembly, consumed by header decoding, failed, forgotten,
// carried on a unidirectional stream — delivered bytes always equal released
// bytes plus what the assembler still retains, and a completed request's
// body stays unreleased for the responder to settle.
func TestH3AssemblerManualCreditBalances(t *testing.T) {
	a := newH3Assembler(Config{})
	released := make(map[uint64]uint64)
	var releasedTotal uint64
	a.Credit = func(id, n uint64) {
		released[id] += n
		releasedTotal += n
	}
	retained := func() int {
		total := a.assembling
		for _, u := range a.unis {
			total += len(u.buf)
		}
		return total
	}
	var delivered uint64
	feed := func(f quic.Frame) (sluice.Batch[Request], []h3StreamError) {
		delivered += uint64(len(f.Data))
		batch, _, failed, err := a.Frames([]quic.Frame{f})
		if err != nil {
			t.Fatal(err)
		}
		return batch, failed
	}
	balance := func(stage string, handedOff uint64) {
		t.Helper()
		if releasedTotal+uint64(retained())+handedOff != delivered { //nolint:gosec // G115: test sums
			t.Fatalf("%s: delivered %d ≠ released %d + retained %d + handed-off %d",
				stage, delivered, releasedTotal, retained(), handedOff)
		}
	}

	// A request with a body, split so the carry buffer is exercised: the
	// HEADERS bytes release on decode, the body stays retained.
	body := make([]byte, 300)
	stream := h3Request("POST", "/upload", [2]string{"content-length", "300"})
	stream = appendH3Frame(stream, h3Data, body)
	cut := len(stream) - 100
	feed(streamFrame(0, stream[:cut], false))
	balance("mid-request", 0)
	batch, _ := feed(streamFrame(0, stream[cut:], true))
	if batch.Len() != 1 {
		t.Fatalf("assembled %d requests, want 1", batch.Len())
	}
	// Complete: everything but the handed-off body is released.
	balance("request complete", uint64(len(batch.Items[0].Body)))
	if released[0] == 0 {
		t.Error("header bytes of stream 0 were never released")
	}
	handedOff := uint64(len(batch.Items[0].Body))

	// A stream that fails mid-assembly releases everything it held.
	feed(streamFrame(4, stream[:cut], false))
	a.fail(4, h3RequestIncomplete, ErrH3Protocol)
	balance("failed stream", handedOff)

	// Stragglers on the tombstone release immediately.
	feed(streamFrame(4, []byte("late bytes"), false))
	balance("tombstone straggler", handedOff)

	// A reset stream releases its carry via Forget.
	feed(streamFrame(8, stream[:cut], false))
	a.Forget(8)
	balance("forgotten stream", handedOff)

	// A unidirectional stream: the control stream's SETTINGS are consumed,
	// not retained.
	feed(quic.Frame{Type: quic.FrameStream, StreamID: 2, Data: appendH3Control(nil, 0)})
	balance("control stream", handedOff)
}

// A client that withholds one response stream's flow-control credit used to
// stall every response on the connection behind it — the single responder
// wrote in order, and order meant shared fate. With per-stream writers the
// starved response stalls alone; this is the test that pins the lift.
func TestH3StarvedStreamDoesNotBlockOtherResponses(t *testing.T) {
	serverPC, clientPC := dgram.Pair()
	defer func() { _ = serverPC.Close() }()
	defer func() { _ = clientPC.Close() }()

	serverUp := make(chan error, 1)
	largeSeen := make(chan struct{})
	serverDone := make(chan struct{})
	// Reaped before the next test starts, as in startH3.
	t.Cleanup(func() { <-serverDone })
	go func() {
		defer close(serverDone)
		buf := make([]byte, 2048)
		if err := serverPC.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			serverUp <- err
			return
		}
		n, peer, err := serverPC.ReadFrom(buf)
		if err != nil {
			serverUp <- err
			return
		}
		conn, err := quic.Accept(serverPC, peer, buf[:n], h3ServerTLS(t), quic.DefaultParameters())
		if err != nil {
			serverUp <- err
			return
		}
		serverUp <- nil
		_ = ServeH3(t.Context(), conn, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
			res := make([]Response, 0, b.Len())
			for _, r := range b.Items {
				if string(r.Target) == "/large" {
					close(largeSeen)
					// Far past the starved stream's window: its writer
					// parks on credit that never comes.
					res = append(res, Response{Status: 200, Body: make([]byte, 8<<10)})
					continue
				}
				res = append(res, Response{Status: 200, Body: []byte("small answer")})
			}
			return sluice.Batch[Response]{Items: res}
		})
	}()

	// The client grants each stream it opens a 128-byte window and then
	// never reads the large response — read-mode credit is granted on Read,
	// so not reading is exactly how a client starves one stream.
	params := quic.DefaultParameters()
	params.InitialMaxStreamDataBidiLocal = 128
	conn, err := quic.Dial(clientPC, serverPC.LocalAddr(), h3ClientTLS(), params)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := <-serverUp; err != nil {
		t.Fatalf("the server: %v", err)
	}

	large, err := conn.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := large.Write(h3Request("GET", "/large")); err != nil {
		t.Fatal(err)
	}
	if err := large.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	// The large request is handled — its batch is the responder's — before
	// the small one is even sent: the small request is a later batch, the
	// order that used to deadlock behind the stalled write.
	select {
	case <-largeSeen:
	case <-time.After(10 * time.Second):
		t.Fatal("the large request never reached the handler")
	}

	small, err := conn.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := small.Write(h3Request("GET", "/small")); err != nil {
		t.Fatal(err)
	}
	if err := small.CloseWrite(); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		reply := make([]byte, 512)
		n, err := small.Read(reply)
		if err != nil {
			done <- err
			return
		}
		frames, _, err := parseH3Frames(nil, reply[:n], 1<<20)
		if err != nil {
			done <- err
			return
		}
		for _, f := range frames {
			if f.Type == h3Data && string(f.Payload) == "small answer" {
				done <- nil
				return
			}
		}
		done <- fmt.Errorf("no DATA with the small answer in %d bytes", n)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the small response: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the small response never arrived: a starved stream still blocks its neighbours")
	}
}

// An upload larger than the transport's initial stream window only
// completes if the assembler's releases keep re-opening the windows —
// under ServeH3's manual credit, a byte the assembler never releases is a
// window the peer never gets back. 600 KiB against a 256 KiB stream window
// and a 1 MiB connection window forces several rounds of re-crediting.
func TestH3UploadLargerThanTheInitialWindows(t *testing.T) {
	serverPC, clientPC := dgram.Pair()
	defer func() { _ = serverPC.Close() }()
	defer func() { _ = clientPC.Close() }()

	serverUp := make(chan error, 1)
	got := make(chan int, 1)
	serverDone := make(chan struct{})
	// Reaped before the next test starts, as in startH3.
	t.Cleanup(func() { <-serverDone })
	go func() {
		defer close(serverDone)
		buf := make([]byte, 2048)
		if err := serverPC.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			serverUp <- err
			return
		}
		n, peer, err := serverPC.ReadFrom(buf)
		if err != nil {
			serverUp <- err
			return
		}
		conn, err := quic.Accept(serverPC, peer, buf[:n], h3ServerTLS(t), Config{}.TransportParameters())
		if err != nil {
			serverUp <- err
			return
		}
		serverUp <- nil
		_ = ServeH3(t.Context(), conn, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
			res := make([]Response, 0, b.Len())
			for _, r := range b.Items {
				select {
				case got <- len(r.Body):
				default:
				}
				res = append(res, Response{Status: 200})
			}
			return sluice.Batch[Response]{Items: res}
		})
	}()

	conn, err := quic.Dial(clientPC, serverPC.LocalAddr(), h3ClientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := <-serverUp; err != nil {
		t.Fatalf("the server: %v", err)
	}

	// Eight uploads whose sum (4.8 MiB) exceeds the connection window the
	// server announced (just over 4 MiB): past the window, every further
	// byte of credit exists only because a finished body was released.
	const bodyLen = 600 << 10
	for i := range 8 {
		s, err := conn.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		stream := h3Request("POST", "/upload", [2]string{"content-length", "614400"})
		stream = appendH3Frame(stream, h3Data, make([]byte, bodyLen))
		wrote := make(chan error, 1)
		go func() {
			if _, err := s.Write(stream); err != nil {
				wrote <- err
				return
			}
			wrote <- s.CloseWrite()
		}()
		select {
		case err := <-wrote:
			if err != nil {
				t.Fatalf("upload %d: %v", i, err)
			}
		case <-time.After(20 * time.Second):
			t.Fatalf("upload %d stalled: released bytes are not re-opening the flow-control windows", i)
		}
		select {
		case n := <-got:
			if n != bodyLen {
				t.Errorf("upload %d: the handler saw %d bytes, want %d", i, n, bodyLen)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("upload %d never reached the handler", i)
		}
	}
}

// A streamed response's producer runs on the response's writer goroutine,
// after apply's recover is gone from the stack. A panic there used to end the
// process; over a real connection, it resets its own stream with
// H3_INTERNAL_ERROR and the connection keeps serving.
func TestH3StreamProducerPanicResetsOneStream(t *testing.T) {
	serverPC, clientPC := dgram.Pair()
	defer func() { _ = serverPC.Close() }()
	defer func() { _ = clientPC.Close() }()

	serverUp := make(chan error, 1)
	serverDone := make(chan struct{})
	// Reaped before the next test starts, as in startH3.
	t.Cleanup(func() { <-serverDone })
	go func() {
		defer close(serverDone)
		buf := make([]byte, 2048)
		if err := serverPC.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			serverUp <- err
			return
		}
		n, peer, err := serverPC.ReadFrom(buf)
		if err != nil {
			serverUp <- err
			return
		}
		conn, err := quic.Accept(serverPC, peer, buf[:n], h3ServerTLS(t), quic.DefaultParameters())
		if err != nil {
			serverUp <- err
			return
		}
		serverUp <- nil
		_ = ServeH3(t.Context(), conn, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
			res := make([]Response, 0, b.Len())
			for _, r := range b.Items {
				if string(r.Target) != "/boom" {
					res = append(res, Response{Status: 200, Body: append([]byte("answering "), r.Target...)})
					continue
				}
				res = append(res, Response{Status: 200, Stream: func(yield func(sluice.Batch[[]byte]) bool) {
					if !yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte("first")}}) {
						return
					}
					panic("producer bug")
				}})
			}
			return sluice.Batch[Response]{Items: res}
		})
	}()

	conn, err := quic.Dial(clientPC, serverPC.LocalAddr(), h3ClientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := <-serverUp; err != nil {
		t.Fatalf("the server: %v", err)
	}

	ask := func(path string) *quic.Stream {
		t.Helper()
		s, err := conn.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Write(h3Request("GET", path)); err != nil {
			t.Fatal(err)
		}
		if err := s.CloseWrite(); err != nil {
			t.Fatal(err)
		}
		return s
	}
	// readAll reads a stream to its end, under a bound: quic.Stream has no
	// read deadline of its own.
	readAll := func(s *quic.Stream) ([]byte, error) {
		t.Helper()
		type result struct {
			b   []byte
			err error
		}
		done := make(chan result, 1)
		go func() {
			var got []byte
			buf := make([]byte, 512)
			for {
				n, err := s.Read(buf)
				got = append(got, buf[:n]...)
				if err != nil {
					done <- result{got, err}
					return
				}
			}
		}()
		select {
		case r := <-done:
			return r.b, r.err
		case <-time.After(5 * time.Second):
			t.Fatal("the stream neither ended nor was reset")
			return nil, nil
		}
	}

	boom := ask("/boom")
	if _, err := readAll(boom); err == nil {
		t.Error("the panicking stream ended cleanly, as if its body were whole")
	}
	if code, reset := boom.WasReset(); !reset {
		t.Error("the panicking stream was not reset")
	} else if code != h3InternalError {
		t.Errorf("the stream was reset with %#x, want H3_INTERNAL_ERROR (%#x)", code, h3InternalError)
	}

	ok := ask("/ok")
	reply, err := readAll(ok)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("the request after the panic was not answered: %v (conn: %v)", err, conn.Err())
	}
	frames, _, err := parseH3Frames(nil, reply, 1<<20)
	if err != nil {
		t.Fatalf("the answer did not parse: %v", err)
	}
	var body strings.Builder
	for _, f := range frames {
		if f.Type == h3Data {
			body.WriteString(string(f.Payload))
		}
	}
	if body.String() != "answering /ok" {
		t.Errorf("the request after the panic answered %q", body.String())
	}
	if err := conn.Err(); err != nil {
		t.Errorf("the connection ended over one producer panic: %v", err)
	}
}

// Response field names obey the token rule on every version, as the HTTP/1.1
// writer has always made them: HPACK and QPACK carry any byte, and a name
// with a space or a colon becomes a second field, or a malformed one, at the
// first hop that re-serialises the response toward HTTP/1.1.
func TestResponseFieldNamesMustBeTokens(t *testing.T) {
	for _, name := range []string{"x bad", "x:y", "x\x00", ""} {
		r := Response{Status: 200, Headers: []Header{{Name: []byte(name), Value: []byte("v")}}}
		if _, err := appendH3Response(nil, r, []byte("GET"), 0, &h3Scratch{}); !errors.Is(err, ErrH3Protocol) {
			t.Errorf("h3, name %q: got %v, want ErrH3Protocol", name, err)
		}
		s := newBenchH2Server(H2Config{})
		if _, err := s.appendHead(nil, 1, []byte("GET"), r, defaultMaxFrameSize); !errors.Is(err, ErrH2Protocol) {
			t.Errorf("h2, name %q: got %v, want ErrH2Protocol", name, err)
		}
	}
	ok := Response{Status: 200, Headers: []Header{{Name: []byte("x-ok"), Value: []byte("v")}}}
	if _, err := appendH3Response(nil, ok, []byte("GET"), 0, &h3Scratch{}); err != nil {
		t.Errorf("h3 refused a token name: %v", err)
	}
}

// The HTTP/2 writer's refusals hold on HTTP/3 too: a connection-specific
// field makes the response malformed (RFC 9114 §4.2), and a caller's
// content-length is a second opinion on framing this package states itself.
func TestH3RefusesConnectionSpecificResponseFields(t *testing.T) {
	for _, field := range []string{"connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade", "content-length"} {
		res := Response{Status: 200, Headers: []Header{{Name: []byte(field), Value: []byte("1")}}}
		if _, err := appendH3ResponseBasic(nil, res); !errors.Is(err, ErrH3Protocol) {
			t.Errorf("%s: got %v, want ErrH3Protocol", field, err)
		}
	}
}

// AppendQPACK marks credential-bearing fields never-indexed, as the HPACK
// writer does (RFC 9204 §4.5.6, §7.1.3), and the mark round-trips.
func TestQPACKMarksCredentialsNeverIndexed(t *testing.T) {
	for _, tt := range []struct {
		name string
		n    bool
	}{{"set-cookie", true}, {"authorization", true}, {"proxy-authorization", true}, {"x-other", false}} {
		block := appendQPACK(nil, []Header{{Name: []byte(tt.name), Value: []byte("v")}})
		if got := block[2]&0x10 != 0; got != tt.n {
			t.Errorf("%s: N bit = %v, want %v", tt.name, got, tt.n)
		}
		fields, err := newQPACKDecoder(nil, 0).Decode(nil, block)
		if err != nil || len(fields) != 1 || string(fields[0].Name) != tt.name {
			t.Errorf("%s: decoded %q, %v", tt.name, fields, err)
		}
	}
}

// A frame declaring more than the stream may carry is excessive load, not a
// malformed message, whatever its type: a reserved GREASE type the assembler
// would otherwise skip is refused with the same code as an oversized DATA.
func TestH3OversizedGREASEFrameIsExcessiveLoad(t *testing.T) {
	a := newH3Assembler(Config{MaxBodyBytes: 1 << 10})
	stream := quic.AppendVarint(nil, 0x21+0x1f) // a reserved frame type
	stream = quic.AppendVarint(stream, 1<<20)
	_, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, stream, false)})
	if err != nil {
		t.Fatalf("Frames failed the connection over one stream: %v", err)
	}
	if len(failed) != 1 || failed[0].Code != h3ExcessiveLoad {
		t.Fatalf("Frames reported %v, want one H3_EXCESSIVE_LOAD stream error", failed)
	}
}

// A static-table hit is a view of the table and the section's literals share
// one buffer, so a section costs one allocation however many literals it
// holds — it cost up to four per field, a string and a []byte each for name
// and value, static hits included.
func TestQPACKDecodeAllocations(t *testing.T) {
	d := newQPACKDecoder(nil, 0)
	dst := make([]Header, 0, 16)

	staticOnly := []byte{0x00, 0x00, 0xc0 | 17, 0xc0 | 23, 0xc0 | 1} // :method GET, :scheme https, :path /
	if n := testing.AllocsPerRun(100, func() {
		if _, err := d.Decode(dst[:0], staticOnly); err != nil {
			t.Fatal(err)
		}
	}); n != 0 {
		t.Errorf("a section of static hits allocated %.0f times, want 0", n)
	}

	section := append([]byte(nil), staticOnly...)
	// :authority by static name reference, its value Huffman-coded.
	section = append(section, 0x50, 0x80|byte(len(wwwExampleHuffman)))
	section = append(section, wwwExampleHuffman...)
	section = append(section, appendQPACK(nil, []Header{
		{Name: []byte("user-agent"), Value: []byte("bench-client/1.0")},
		{Name: []byte("accept"), Value: []byte("text/html,application/json")},
		{Name: []byte("x-empty"), Value: []byte{}},
	})[2:]...) // AppendQPACK's own two-byte prefix dropped
	fields, err := d.Decode(nil, section)
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) != 7 || string(fields[3].Value) != "www.example.com" || string(fields[5].Value) != "text/html,application/json" {
		t.Fatalf("decoded %q", fields)
	}
	if fields[6].Value == nil {
		t.Error("an empty literal decoded to nil, which reads as absent")
	}
	if n := testing.AllocsPerRun(100, func() {
		if _, err := d.Decode(dst[:0], section); err != nil {
			t.Fatal(err)
		}
	}); n != 1 {
		t.Errorf("a section of seven fields allocated %.0f times, want 1", n)
	}

	// The views are capped: an append through one cannot reach the next.
	_ = append(fields[4].Name, "XXXXXXXXXXXXXXXX"...)
	if string(fields[4].Value) != "bench-client/1.0" {
		t.Errorf("an append to a decoded name overwrote the value after it: %q", fields[4].Value)
	}
}

// A caller's static table is copied when the decoder is built: a decoded
// field does not alias bytes the caller still holds.
func TestQPACKCallerTableIsNotAliased(t *testing.T) {
	table := []Header{{Name: []byte("x-k"), Value: []byte("v1")}}
	d := newQPACKDecoder(table, 0)
	fields, err := d.Decode(nil, []byte{0x00, 0x00, 0xc0})
	if err != nil {
		t.Fatal(err)
	}
	table[0].Value[1] = '2'
	if string(fields[0].Value) != "v1" {
		t.Errorf("rewriting the caller's table changed a decoded field to %q", fields[0].Value)
	}
}
