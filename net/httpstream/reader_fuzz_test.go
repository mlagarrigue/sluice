package httpstream

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// FuzzReaderChunkedAcrossReads: how the bytes are cut into reads is the
// network's choice, never the client's meaning. The same bytes delivered in
// one read and in reads of fuzzer-chosen sizes must yield the same requests
// — method, target, body — and end the same way. What it reaches that
// FuzzParseRequest's byte-at-a-time check does not: the reader's own state
// between reads, pipelined requests, and the chunk walk's saved offset
// surviving the buffer compaction under it (chunkProgress).
func FuzzReaderChunkedAcrossReads(f *testing.F) {
	chunked := "POST /u HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n"
	f.Add([]byte(chunked+"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n"), []byte{1, 7, 3})
	pipelined := []byte("GET /a HTTP/1.1\r\nHost: h\r\n\r\n" + chunked + "5\r\nhello\r\n6\r\n world\r\n0\r\n\r\nGET /c HTTP/1.1\r\nHost: h\r\n\r\n")
	f.Add(pipelined, []byte{30, 2, 9})
	// The first read ends inside the chunked body behind a whole request:
	// its walk is saved, then the whole request is consumed under it.
	f.Add(pipelined, []byte{93, 4})
	f.Add([]byte(chunked+"a;ext=1\r\n0123456789\r\n0\r\nTrailer: t\r\n\r\n"), []byte{1})
	f.Add([]byte("POST /l HTTP/1.1\r\nHost: h\r\nContent-Length: 4\r\nExpect: 100-continue\r\n\r\nbody"), []byte{50, 1})
	f.Add([]byte(chunked+"5\r\nhel"), []byte{60})                // cut short: the same refusal either way
	f.Add([]byte(chunked+"zz\r\nhello\r\n0\r\n\r\n"), []byte{2}) // a bad size line

	cfg := Config{BodyDelivery: BodyWhole, ReadTimeout: time.Minute}.mustResolve()
	f.Fuzz(func(t *testing.T, data, cuts []byte) {
		if len(data) > 8<<10 {
			return
		}
		whole, wholeErr := readThrough(&piecesConn{data: data}, cfg)
		cut, cutErr := readThrough(&piecesConn{data: data, cuts: cuts}, cfg)
		if len(whole) != len(cut) {
			t.Fatalf("%d requests in one read, %d across reads %v", len(whole), len(cut), cuts)
		}
		for i := range whole {
			if whole[i] != cut[i] {
				t.Fatalf("request %d: %q in one read, %q across reads %v", i, whole[i], cut[i], cuts)
			}
		}
		if (wholeErr == nil) != (cutErr == nil) || (wholeErr != nil && wholeErr.Error() != cutErr.Error()) {
			t.Fatalf("ended with %v in one read, %v across reads %v", wholeErr, cutErr, cuts)
		}
	})
}

// readThrough drains c's requests into "method target body" strings, since
// every field borrows the read buffer.
func readThrough(c net.Conn, cfg Config) ([]string, error) {
	var got []string
	src := newRequestReader(c, nil, cfg)
	src.Stream()(func(b sluice.Batch[Request]) bool {
		for i := range b.Items {
			r := &b.Items[i]
			got = append(got, string(r.Method)+" "+string(r.Target)+" "+string(r.Body))
		}
		return true
	})
	return got, src.Err()
}

// piecesConn delivers data in reads of the sizes cuts names, in turn and
// cycling, each at least one byte; with no cuts, all of it at once. Writes
// (an interim 100 Continue) are accepted and dropped.
type piecesConn struct {
	data []byte
	cuts []byte
	k    int
}

func (p *piecesConn) Read(b []byte) (int, error) {
	if len(p.data) == 0 {
		return 0, io.EOF
	}
	n := len(p.data)
	if len(p.cuts) > 0 {
		n = max(int(p.cuts[p.k%len(p.cuts)]), 1)
		p.k++
	}
	n = copy(b, p.data[:min(n, len(p.data))])
	p.data = p.data[n:]
	return n, nil
}

func (p *piecesConn) Write(b []byte) (int, error)      { return len(b), nil }
func (p *piecesConn) Close() error                     { return nil }
func (p *piecesConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (p *piecesConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (p *piecesConn) SetDeadline(time.Time) error      { return nil }
func (p *piecesConn) SetReadDeadline(time.Time) error  { return nil }
func (p *piecesConn) SetWriteDeadline(time.Time) error { return nil }
