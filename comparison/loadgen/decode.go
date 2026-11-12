package main

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"
)

// The content codings the generator asks for (§2.3 bis). It asks for one
// at most, never a list: what comes back compressed is then in that coding
// or in clear.
const (
	codingGzip    = "gzip"
	codingDeflate = "deflate"
	codingBrotli  = "br"   // RFC 7932
	codingZstd    = "zstd" // RFC 8878
)

// maxDecoded bounds a decoded document: a list page is ~50 KB, and a
// zstd frame may announce a window or a size the generator has no reason
// to allocate.
const maxDecoded = 64 << 20

// decoded is a body as the check reads it, and what it cost on the wire.
type decoded struct {
	doc        []byte
	compressed bool // the body came in the coding asked for
	raw        bool // a deflate body without its zlib wrapper
}

// decode returns the document under the body's Content-Encoding, read to
// its end: a gzip member's trailer, a zlib stream's checksum, a brotli
// stream's last meta-block or a zstd frame's end is where a compressed body
// is complete, whether or not a Content-Length announced it; past
// maxDecoded bytes it is refused. A body in clear is always accepted (§2.3 bis: a server may decline
// to compress, and that is counted, not a fault); a coding the client did
// not ask for never is, since a client could not read it.
//
// HTTP's "deflate" is a deflate stream inside the zlib format (RFC 9110
// §8.4.1.2). Some servers send the raw stream, which the RFC notes and
// browsers accept: it is decoded too, and counted apart.
func decode(asked, coding string, body []byte) (decoded, error) {
	switch {
	case coding == "" || strings.EqualFold(coding, "identity"):
		return decoded{doc: body}, nil
	case asked == "" || !strings.EqualFold(coding, asked):
		return decoded{}, fmt.Errorf("Content-Encoding %q, not asked for", coding)
	}
	var (
		r   io.Reader
		raw bool
	)
	switch asked {
	case codingGzip:
		zr, _ := gzipReaders.Get().(*gzip.Reader)
		var err error
		if zr == nil {
			zr, err = gzip.NewReader(bytes.NewReader(body))
		} else {
			err = zr.Reset(bytes.NewReader(body))
		}
		if zr != nil {
			defer gzipReaders.Put(zr)
		}
		if err != nil {
			return decoded{}, fmt.Errorf("gzip: %w", err)
		}
		r = zr
	case codingDeflate:
		zr, err := zlibReader(body)
		switch {
		case errors.Is(err, zlib.ErrHeader):
			raw = true
			r = flate.NewReader(bytes.NewReader(body))
		case err != nil:
			return decoded{}, fmt.Errorf("deflate: %w", err)
		default:
			defer zlibReaders.Put(zr)
			r = zr
		}
	case codingBrotli:
		br, _ := brotliReaders.Get().(*brotli.Reader)
		if br == nil {
			br = brotli.NewReader(bytes.NewReader(body))
		} else {
			_ = br.Reset(bytes.NewReader(body)) // always nil
		}
		defer brotliReaders.Put(br)
		r = br
	case codingZstd:
		// DecodeAll reads whole frames: a frame cut short is an error, and
		// a checksum the frame carries is verified.
		zd, err := zstdDecoder()
		if err != nil {
			return decoded{}, fmt.Errorf("zstd: %w", err)
		}
		doc, err := zd.DecodeAll(body, make([]byte, 0, 4*len(body)))
		if err != nil {
			return decoded{}, fmt.Errorf("zstd: %w", err)
		}
		return decoded{doc: doc, compressed: true}, nil
	}
	var buf bytes.Buffer
	buf.Grow(4 * len(body))
	if _, err := buf.ReadFrom(io.LimitReader(r, maxDecoded+1)); err != nil {
		return decoded{}, fmt.Errorf("%s: %w", asked, err)
	}
	if buf.Len() > maxDecoded {
		return decoded{}, fmt.Errorf("%s: a document past %d bytes", asked, maxDecoded)
	}
	return decoded{doc: buf.Bytes(), compressed: true, raw: raw}, nil
}

// The decoders are reused: their tables are the cost, and the generator
// decodes every answer of a stage (§2.3 bis: it must not become the
// bottleneck).
var gzipReaders, zlibReaders, brotliReaders sync.Pool

// zstdDecoder is one decoder for the process: DecodeAll is safe from every
// goroutine at once, and its block decoders, one per GOMAXPROCS, are the
// tables reused.
var zstdDecoder = sync.OnceValues(func() (*zstd.Decoder, error) {
	return zstd.NewReader(nil, zstd.WithDecoderConcurrency(0), zstd.WithDecoderMaxMemory(maxDecoded))
})

// zlibResetter is what zlib.NewReader returns.
type zlibResetter interface {
	io.ReadCloser
	zlib.Resetter
}

func zlibReader(body []byte) (io.ReadCloser, error) {
	if zr, ok := zlibReaders.Get().(zlibResetter); ok {
		if err := zr.Reset(bytes.NewReader(body), nil); err != nil {
			zlibReaders.Put(zr)
			return nil, err
		}
		return zr, nil
	}
	return zlib.NewReader(bytes.NewReader(body))
}
