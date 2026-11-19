package httpstream

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
)

// ErrNoInterim reports that [Request.Interim] cannot send on this request:
// its batch has returned, the request did not come from a connection, or
// HTTP/1.1 has a response owed before it on the connection.
var ErrNoInterim = errors.New("httpstream: no interim response can be sent for this request")

// interimSender writes an interim response on the stream sid of one
// connection, before that stream's final response.
type interimSender interface {
	sendInterim(sid uint64, status int, fields []Header) error
}

// interimBatch is what the requests of one batch send their interim
// responses through. It closes when the batch returns: an interim response
// after the final one is a protocol error on every version (RFC 9110 §15.2
// puts them before it), and the final responses leave once the batch has
// returned.
type interimBatch struct {
	to   interimSender
	mu   sync.Mutex
	done bool
}

// close refuses every interim response from here on, waiting for one being
// written.
func (b *interimBatch) close() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.done = true
	b.mu.Unlock()
}

// stampInterim lets the requests of a batch send interim responses through
// to, each on its own stream, and returns what closes them once the batch
// has returned.
func stampInterim(reqs []Request, sids func(i int) uint64, to interimSender) *interimBatch {
	b := &interimBatch{to: to}
	for i := range reqs {
		reqs[i].interim, reqs[i].sid = b, sids(i)
	}
	return b
}

// Interim sends an interim response — 102 (Processing) or 103 (Early
// Hints), say — ahead of the request's final response, with its own fields:
// the Link fields of a 103, which let a client fetch what the final answer
// will need while the handler is still building it (RFC 8297). It is sent at
// once, from the handler's goroutine, and returns once it has left or failed.
//
// status is 102 to 199: a 100 (Continue) is the transport's, sent when the
// handler first reads a body a client asked to continue, and a 101 changes
// protocols, which this package does not do. The fields are held to the
// rules of a final response's, framing and connection fields refused.
//
// It returns [ErrNoInterim] once the request's batch has returned, since
// the final response may already be on its way; on HTTP/1.1 for every
// request of a batch but the first, whose responses wait for the ones
// before them; to an HTTP/1.0 client, which RFC 9110 §15.2 forbids; and for
// a request built by hand.
func (r Request) Interim(status int, fields []Header) error {
	if status < 102 || status > 199 {
		return fmt.Errorf("%w: an interim status is 102 to 199, not %d", ErrMalformed, status)
	}
	if r.interim == nil {
		return ErrNoInterim
	}
	lowered, err := interimFields(fields)
	if err != nil {
		return err
	}
	b := r.interim
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done {
		return ErrNoInterim
	}
	return b.to.sendInterim(r.sid, status, lowered)
}

// interimFields checks an interim response's fields as a final response's
// are checked, and returns them with lowercase names, which HTTP/2 and
// HTTP/3 require and HTTP/1.1 reads the same.
func interimFields(fields []Header) ([]Header, error) {
	out := make([]Header, 0, len(fields))
	for _, h := range fields {
		name := bytes.ToLower(h.Name)
		switch {
		case !isToken(name):
			return nil, fmt.Errorf("%w: interim field %q is not a token", ErrMalformed, h.Name)
		case hasCRLF(h.Value):
			return nil, ErrHeaderInjection
		case hasFieldControl(h.Value):
			return nil, fmt.Errorf("%w: the value of %q holds a control character", ErrMalformed, h.Name)
		case connectionSpecific(name), string(name) == "content-length", string(name) == "transfer-encoding", string(name) == "trailer":
			return nil, fmt.Errorf("%w: %s has no place in an interim response", ErrMalformed, h.Name)
		}
		if err := checkResponseField(name); err != nil {
			return nil, err
		}
		out = append(out, Header{Name: name, Value: h.Value})
	}
	return out, nil
}
