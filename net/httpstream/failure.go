package httpstream

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"runtime/debug"
	"syscall"
)

// panicError is a panic in the caller's code — the handler, a streamed
// response's producer, a Trailers function — turned into an error where it
// was recovered. Its message is what it always was; the value and the stack
// travel beside it for the log (reportFailure) and never into a response.
type panicError struct {
	what  string // "handler", "response producer", "response trailers"
	value any
	stack []byte
}

// recovered builds the error for a panic of what, with the stack taken here:
// called from the deferred recover, the panicking frames are still below.
func recovered(what string, value any) *panicError {
	return &panicError{what: what, value: value, stack: debug.Stack()}
}

func (e *panicError) Error() string {
	return fmt.Sprintf("httpstream: the %s panicked: %v", e.what, e.value)
}

// reportFailure logs, at the error level, what the caller's code did wrong
// that the client only sees as a 500 or a reset stream: a panic, with its
// value and stack, or a handler's answer of the wrong length. The server
// answers and carries on; without this line the operator would see the 500
// and nothing of its cause.
func reportFailure(log *slog.Logger, err error) {
	if !reportPanic(log, err) {
		log.Error(err.Error())
	}
}

// reportPanic logs err if it is a recovered panic, and reports whether it
// did. The sites that encode a response use it rather than reportFailure:
// their other errors are a field the server refuses or a peer's size limit,
// which the handler meets as a 500 in its own tests, or the peer caused.
func reportPanic(log *slog.Logger, err error) bool {
	p, ok := errors.AsType[*panicError](err)
	if ok {
		log.Error(p.Error(), "panic", p.value, "stack", string(p.stack))
	}
	return ok
}

// reportConnEnd logs, at the warn level, the error that ended a connection
// [Serve] or [Server] accepted, which nothing else returns to anyone: a
// peer that broke the protocol (a rapid reset, an HPACK or QPACK block that
// does not decode, a frame of the wrong size), or a transport that failed
// under the connection. The ordinary ends say nothing: the peer's close, a
// socket closed under the reader, a deadline, a reset, and the forced stop,
// which [Server] reports itself. ServeH2Conn and ServeH3Conn return the same
// error to their caller instead, and log nothing.
func reportConnEnd(log *slog.Logger, proto string, peer net.Addr, err error) {
	if err == nil || errors.Is(err, ErrShutdownForced) || errors.Is(err, io.EOF) ||
		errors.Is(err, net.ErrClosed) || errors.Is(err, os.ErrDeadlineExceeded) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return
	}
	log.Warn("httpstream: connection ended in error", "proto", proto, "peer", peer.String(), "err", err)
}
