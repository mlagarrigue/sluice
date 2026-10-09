package web

import (
	"errors"
	"net/http"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/diagnostics"
)

// CodeInternal is the diagnostic code for a request that failed for a reason
// the client cannot act on.
const CodeInternal = "Transport.Request.Internal"

// Recover turns a panic in a handler into a response.
//
// # What net/http already does, and what it does not
//
// The standard server recovers panics itself: the process survives and the
// stack is logged. What it does not do is **answer** — it closes the
// connection, so the client sees a transport error rather than a status code.
// A caller retrying a network failure retries a request that will panic again,
// and a caller distinguishing 5xx from a broken pipe cannot.
//
// So this writes a 500 with a diagnostic body, and it does that only if the
// handler had not already started a response. Once bytes are on the wire the
// status is spent; overwriting it is impossible and pretending otherwise
// produces a body appended to a 200. In that case it panics with
// [http.ErrAbortHandler] after observing, so the server resets the connection
// rather than terminating the body cleanly: a short body ended normally is a
// truncated response the client would accept as complete.
//
//	mux.Handle("PATCH /orders/{order}", web.Recover(log.Println, handler))
//
// # It does not swallow the panic
//
// observe is called with every panic it catches, before the response is
// written. That is not optional decoration: a boundary that turns crashes into
// 500s and says nothing has converted a bug into a metric nobody reads. Pass
// something that logs with the stack.
//
// observe runs inside the deferred recovery, before the panicking frames are
// unwound, so a [runtime/debug.Stack] taken there still shows where the
// panic happened, not just this boundary.
//
// # http.ErrAbortHandler is re-raised
//
// It is net/http's own sentinel for "drop this connection without a message",
// used by [http.Handler] implementations that have decided the request is not
// worth answering. Catching it and replying 500 would answer a request the
// standard library was told to abandon, and would log a bug where there is
// none.
//
// # The body never carries the panic
//
// A panic value is internal state — a nil dereference names a field, a failed
// assertion quotes a value. It goes to observe and nowhere else. The client
// gets a code it can match on and nothing to learn from.
func Recover(observe func(any), next http.Handler) http.Handler {
	if next == nil {
		panic("web: Recover requires a handler")
	}
	if observe == nil {
		panic("web: Recover requires an observer; a boundary that catches panics silently converts bugs into a metric nobody reads")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tracked := &statusWriter{ResponseWriter: w}
		defer func() {
			v := recover()
			if v == nil {
				return
			}
			// net/http's own signal to abandon the connection in silence.
			// Re-raised so the server does what it was told, rather than
			// answering a request that was deliberately dropped.
			if err, ok := v.(error); ok && errors.Is(err, http.ErrAbortHandler) {
				panic(v)
			}

			observe(v)

			if tracked.wrote {
				// The response has begun. There is no status left to set and
				// no honest way to finish. Returning normally would let
				// net/http terminate the body cleanly (a final chunk, or a
				// short HTTP/2 stream with END_STREAM), and the client would
				// take a truncated response for a complete one. Aborting is
				// the truthful outcome: the connection (or stream) is reset.
				panic(http.ErrAbortHandler)
			}
			_ = WriteJSON(tracked, http.StatusInternalServerError, InternalReport())
		}()
		next.ServeHTTP(tracked, r)
	})
}

// TryHandler runs a handler under [sluice.Try], so the library's sentinel
// panics — an overflow under a Fail policy, an out-of-order stream — become
// errors a handler can answer rather than crashes.
//
// It is separate from [Recover] because the two catch different things and
// only one of them means a bug. A sentinel is the library telling a caller
// that data did something the configured policy refuses; a nil dereference is
// a defect. Wrapping both in one boundary would make them indistinguishable at
// exactly the moment the difference matters.
//
// Compose them, outermost first:
//
//	web.Recover(logPanic, web.TryHandler(logSentinel, answer, handler))
//
// observe is called with every sentinel caught, before anything is written —
// the same contract as [Recover]'s observer, and for the same reason: a
// sentinel raised after the response has begun cannot be answered, and a
// boundary that discarded it silently would turn a truncated response into a
// mystery nobody can diagnose. Pass something that logs.
//
// answer is called with the sentinel error and must write a response; it runs
// only when the response has not begun. When it has, TryHandler panics with
// [http.ErrAbortHandler] so the truncated response is aborted rather than
// ended cleanly; [Recover] re-raises that sentinel. Anything that is not a
// sentinel keeps unwinding, into [Recover] or out of the process.
func TryHandler(observe func(error), answer func(http.ResponseWriter, *http.Request, error), next http.Handler) http.Handler {
	if next == nil {
		panic("web: TryHandler requires a handler")
	}
	if observe == nil {
		panic("web: TryHandler requires an observer; a sentinel caught after the response began is otherwise dropped without a trace")
	}
	if answer == nil {
		panic("web: TryHandler requires a function to answer with")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tracked := &statusWriter{ResponseWriter: w}
		err := sluice.Try(func() { next.ServeHTTP(tracked, r) })
		if err == nil {
			return
		}
		// Observed before the wrote check, as in Recover: when the response
		// has begun this is the only trace the sentinel leaves.
		observe(err)
		if tracked.wrote {
			// Already answered: there is no status left to set, and a
			// normal return would end the body cleanly, passing a
			// truncated response off as complete. Abort instead.
			panic(http.ErrAbortHandler)
		}
		answer(tracked, r, err)
	})
}

// InternalReport is the body [Recover] writes, for a caller that wants to
// answer a sentinel the same way.
func InternalReport() Report {
	return ReportOf(diagnostics.NewDiagnostic(diagnostics.Critical, CodeInternal, diagnostics.Path{}).
		WithMessage("RequestFailed", nil).
		WithOrigin(diagnostics.OriginUnknown))
}

// statusWriter records whether a response has begun, which is the one fact a
// recovery boundary cannot work without.
//
// It deliberately implements almost nothing else. Wrapping a
// [http.ResponseWriter] costs the optional interfaces the real one carries —
// Flusher, Hijacker, ReadFrom — and a wrapper that forwards some of them
// silently drops the rest. [http.NewResponseController] is how a handler
// reaches those on Go 1.20 and later, and it works through this because the
// standard library unwraps it.
//
// FlushError is the one exception, because a flush is the one escape that
// commits the response without passing through WriteHeader or Write: the
// controller prefers FlushError on the wrapper over unwrapping, so
// implementing it is what keeps a handler that flushed and then panicked from
// reading as "nothing written yet" — which would append a 500 body to a 200
// already on the wire.
type statusWriter struct {
	http.ResponseWriter
	wrote bool
}

func (w *statusWriter) WriteHeader(status int) {
	if w.wrote {
		return
	}
	w.wrote = true
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	// An implicit 200: writing without a status is writing one.
	w.wrote = true
	return w.ResponseWriter.Write(b)
}

// FlushError flushes the underlying writer and records that the response has
// begun — a flush commits the status and headers exactly as a Write does. See
// the type documentation for why this one optional method is implemented.
//
// A flush the underlying writer does not support commits nothing, so it does
// not mark the response as begun: a handler that flushed into a non-flushable
// wrapper and then panicked must still get its 500, not an empty implicit 200.
// Any other failure is a real write that reached the wire and counts.
func (w *statusWriter) FlushError() error {
	err := http.NewResponseController(w.ResponseWriter).Flush()
	if err == nil || !errors.Is(err, http.ErrNotSupported) {
		w.wrote = true
	}
	return err
}

// Unwrap lets [http.NewResponseController] reach the underlying writer, so a
// handler that needs SetWriteDeadline and friends still can.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
