package httpstream

import (
	"sync/atomic"
	"time"
)

// dateLayout is RFC 9110 §5.6.7's IMF-fixdate, the one form a sender is
// required to generate. It is net/http's TimeFormat, spelled here so this
// package keeps its import graph.
const dateLayout = "Mon, 02 Jan 2006 15:04:05 GMT"

// cachedDate is one second's Date value, built once and read by every
// response of that second. The value is never written after the pointer is
// published, so a reader may hold the slice for as long as it likes.
type cachedDate struct {
	unix  int64
	value []byte
}

var currentDate atomic.Pointer[cachedDate]

// dateFor returns the Date field value for now — RFC 9110 §6.6.1's
// origin-server clock — formatted at most once per second. There is no
// goroutine refreshing it and no lock on the way: a caller reads the
// pointer, compares the second, and only the first caller of a new second
// formats. Two callers racing into a new second both format and both
// store; the loser's value is the same 29 bytes, so nothing is lost.
//
// The clock is the caller's to read, once per batch: on this machine
// time.Now is 35 ns, a fifth of a whole HTTP/2 head, and a batch of
// responses shares one second as surely as it shares one write.
func dateFor(now time.Time) []byte {
	unix := now.Unix()
	d := currentDate.Load()
	if d == nil || d.unix != unix {
		d = &cachedDate{unix: unix, value: now.UTC().AppendFormat(make([]byte, 0, len(dateLayout)), dateLayout)}
		currentDate.Store(d)
	}
	return d.value
}
