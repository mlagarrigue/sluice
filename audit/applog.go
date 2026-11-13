package audit

import (
	"fmt"
	"strings"
)

// What the application said about itself while it ran. A server that runs
// out of file descriptors cannot accept a connection, and the only trace it
// leaves is its own log: the kernel counts nothing, and the client sees a
// connection that hangs until it times out. Every stack prints the errno one
// way or the other — its text, "too many open files", or its name, EMFILE —
// so the lines are found by those, not by a format of one stack's.

// errnoLines returns the lines of the application's log that name an errno
// by its text (any case) or by its name, but for those holding unless.
func errnoLines(t *Target, text, name, unless string) []string {
	var out []string
	for line := range strings.SplitSeq(t.AppLog, "\n") {
		lower := strings.ToLower(line)
		if unless != "" && strings.Contains(lower, unless) {
			continue
		}
		if strings.Contains(lower, text) || strings.Contains(line, name) {
			out = append(out, line)
		}
	}
	return out
}

// init registers app/out-of-descriptors.
func init() {
	Register(Rule{
		Name:  "app/out-of-descriptors",
		Needs: Selection{Components: {"app"}, Goals: {"reliability"}, Sources: {"logs"}},
		Doc:   "https://man7.org/linux/man-pages/man2/accept.2.html",
		Check: func(t *Target, _ Answers) []Finding {
			var out []Finding
			// ENFILE first: its text contains EMFILE's, and its cause is the
			// system's table, not the process's limit.
			if lines := errnoLines(t, "too many open files in system", "ENFILE", ""); len(lines) > 0 {
				out = append(out, Finding{
					Current:  fmt.Sprintf("%d line(s) naming ENFILE (\"too many open files in system\") in %s, the first: %s", len(lines), t.AppLogPath, cite(lines[0])),
					Proposed: "fs.file-max raised (sysctl), the open files of the whole system",
					Why: "the system's table of open files is full: every process fails to open " +
						"one, the server's accept first among them, and a connection waits in " +
						"the listen queue until its client gives up",
				})
			}
			if lines := errnoLines(t, "too many open files", "EMFILE", "too many open files in system"); len(lines) > 0 {
				out = append(out, Finding{
					Current:  fmt.Sprintf("%d line(s) naming EMFILE (\"too many open files\") in %s, the first: %s", len(lines), t.AppLogPath, cite(lines[0])),
					Proposed: "the server's nofile limit raised above the connections it is to hold, see linux/nofile and container/nofile",
					Why: "a connection is a file: at the process's nofile limit, accept fails " +
						"with EMFILE, the server backs off and retries, and the connections " +
						"waiting meanwhile see a latency of seconds, or a timeout, with no " +
						"error of their own",
				})
			}
			return out
		},
	})
}

// cite keeps a quoted log line to a length a report can carry.
func cite(line string) string {
	line = strings.TrimSpace(line)
	if len(line) > 200 {
		line = line[:200] + "…"
	}
	return fmt.Sprintf("%q", line)
}
