//go:build !unix && !windows

package quic

import "errors"

// platformRefusal stands for a refusal on a platform where isRefusal
// recognises none; the tests that need one skip.
var platformRefusal = errors.New("no refusal on this platform")
