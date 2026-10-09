//go:build !unix && !windows

package quic

// isRefusal has no ICMP refusal to recognise where the socket API has no
// such error; the handshake then counts read-side errors only.
func isRefusal(error) bool { return false }
