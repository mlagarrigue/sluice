package httpstream

import (
	"bufio"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/net/quic"
)

// wholeButRaised is a configuration Config.resolve accepts: bodies whole,
// and MaxStreamedBodyBytes — "read when BodyDelivery streams" — above
// MaxBodyBytes. Under it a body is held to MaxBodyBytes alone, so a
// declared length past that is refused with 413 on its declaration, before
// the 100 Continue the client asked for: refused only once the body crossed
// MaxBodyBytes, the client paid MaxBodyBytes of sending, and an invitation
// to send, for a refusal its declaration announced.
var wholeButRaised = Config{MaxBodyBytes: 1 << 10, MaxStreamedBodyBytes: 1 << 20, BodyDelivery: BodyWhole, ReadTimeout: 2 * time.Second}

func TestADeclaredLengthIsHeldToTheWholeBodyBound(t *testing.T) {
	t.Run("HTTP/1.1", func(t *testing.T) {
		addr, _ := serveTest(t, wholeButRaised, echoTarget)
		c := dial(t, addr)
		br := bufio.NewReader(c)
		fmt.Fprint(c, "POST /u HTTP/1.1\r\nHost: h\r\nContent-Length: 4096\r\nExpect: 100-continue\r\n\r\n")
		if status := readStatusLine(t, br); !strings.HasPrefix(status, "HTTP/1.1 413") {
			t.Fatalf("a content-length of 4096 against MaxBodyBytes 1024 under BodyWhole was answered %q first, want 413", status)
		}
	})
	t.Run("HTTP/2", func(t *testing.T) {
		addr, _ := serveTest(t, wholeButRaised, echoTarget)
		c := dialH2(t, addr)
		c.frame(h2Frame{
			Type: frameHeaders, Flags: flagEndHeaders, StreamID: 1,
			Payload: headBlock("POST", "/u", [2]string{"content-length", "4096"}, [2]string{"expect", "100-continue"}),
		})
		c.flush()
		c.pump(func() bool { _, seen := c.heads[1]; return seen || c.sawGoAway })
		head, seen := c.heads[1]
		if !seen {
			t.Fatal("no status arrived on the declaration")
		}
		if status := h2Status(head); status != 413 {
			t.Fatalf("a content-length of 4096 against MaxBodyBytes 1024 under BodyWhole was answered :status %d first, want 413", status)
		}
	})
	t.Run("HTTP/3", func(t *testing.T) {
		a := testAssembler(wholeButRaised)
		head := h3Request("POST", "/u", [2]string{"content-length", "4096"}, [2]string{"expect", "100-continue"})
		_, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, head, false)})
		if err != nil {
			t.Fatalf("Frames: %v", err)
		}
		if len(failed) != 1 || failed[0].Status != 413 {
			t.Fatalf("a content-length of 4096 against MaxBodyBytes 1024 under BodyWhole gave failed=%v, want one 413 on its declaration", failed)
		}
	})
}
