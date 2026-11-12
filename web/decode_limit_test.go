package web_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice/web"
)

// DecodeJSON keeps "too large" apart from "malformed" (they need different
// fixes). A body whose JSON value fits the limit and whose length does not
// — the value, then blanks past the limit — is over the limit and nothing
// else: no second value follows it. The read that looks for a second value
// is the one that meets the limit, and its error was only compared with
// io.EOF: the body was refused as trailing content, where web/stream,
// which knows the length first, answers 413. The controls: the value alone
// decodes, a value past the limit is too large.
func TestDecodeJSONCallsBlanksPastTheLimitTooLarge(t *testing.T) {
	const limit = 64
	decode := func(body string) (code, messageID string, ok bool) {
		r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		var dst struct {
			Qty int `json:"qty"`
		}
		d, ok := web.DecodeJSON(r, limit, &dst)
		return d.Code, d.MessageID, ok
	}

	// Controls: the value alone decodes, and a value past the limit is too
	// large.
	if _, _, ok := decode(`{"qty":1}`); !ok {
		t.Fatal("control: the value alone was refused")
	}
	if code, _, ok := decode(`{"qty":` + strings.Repeat("1", 2*limit) + `}`); ok || code != web.CodeBodyTooLarge {
		t.Fatalf("control: a value past the limit is %q, want %q", code, web.CodeBodyTooLarge)
	}

	code, messageID, ok := decode(`{"qty":1}` + strings.Repeat(" ", 16*limit))
	if ok {
		t.Fatal("a body past the limit decoded")
	}
	if code != web.CodeBodyTooLarge {
		t.Errorf("a body past the limit by blanks alone is refused as %s / %s, want %s: nothing follows the value but the limit", code, messageID, web.CodeBodyTooLarge)
	}
}
