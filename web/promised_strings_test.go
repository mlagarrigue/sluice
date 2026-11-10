package web_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice/web"
)

// ADR 0020 promises the strings a client compares: the JSON field names of
// a Report, and the message identifiers a client keys its translations on.
// New ones are added; existing ones do not change. The values of the Code*
// constants are guarded by scripts/check-api.sh — apidiff reports a
// constant whose value changed — but a struct tag is not part of a
// signature and a message identifier is a literal inside a function, so
// neither had a reader. The lists below are what was promised on
// 2026-11-10: a string may be added to the code without touching them (add
// it here too, so it is guarded from then on), and none may leave.

// promisedJSON is every JSON field name a Report carries, by Go type.
var promisedJSON = map[string][]string{
	"Report":  {"problems", "remedies"},
	"Problem": {"severity", "code", "messageId", "args", "path", "ruleId"},
	"Remedy":  {"action", "method", "target", "path", "inputs"},
	"Input":   {"name", "kind", "required", "readOnly", "pattern", "value"},
}

// promisedMessageIDs is every message identifier package web and its
// subpackages write into a diagnostic.
var promisedMessageIDs = []string{
	"CredentialsDuplicated", "CredentialsMalformed", "CredentialsMissing",
	"CredentialsNotBearer", "CredentialsRejected",
	"MethodNotAllowed", "MethodNotImplemented", "NoRoute",
	"ParameterInvalid", "ParameterMissing",
	"RequestBodyMalformed", "RequestBodyMediaType", "RequestBodyNull",
	"RequestBodyTimeout", "RequestBodyTooLarge", "RequestBodyTrailingContent",
	"RequestBodyTruncated",
	"RequestFailed", "RequestForbidden", "RequestTooEarly",
}

func TestPromisedStringsAreStable(t *testing.T) {
	t.Run("JSON field names", func(t *testing.T) {
		got := map[string]map[string]bool{}
		var walk func(reflect.Type)
		walk = func(ty reflect.Type) {
			for ty.Kind() == reflect.Slice || ty.Kind() == reflect.Pointer {
				ty = ty.Elem()
			}
			if ty.Kind() != reflect.Struct || got[ty.Name()] != nil {
				return
			}
			got[ty.Name()] = map[string]bool{}
			for f := range ty.Fields() {
				name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
				got[ty.Name()][name] = true
				walk(f.Type)
			}
		}
		walk(reflect.TypeFor[web.Report]())
		for ty, names := range promisedJSON {
			for _, n := range names {
				if !got[ty][n] {
					t.Errorf("%s no longer has a field named %q in JSON: ADR 0020 promises it to clients", ty, n)
				}
			}
		}
	})
	t.Run("message identifiers", func(t *testing.T) {
		var src strings.Builder
		err := filepath.WalkDir(".", func(p string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return err
			}
			b, err := os.ReadFile(p) //nolint:gosec // G304: a path WalkDir found under the package, not input
			src.Write(b)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range promisedMessageIDs {
			if !strings.Contains(src.String(), `"`+id+`"`) {
				t.Errorf("message identifier %q is no longer written by package web: ADR 0020 promises it to clients", id)
			}
		}
	})
}
