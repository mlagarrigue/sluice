package postgres

import (
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// Every default Config's comments name is the value the zero Config
// resolves to. The defaults are chosen by review, and a review reads the
// comments: a default changed in the code alone — the cancellation wait or
// the message ceiling moved by one — left every other test green, so the
// value deployed was not the value documented (architecture S3, ADR 0018).
// The figure is read from each field's comment, the value from the code
// that applies it.
func TestZeroConfigResolvesToTheDefaultsItsCommentsName(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "auth.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	docs := map[string]string{}
	ast.Inspect(f, func(n ast.Node) bool {
		ts, ok := n.(*ast.TypeSpec)
		if !ok || ts.Name.Name != "Config" {
			return true
		}
		for _, field := range ts.Type.(*ast.StructType).Fields.List {
			for _, name := range field.Names {
				docs[name.Name] = strings.Join(strings.Fields(field.Doc.Text()), " ")
			}
		}
		return false
	})

	server, client := net.Pipe()
	defer func() { _ = server.Close(); _ = client.Close() }()
	c := NewConn(client, Config{})
	bounds := Config{}.scramBounds()

	number := func(s string) int64 {
		n, err := strconv.ParseInt(strings.ReplaceAll(s, ",", ""), 10, 64)
		if err != nil {
			t.Fatalf("%q is not a number: %v", s, err)
		}
		return n
	}
	for _, tc := range []struct {
		field  string
		phrase string // the comment's wording, the figure in its first group
		unit   int64
		got    int64
	}{
		{"MinSCRAMIterations", `Zero means the default: ([\d,]+) below`, 1, int64(bounds.minIter)},
		{"MinSCRAMIterations", `and ([\d,]+) above`, 1, int64(bounds.maxIter)},
		{"MinSCRAMSaltLen", `Zero means (\d+):`, 1, int64(bounds.minSalt)},
		{"MaxMessage", `Zero, the default, is (\d+) MiB`, 1 << 20, pgwire.DefaultMaxMessage},
		{"StatementCacheSize", `Zero, the default, is (\d+) statements`, 1, int64(c.stmts.max)},
		{"CancelTimeout", `Zero, the default, is (\d+) seconds`, int64(time.Second), int64(c.cancelBound())},
	} {
		m := regexp.MustCompile(tc.phrase).FindStringSubmatch(docs[tc.field])
		if m == nil {
			t.Errorf("Config.%s: its comment no longer says %q; the default was not read", tc.field, tc.phrase)
			continue
		}
		if want := number(m[1]) * tc.unit; tc.got != want {
			t.Errorf("Config.%s: the zero Config resolves to %d, its comment says %s", tc.field, tc.got, m[0])
		}
	}
	if !strings.Contains(docs["MaxProtocol"], "Zero, the default, asks for [Protocol32]") {
		t.Error("Config.MaxProtocol: its comment no longer names Protocol32 as the default")
	} else if got := (Config{}).maxProtocol(); got != Protocol32 {
		t.Errorf("Config.MaxProtocol: the zero Config asks for %v, its comment says Protocol32", got)
	}
}
