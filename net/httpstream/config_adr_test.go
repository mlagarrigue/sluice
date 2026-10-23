package httpstream

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// ADR 0018 names, in its decisions 2 and 4, the Config fields whose zero
// means "no bound" and those a single protocol reads. Both lists drifted
// from the code once — MaxConnsPerIP was left out of the first,
// LingerTimeout out of the second — so each is read here from the ADR and
// held to what the fields' own documentation says.
func TestADR0018ListsMatchConfig(t *testing.T) {
	fields := configFieldDocs(t)
	adr, err := os.ReadFile("../../docs/design/adr/0018-zero-values-and-mandatory-bounds.md")
	if err != nil {
		t.Fatal(err)
	}
	text := strings.Join(strings.Fields(string(adr)), " ")

	// Decision 2: the fields whose zero means no bound.
	two := between(t, text, "2. **The bound is mandatory.**", "3. **")
	var unbounded []string
	for name, doc := range fields {
		if strings.Contains(doc, "Zero means no bound") {
			unbounded = append(unbounded, name)
		}
	}
	same(t, "zero means no bound", namesIn(two, fields), unbounded)

	// Decision 4: one list per label, each from its quoted label to the next
	// semicolon or closing parenthesis.
	four := between(t, text, "4. **A field says which protocols read it.**", "5. **")
	labels := []string{"HTTP/1.1 only", "HTTP/2 only", "HTTP/3 only", "HTTP/2 and HTTP/3 only", "HTTP/1.1 and HTTP/2 only"}
	for _, label := range labels {
		listed := regexp.MustCompile(regexp.QuoteMeta(`"`+label+`":`) + `([^;)]*)`).FindStringSubmatch(four)
		if listed == nil {
			t.Errorf("ADR 0018 decision 4 has no %q list", label)
			continue
		}
		var documented []string
		for name, doc := range fields {
			// A field belongs to the longest label its doc holds: "HTTP/2
			// only" is inside "HTTP/1.1 and HTTP/2 only".
			owner := ""
			for _, l := range labels {
				if strings.Contains(doc, l) && len(l) > len(owner) {
					owner = l
				}
			}
			if owner == label {
				documented = append(documented, name)
			}
		}
		same(t, label, namesIn(listed[1], fields), documented)
	}
}

// configFieldDocs reads Config's fields and their documentation, whitespace
// collapsed, from this package's source.
func configFieldDocs(t *testing.T) map[string]string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), "httpstream.go", nil, parser.ParseComments)
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
	if len(docs) == 0 {
		t.Fatal("no Config fields found in httpstream.go")
	}
	return docs
}

// between returns the text from start to the next end after it.
func between(t *testing.T, text, start, end string) string {
	t.Helper()
	i := strings.Index(text, start)
	if i < 0 {
		t.Fatalf("ADR 0018 has no %q", start)
	}
	rest := text[i+len(start):]
	if j := strings.Index(rest, end); j >= 0 {
		rest = rest[:j]
	}
	return rest
}

// namesIn returns the backquoted Config field names in s.
func namesIn(s string, fields map[string]string) []string {
	var names []string
	for _, m := range regexp.MustCompile("`([A-Za-z]+)`").FindAllStringSubmatch(s, -1) {
		if _, ok := fields[m[1]]; ok && !slices.Contains(names, m[1]) {
			names = append(names, m[1])
		}
	}
	return names
}

func same(t *testing.T, what string, adr, code []string) {
	t.Helper()
	slices.Sort(adr)
	slices.Sort(code)
	if !slices.Equal(adr, code) {
		t.Errorf("%s: ADR 0018 lists %v, the fields' documentation says %v", what, adr, code)
	}
}
