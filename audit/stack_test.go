package audit

import (
	"strings"
	"testing"
)

func stackFindings(t *testing.T, spec Spec) map[string][]Finding {
	t.Helper()
	target, err := Open(spec)
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(Selection{Components: {"app"}, Sources: {"config", "logs"}}, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]Finding{}
	for _, f := range findings {
		out[f.Rule] = append(out[f.Rule], f)
	}
	return out
}

func mentions(fs []Finding, text string) bool {
	for _, f := range fs {
		if strings.Contains(f.Current, text) {
			return true
		}
	}
	return false
}

// TestStackDefaultsAreNamed: each stack's ceiling left at its default is
// found in the file that sets it, and nothing once each is set.
func TestStackDefaultsAreNamed(t *testing.T) {
	spring := seedFile(t, "application.properties", "server.port=8443\n# nothing tuned\n")
	uvicorn := seedFile(t, "start.txt", "STUDY_POOL=8\nuvicorn main:app --host 0.0.0.0 --port 8443\n")
	node := seedFile(t, "server.js", "const pool = new pg.Pool({ connectionString });\nconst server = http.createServer(app);\n")
	dotnet := seedFile(t, "appsettings.json", `{"ConnectionStrings": {"study": "Host=db;Username=study;Database=study"}}`)
	load := seedFile(t, "stage.jsonl", stageLine(100000, 10000, 0, 0))
	pg := seedFile(t, "s.csv", "name,setting,unit\nmax_connections,100,\n")
	got := stackFindings(t, Spec{StackConfig: strings.Join([]string{spring, uvicorn, node, dotnet}, ","), LoadLine: load, PGSettings: pg})
	for rule, want := range map[string][]string{
		"app/tomcat-threads":   {"threads.max left at 200", "max-connections left at 8192", "HikariCP's 10"},
		"app/uvicorn-defaults": {"--timeout-keep-alive left at 5 seconds", "access log on", "--workers left at 1"},
		"app/node-defaults":    {"keepAliveTimeout left at 5 seconds", "node-postgres's 10"},
		"app/npgsql-pool":      {"Npgsql's 100, in " + dotnet + ", against max_connections 100"},
	} {
		for _, w := range want {
			if !mentions(got[rule], w) {
				t.Errorf("%s: no %q in %+v", rule, w, got[rule])
			}
		}
	}

	spring = seedFile(t, "application.properties", "spring.threads.virtual.enabled=true\nserver.tomcat.max-connections=20000\n"+
		"spring.datasource.hikari.maximum-pool-size: 16\n")
	uvicorn = seedFile(t, "start.txt", "python -m uvicorn main:app --workers 4 --timeout-keep-alive=75 --no-access-log\n")
	node = seedFile(t, "server.js", "const pool = new pg.Pool({ max: 16 });\nserver.keepAliveTimeout = 75000;\n")
	dotnet = seedFile(t, "appsettings.json", `{"ConnectionStrings": {"study": "Host=db;Maximum Pool Size=16"}}`)
	if got := stackFindings(t, Spec{StackConfig: strings.Join([]string{spring, uvicorn, node, dotnet}, ","), LoadLine: load}); len(got) != 0 {
		t.Errorf("every ceiling set: %+v", got)
	}
}

// TestSpringRelaxedBinding: the environment's form of a property is the
// property.
func TestSpringRelaxedBinding(t *testing.T) {
	env := seedFile(t, "env.txt", "SPRING_THREADS_VIRTUAL_ENABLED=true\nSPRING_DATASOURCE_HIKARI_MAXIMUMPOOLSIZE=16\n")
	if got := stackFindings(t, Spec{StackConfig: env}); len(got) != 0 {
		t.Errorf("set through the environment: %+v", got)
	}
}

func TestStackFileRefused(t *testing.T) {
	for name, body := range map[string]string{
		"notes.txt":                   "nothing a stack reads\n",
		"logging.json":                `{"Logging": {}}`,
		"broken.json":                 "{",
		"appsettings.Production.json": `{"Kestrel": {"Limits": []}}`,
	} {
		if _, err := Open(Spec{StackConfig: seedFile(t, name, body)}); err == nil {
			t.Errorf("%s: read", name)
		}
	}
}
