package audit

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// dockerSelection asks for everything a Dockerfile rule needs.
var dockerSelection = Selection{Components: {"docker"}, Sources: {"config"}}

// firedOn returns the rules that spoke about a Dockerfile written here.
func firedOn(t *testing.T, name, body string, beside ...string) []string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, f := range beside {
		if err := os.WriteFile(filepath.Join(dir, f), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	target, err := Open(Spec{Dockerfile: path})
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(dockerSelection, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, f := range findings {
		if f.Current == "" || f.Proposed == "" || f.Why == "" || f.Doc == "" || f.Goal == "" {
			t.Errorf("a finding of %s is missing one of its five fields: %+v", f.Rule, f)
		}
		out = append(out, f.Rule)
	}
	return out
}

// TestASeededDockerfileIsReadForEveryFault: one file carrying all of them
// at once, because that is how they arrive — and because a rule that only
// fires alone is a rule that will miss the real file.
func TestASeededDockerfileIsReadForEveryFault(t *testing.T) {
	seeded := `# A single stage, a moving base, root, and a secret in a layer.
FROM golang:latest
WORKDIR /src
COPY . .
ENV API_TOKEN=s3cret
ARG DB_PASSWORD=hunter2
RUN go build -o /app ./cmd/app
ENTRYPOINT ["/app"]
`
	fired := firedOn(t, "Dockerfile", seeded)
	for _, want := range []string{
		"docker/pinned-base", "docker/multi-stage", "docker/minimal-runtime",
		"docker/user", "docker/no-secret", "docker/dockerignore",
	} {
		if !slices.Contains(fired, want) {
			t.Errorf("%s stayed silent on the seeded file: %v", want, fired)
		}
	}
	// Two secrets, two findings: a rule that stops at the first leaves the
	// second in the image.
	if n := strings.Count(strings.Join(fired, " "), "docker/no-secret"); n != 2 {
		t.Errorf("%d secret findings, want one per line that carries one", n)
	}
}

// TestACleanDockerfileIsSilent: the same rules on a file that does
// everything they ask. Silence here is what proves they read the file
// rather than fire on anything.
func TestACleanDockerfileIsSilent(t *testing.T) {
	clean := `FROM golang:1.26-bookworm@sha256:1111111111111111111111111111111111111111111111111111111111111111 AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /app ./cmd/app

FROM gcr.io/distroless/static-debian12@sha256:2222222222222222222222222222222222222222222222222222222222222222
COPY --from=build /app /app
USER nonroot
ENTRYPOINT ["/app"]
`
	if fired := firedOn(t, "Dockerfile", clean, ".dockerignore"); len(fired) != 0 {
		t.Errorf("a file that does everything asked was reported: %v", fired)
	}
}

// TestTheRepositorysOwnDockerfileHoldsTheThreeItClaims: interop/Dockerfile
// is multi-stage and pins its bases; those three rules must be silent on
// it. The ones it does trip are its own choices, and this test does not
// hide them — it names what it checks and nothing else.
func TestTheRepositorysOwnDockerfileHoldsTheThreeItClaims(t *testing.T) {
	path := filepath.Join("..", "interop", "Dockerfile")
	if _, err := os.Stat(path); err != nil {
		t.Skipf("%s is not here: %v", path, err)
	}
	target, err := Open(Spec{Dockerfile: path})
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(dockerSelection, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	var fired []string
	for _, f := range findings {
		fired = append(fired, f.Rule)
	}
	for _, quiet := range []string{"docker/multi-stage", "docker/no-secret", "docker/dockerignore"} {
		if slices.Contains(fired, quiet) {
			t.Errorf("%s fired on %s: %v", quiet, path, findings)
		}
	}
}

// TestAContinuationLineIsOneInstruction: a RUN or an ENV broken over
// several lines is read whole, or a secret hides behind a backslash.
func TestAContinuationLineIsOneInstruction(t *testing.T) {
	body := `FROM scratch@sha256:3333333333333333333333333333333333333333333333333333333333333333
USER 65532
ENV API_TOKEN=\
	s3cret
`
	fired := firedOn(t, "Dockerfile", body, ".dockerignore")
	if !slices.Contains(fired, "docker/no-secret") {
		t.Errorf("a secret split over two lines went unseen: %v", fired)
	}
}

// TestAStageNamedByAnEarlierOneIsNotABase: "FROM build" copies from a
// stage of this file, and asking it for a digest would be nonsense.
func TestAStageNamedByAnEarlierOneIsNotABase(t *testing.T) {
	body := `FROM golang:1.26@sha256:4444444444444444444444444444444444444444444444444444444444444444 AS build
RUN true

FROM build AS test
RUN true

FROM scratch
COPY --from=build /app /app
USER 65532
`
	fired := firedOn(t, "Dockerfile", body, ".dockerignore")
	if slices.Contains(fired, "docker/pinned-base") {
		t.Errorf("a stage of the file was asked for a digest: %v", fired)
	}
}

// TestSBOMListsWhatTheBinaryLinks: the bill of materials is a reading of
// the binary, so it carries the main module, the toolchain, and no
// recommendation.
func TestSBOMListsWhatTheBinaryLinks(t *testing.T) {
	dir := t.TempDir()
	for name, body := range map[string]string{
		"go.mod":  "module probe\n\ngo 1.26.0\n",
		"main.go": "package main\n\nfunc main() {}\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	bin := filepath.Join(dir, "probe")
	build(t, dir, bin)

	target, err := Open(Spec{Binary: bin})
	if err != nil {
		t.Fatal(err)
	}
	modules := SBOM(target)
	if len(modules) < 2 {
		t.Fatalf("the bill of materials holds %d lines: %+v", len(modules), modules)
	}
	if modules[0].Path != "probe" || !modules[0].Main {
		t.Errorf("the first line is not the main module: %+v", modules[0])
	}
	if modules[1].Path != "stdlib" || !strings.HasPrefix(modules[1].Version, "go1.") {
		t.Errorf("the toolchain is not in the bill of materials: %+v", modules[1])
	}
	if SBOM(&Target{}) != nil {
		t.Error("a target with no binary produced a bill of materials")
	}
}
