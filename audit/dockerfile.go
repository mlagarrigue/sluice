package audit

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// What the Dockerfile says the image is. This reads the text, not the
// image: the text is the instruction, and an image read back is another
// source with its own rules. Six things are checked, and each of them is
// something a reader of the file could miss.

// A dockerStage is one FROM and the instructions under it.
type dockerStage struct {
	image string // what FROM named, as written
	as    string // its name under AS, empty when it has none
	lines []string
}

var (
	fromLine   = regexp.MustCompile(`(?i)^\s*FROM\s+(\S+)(?:\s+AS\s+(\S+))?\s*$`)
	userLine   = regexp.MustCompile(`(?i)^\s*USER\s+(\S+)`)
	secretName = regexp.MustCompile(`(?i)(password|passwd|secret|token|api[_-]?key|private[_-]?key|credential)`)
	envOrArg   = regexp.MustCompile(`(?i)^\s*(ENV|ARG)\s+(.*)$`)
)

// stages splits a Dockerfile into its stages, continuation lines joined so
// that an instruction broken over several lines is read as one.
func stages(text string) []dockerStage {
	var joined []string
	var current strings.Builder
	for line := range strings.SplitSeq(text, "\n") {
		trimmed := strings.TrimRight(line, " \t\r")
		if strings.HasSuffix(trimmed, "\\") {
			current.WriteString(strings.TrimSuffix(trimmed, "\\") + " ")
			continue
		}
		current.WriteString(trimmed)
		joined = append(joined, current.String())
		current.Reset()
	}
	if current.Len() > 0 {
		joined = append(joined, current.String())
	}

	var out []dockerStage
	for _, line := range joined {
		if m := fromLine.FindStringSubmatch(line); m != nil {
			out = append(out, dockerStage{image: m[1], as: m[2]})
			continue
		}
		if len(out) == 0 {
			continue // a directive or a comment before the first FROM
		}
		out[len(out)-1].lines = append(out[len(out)-1].lines, line)
	}
	return out
}

// final is the stage the image is: the last one, which is what docker
// build produces when no target is named.
func final(ss []dockerStage) dockerStage {
	if len(ss) == 0 {
		return dockerStage{}
	}
	return ss[len(ss)-1]
}

// init registers the Dockerfile rules, in this order: docker/pinned-base,
// docker/multi-stage, docker/minimal-runtime, docker/user, docker/no-secret,
// docker/dockerignore. Each is one Register call, read on its own.
func init() {
	needs := func(goal, source string) Selection {
		return Selection{
			Components: {"docker"},
			Goals:      {goal},
			Sources:    {source},
		}
	}
	read := func(t *Target) ([]dockerStage, bool) {
		if t.Dockerfile == "" {
			return nil, false
		}
		return stages(t.Dockerfile), true
	}

	Register(Rule{
		Name:  "docker/pinned-base",
		Needs: needs("security", "config"),
		Doc:   "https://docs.docker.com/build/building/best-practices/#pin-base-image-versions",
		Check: func(t *Target, _ Answers) []Finding {
			ss, ok := read(t)
			if !ok {
				return nil
			}
			var out []Finding
			names := map[string]bool{}
			for _, s := range ss {
				if s.as != "" {
					names[s.as] = true
				}
				if names[s.image] || strings.EqualFold(s.image, "scratch") {
					continue // an earlier stage of this file, or the empty image
				}
				if strings.Contains(s.image, "@sha256:") {
					continue
				}
				out = append(out, Finding{
					Current:  "FROM " + s.image,
					Proposed: "FROM " + s.image + "@sha256:… (the digest the tag points at today)",
					Why: "a tag moves: the image built tomorrow from this file is not the one built " +
						"today, so a build cannot be reproduced and a base that changed under the " +
						"build is invisible. The digest pins the bytes; a bot moves it",
				})
			}
			return out
		},
	})

	Register(Rule{
		Name:  "docker/multi-stage",
		Needs: needs("security", "config"),
		Doc:   "https://docs.docker.com/build/building/multi-stage/",
		Check: func(t *Target, _ Answers) []Finding {
			ss, ok := read(t)
			if !ok || len(ss) >= 2 {
				return nil
			}
			return []Finding{{
				Current:  "one stage: the image carries whatever built it",
				Proposed: "a build stage, then a runtime stage that copies only the artefact",
				Why: "the compiler, the module cache and the sources stay in the shipped image: " +
					"every one of them is attack surface and bytes to pull, for something the " +
					"running program never reads",
			}}
		},
	})

	Register(Rule{
		Name:  "docker/minimal-runtime",
		Needs: needs("security", "config"),
		Doc:   "https://github.com/GoogleContainerTools/distroless#why-should-i-use-distroless-images",
		Check: func(t *Target, _ Answers) []Finding {
			ss, ok := read(t)
			if !ok {
				return nil
			}
			base := final(ss).image
			for _, small := range []string{"scratch", "distroless"} {
				if strings.Contains(strings.ToLower(base), small) {
					return nil
				}
			}
			return []Finding{{
				Current:  "the runtime stage is " + base,
				Proposed: "scratch, or a distroless image, when the binary is static",
				Why: "a shell, a package manager and a libc in the image are what an attacker uses " +
					"after the first mistake; a static Go binary needs none of them. The trade is " +
					"that there is nothing to debug with inside the container",
			}}
		},
	})

	Register(Rule{
		Name:  "docker/user",
		Needs: needs("security", "config"),
		Doc:   "https://docs.docker.com/reference/dockerfile/#user",
		Check: func(t *Target, _ Answers) []Finding {
			ss, ok := read(t)
			if !ok {
				return nil
			}
			last := final(ss)
			user := ""
			for _, line := range last.lines {
				if m := userLine.FindStringSubmatch(line); m != nil {
					user = m[1]
				}
			}
			switch user {
			case "":
				return []Finding{{
					Current:  "no USER in the runtime stage, so root",
					Proposed: "USER nobody, or a numeric uid the filesystem allows",
					Why: "root in the container is uid 0 on the host: a container escape, a mounted " +
						"volume or a shared namespace then has the host's root, and nothing the " +
						"program does needs it",
				}}
			case "root", "0":
				return []Finding{{
					Current:  "USER " + user,
					Proposed: "a non-root user",
					Why:      "the same uid 0 as the host's root, set on purpose rather than by omission",
				}}
			}
			return nil
		},
	})

	Register(Rule{
		Name:  "docker/no-secret",
		Needs: needs("security", "config"),
		Doc:   "https://docs.docker.com/build/building/secrets/",
		Check: func(t *Target, _ Answers) []Finding {
			ss, ok := read(t)
			if !ok {
				return nil
			}
			var out []Finding
			for _, s := range ss {
				for _, line := range s.lines {
					m := envOrArg.FindStringSubmatch(line)
					if m == nil {
						continue
					}
					name, value, hasValue := strings.Cut(strings.TrimSpace(m[2]), "=")
					if !secretName.MatchString(name) || (!hasValue && strings.EqualFold(m[1], "ARG")) {
						continue
					}
					if hasValue && strings.TrimSpace(value) == "" {
						continue
					}
					out = append(out, Finding{
						Current:  strings.ToUpper(m[1]) + " " + name + " carries a value",
						Proposed: "pass it at run time, or with a build secret mount (--mount=type=secret)",
						Why: "an ENV or an ARG with a value is a layer of the image, and a layer is " +
							"readable by anyone who can pull it, forever — deleting it in a later " +
							"layer removes nothing",
					})
				}
			}
			return out
		},
	})

	Register(Rule{
		Name:  "docker/dockerignore",
		Needs: needs("reliability", "config"),
		Doc:   "https://docs.docker.com/build/concepts/context/#dockerignore-files",
		Check: func(t *Target, _ Answers) []Finding {
			if t.DockerfilePath == "" {
				return nil
			}
			// The context is where the build is run from, which this cannot
			// know; the repository root above the file is the common case,
			// and so is the file's own directory.
			dir := filepath.Dir(t.DockerfilePath)
			for _, candidate := range []string{dir, filepath.Join(dir, "..")} {
				if _, err := os.Stat(filepath.Join(candidate, ".dockerignore")); err == nil {
					return nil
				}
			}
			return []Finding{{
				Current:  "no .dockerignore beside the Dockerfile or above it",
				Proposed: "a .dockerignore listing .git, the build output and the local overrides",
				Why: "without it the whole context is sent to the daemon and lands in the build " +
					"cache: .git, editor files and any local secret, which slows every build and " +
					"can put a key in a layer through a COPY that was meant to be narrow",
			}}
		},
	})
}
