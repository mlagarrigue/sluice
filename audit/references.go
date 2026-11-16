package audit

import "slices"

// The published benchmarks a rule answers for. A reader held to a CIS
// Benchmark or to an ANSSI guide wants to know which of its
// recommendations the audit checks:
// every rule is listed here, with the items it covers, or with none — said
// once, so that a rule added without a look at the benchmarks fails
// TestEveryRuleIsClassified instead of passing for one that covers none.
//
// A rule is put on an item only when the item checks the same setting in
// the same direction. A neighbour does not count: net.ipv4.tcp_syncookies
// (CIS Ubuntu 3.3.1.18) is not the length of the SYN queue. Most of
// the performance and reliability rules have no item: the benchmarks are
// hardening guides. An item is named by its benchmark, version and number,
// never quoted: the number finds it in the document.

// A Reference is one recommendation of a published benchmark.
type Reference struct {
	Referential string `json:"referential"` // the family, a value of the referentials axis
	Benchmark   string `json:"benchmark"`   // the document and its version
	ID          string `json:"id"`          // the recommendation's number in it
}

// String is the reference as a report prints it.
func (r Reference) String() string { return r.Benchmark + " " + r.ID }

// The benchmarks cited, one version each: the one the study deploys
// (PostgreSQL 17) or the latest at the time of writing (Docker, the ANSSI
// guides: its GNU/Linux configuration guide and its Docker technical
// sheet, both under the Licence Ouverte).
const (
	cisDocker   = "CIS Docker Benchmark v1.9.0"
	cisPostgres = "CIS PostgreSQL 17 Benchmark v1.1.0"
	anssiLinux  = "ANSSI-BP-028 v2.0"
	anssiDocker = "ANSSI-FT-082 v1.0"
)

func cis(benchmark string, ids ...string) []Reference { return refs("cis", benchmark, ids) }

func anssi(guide string, ids ...string) []Reference { return refs("anssi", guide, ids) }

func refs(referential, benchmark string, ids []string) []Reference {
	out := make([]Reference, 0, len(ids))
	for _, id := range ids {
		out = append(out, Reference{Referential: referential, Benchmark: benchmark, ID: id})
	}
	return out
}

// references is every rule's, nil for a rule no benchmark item carries.
var references = map[string][]Reference{
	// The application and its stack: no benchmark of the audit's covers an
	// application server's settings.
	"app/max-requests-per-conn":    nil,
	"app/db-in-clear":              nil, // the client side; CIS PostgreSQL 6.8 is the server's
	"app/db-conns":                 nil,
	"app/tls-off":                  nil,
	"app/tls-resume":               nil,
	"app/out-of-descriptors":       nil,
	"app/access-log":               nil,
	"app/pool-exhausted":           nil,
	"app/statement-cancelled":      nil,
	"app/requests-per-connection":  nil,
	"app/tomcat-threads":           nil,
	"app/uvicorn-defaults":         nil,
	"app/node-defaults":            nil,
	"app/npgsql-pool":              nil,
	"app/gc-share":                 nil,
	"app/batch-size":               nil,
	"app/idle-timeout":             nil,
	"app/read-timeouts":            nil,
	"tls/cert-expiry":              nil,
	"tls/cert-key":                 nil,
	"tls/cert-chain":               nil,
	"tls/legacy-version":           nil,
	"tls/version-in-comparison":    nil,
	"tls/weak-cipher":              nil,
	"tls/resumption-off":           nil,
	"tls/resumption-in-comparison": nil,

	// A container as it was run (docker inspect): CIS Docker §5, the
	// runtime, and §4.1 for the user the image or --user sets.
	"container/nofile":            nil, // CIS 5.19 asks the opposite: override the default only when needed
	"container/cpuset":            nil,
	"container/memory":            slices.Concat(cis(cisDocker, "5.11"), anssi(anssiDocker, "R10")),
	"container/network":           nil, // comparative profile only; CIS 5.10 and ANSSI-FT-082 R4 ask the host's namespace not be shared
	"container/log-driver":        nil,
	"container/capabilities":      slices.Concat(cis(cisDocker, "5.4", "5.5"), anssi(anssiDocker, "R8", "R8-")),
	"container/no-new-privileges": cis(cisDocker, "5.26"),
	"container/seccomp":           cis(cisDocker, "5.22"),
	"container/apparmor":          slices.Concat(cis(cisDocker, "5.2"), anssi(anssiLinux, "R45")),
	"container/read-only":         slices.Concat(cis(cisDocker, "5.13"), anssi(anssiDocker, "R12")),
	"container/user":              cis(cisDocker, "4.1"),
	"container/docker-socket":     slices.Concat(cis(cisDocker, "5.32"), anssi(anssiDocker, "R15"), anssi(anssiLinux, "R52")),

	// The Dockerfile: CIS Docker §4, the images and their build.
	"docker/pinned-base":     nil, // CIS 4.2 asks for a trusted base, not a digest
	"docker/multi-stage":     nil,
	"docker/minimal-runtime": slices.Concat(cis(cisDocker, "4.3"), anssi(anssiLinux, "R58")),
	"docker/user":            cis(cisDocker, "4.1"),
	"docker/no-secret":       cis(cisDocker, "4.9"),
	"docker/dockerignore":    nil,

	// The image in its registry: CIS Docker 4.11 asks signatures be
	// validated, which an unsigned image cannot be.
	"docker/image-signature":  cis(cisDocker, "4.11"),
	"docker/image-provenance": nil,

	// Go: its build, its runtime, its modules.
	"go/godebug":        nil,
	"go/gomaxprocs":     nil,
	"deps/cve":          nil,
	"deps/gosum":        nil,
	"deps/license":      nil,
	"gobuild/trimpath":  nil,
	"gobuild/cgo":       nil,
	"gobuild/goamd64":   nil,
	"gobuild/pgo":       nil,
	"gobuild/toolchain": nil,
	"gobuild/vcs":       nil,

	// The host and its kernel: the CIS Ubuntu items on the network stack
	// are its hardening (redirects, source routing, syncookies), none of
	// them the queue and buffer sizes these rules read.
	"host/steal":                  nil,
	"host/smt":                    nil,
	"linux/conntrack-full":        nil,
	"linux/oom-kill":              nil,
	"linux/somaxconn":             nil,
	"linux/syn-backlog":           nil,
	"linux/netdev-backlog":        nil,
	"linux/udp-rmem":              nil,
	"linux/udp-wmem":              nil,
	"linux/conntrack":             nil,
	"linux/ephemeral-ports":       nil,
	"linux/swap":                  nil,
	"linux/transparent-hugepages": nil,
	"linux/cpu-governor":          nil,
	"linux/nofile":                nil,
	"linux/reconnections":         nil,
	"linux/listen-overflows":      nil,
	"linux/time-wait":             nil,
	"firewall/postgres-exposed":   slices.Concat(cis(cisPostgres, "5.2"), anssi(anssiLinux, "R80")),

	// PostgreSQL: its authentication (§5), its TLS (§6.8), its roles
	// (§4.3), its patches (§1.5).
	"postgres/connections-saturated": nil,
	"postgres/idle-in-transaction":   nil,
	"postgres/max-connections":       nil,
	"postgres/shared-buffers":        nil,
	"postgres/autovacuum":            nil,
	"postgres/password-encryption":   cis(cisPostgres, "5.4"),
	"postgres/ssl":                   cis(cisPostgres, "6.8"),
	"postgres/hba-trust":             cis(cisPostgres, "5.3", "5.4"),
	"postgres/hba-md5":               cis(cisPostgres, "5.4"),
	"postgres/checkpoints":           nil,
	"postgres/too-many-clients":      nil,
	"postgres/superusers":            cis(cisPostgres, "4.3"),
	"postgres/cve":                   slices.Concat(cis(cisPostgres, "1.5"), anssi(anssiLinux, "R61")),

	// The proxy: no CIS NGINX or HAProxy benchmark is read yet; an advisory
	// is the ANSSI's regular updates.
	"proxy/upstream-keepalive": nil,
	"proxy/buffering":          nil,
	"proxy/timeouts":           nil,
	"proxy/tls-protocols":      nil,
	"proxy/forwarded-for":      nil,
	"proxy/cve":                anssi(anssiLinux, "R61"),
}

// referentialsOf is the values of the referentials axis a rule's
// references put it under: their families, or "none".
func referentialsOf(refs []Reference) []string {
	var out []string
	for _, r := range refs {
		if !slices.Contains(out, r.Referential) {
			out = append(out, r.Referential)
		}
	}
	if len(out) == 0 {
		return []string{"none"}
	}
	return out
}
