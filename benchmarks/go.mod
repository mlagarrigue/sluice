// The comparative benchmarks live in their own module, on purpose.
//
// The root module has exactly one production dependency, golang.org/x/crypto,
// and CI enforces the allowlist two ways: go.mod requiring nothing else, and
// `go list -deps ./...` finding no other non-stdlib module in the graph.
// Measuring against pgx, gin, echo, chi or fiber means importing
// them, so the honest way to do it is a second module that depends on the
// framework rather than the other way round.
//
// Two rules, and they are what make this safe rather than a loophole:
//
//   - Nothing here may ever be imported by the root module. Not a helper, not
//     a fixture. The day something is, the root's own checks go red — a nested
//     module is excluded from `./...`, so the import would have to become a
//     real require in the root go.mod to compile at all.
//   - Versions are pinned, recorded with every published figure, and never
//     bumped by a bot. "faster than pgx" without a version is not a claim.
//
// Run it from here: `cd benchmarks && go test -bench . ./...`. The root's
// `go test ./...` does not see this directory, which is the point.
module github.com/mlagarrigue/sluice/benchmarks

go 1.26.0

replace github.com/mlagarrigue/sluice => ../

require (
	github.com/gin-gonic/gin v1.12.0
	github.com/go-chi/chi/v5 v5.3.2
	github.com/gofiber/fiber/v2 v2.52.15
	github.com/jackc/pgx/v5 v5.10.0
	github.com/labstack/echo/v4 v4.15.4
	github.com/lib/pq v1.12.3
	github.com/mlagarrigue/sluice v0.0.0-20260817025111-7de27a56a793
)

require (
	github.com/andybalholm/brotli v1.1.0 // indirect
	github.com/bytedance/gopkg v0.1.3 // indirect
	github.com/bytedance/sonic v1.15.0 // indirect
	github.com/bytedance/sonic/loader v0.5.0 // indirect
	github.com/cloudwego/base64x v0.1.6 // indirect
	github.com/gabriel-vasile/mimetype v1.4.12 // indirect
	github.com/gin-contrib/sse v1.1.0 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/go-playground/validator/v10 v10.30.1 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/labstack/gommon v0.5.0 // indirect
	github.com/leodido/go-urn v1.4.0 // indirect
	github.com/mattn/go-colorable v0.1.15 // indirect
	github.com/mattn/go-isatty v0.0.22 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pelletier/go-toml/v2 v2.2.4 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/quic-go/quic-go v0.59.0 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/ugorji/go/codec v1.3.1 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasthttp v1.51.0 // indirect
	github.com/valyala/fasttemplate v1.2.2 // indirect
	github.com/valyala/tcplisten v1.0.0 // indirect
	go.mongodb.org/mongo-driver/v2 v2.5.0 // indirect
	golang.org/x/arch v0.22.0 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
)
