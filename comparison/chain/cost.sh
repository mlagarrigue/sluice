#!/usr/bin/env bash
# The writing cost of every contender (study §1.1), one tab-separated line
# per version: contender, version, the lines written by hand for it, the
# lines of the contract code its contender shares, the dependencies linked
# into the deliverable (direct, then all), and the concepts one must know.
#
#   comparison/chain/cost.sh
#
# Lines are counted by one tool for every language, scc, code lines only
# (no blank, no comment), tests and build configuration left out. scc runs
# in its own image (scc/Dockerfile), with no network, on a copy of the
# files it counts. A variant
# "with" counts the lines it adds or changes against the version it starts
# from: the lines of its own files, or those a diff against its base shows.
# Dependencies follow the command §1.1 names for each ecosystem, on what the
# deliverable links: `go version -m` on each Go binary, `cargo tree -e
# normal` on each Rust binary, and for the stacks whose image ships one set
# for every version -- `npm ls --omit=dev --all`, the image's pip packages,
# the jars of the Spring Boot jar, `dotnet list package --include-transitive`
# -- that set. The tools run in the stacks' own pinned images, on this tree
# through the devcontainer's volumes (DEVCONTAINER, sluice-dev). The
# concepts are written by hand, in concepts.tsv.
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 2
root=$PWD
DEVCONTAINER=${DEVCONTAINER:-sluice-dev}
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
docker build -q -t sluice-scc comparison/chain/scc >/dev/null || exit 2

# code counts the code lines of the files and directories given.
code() {
	local d c
	d=$(mktemp -d "$tmp/code.XXXX") && chmod 755 "$d" && cp -r "$@" "$d"/ || exit 2
	c=$(docker create --network none sluice-scc --format json /w) || exit 2
	docker cp -q "$d" "$c:/w" && docker start "$c" >/dev/null && docker wait "$c" >/dev/null
	docker logs "$c" 2>/dev/null | jq '[.[].Code] | add // 0'
	docker rm "$c" >/dev/null
}

# added counts the code lines a variant adds or changes against its base:
# the + lines of a diff, kept in a file of the variant's language.
added() {
	local base=$1 variant=$2 out="$tmp/added.${2##*.}"
	diff -U0 "$base" "$variant" | grep '^+' | grep -v '^+++' | cut -c2- >"$out"
	code "$out"
}

# indocker runs a command in an image, the tree mounted, and prints its
# output: an attached run loses its output after a few seconds here.
indocker() {
	local image=$1 c
	shift
	c=$(docker create --volumes-from "$DEVCONTAINER" -u "$(id -u):$(id -g)" -e HOME=/tmp "$image" "$@") || return 1
	docker start "$c" >/dev/null && docker wait "$c" >/dev/null
	docker logs "$c" 2>/dev/null
	docker rm "$c" >/dev/null
}

concepts() { awk -F'\t' -v c="$1" -v v="$2" '$1 == c && $2 == v { print $3 }' comparison/chain/concepts.tsv; }

row() { printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" "$5" "$6" "$(concepts "$1" "$2")"; }

printf 'contender\tversion\tlines\tcontract lines\tdirect deps\tall deps\tconcepts\n'

# Go: one binary per version, the deps it links (the module itself is no
# dep: it requires no other module of this tree); direct ones are those
# go.mod requires without "// indirect".
g=comparison/go-nethttp
gc=$(code $g/internal/contract/contract.go $g/internal/contract/metrics.go)
direct_go=$(awk '/^require/ { r = 1; next } r && /^\)/ { r = 0 } r && !/indirect/ { print $1 }' $g/go.mod | sort)
for v in standard optimised gin-standard gin-optimised dataloader; do
	GOWORK=off go build -C $g -o "$tmp/$v" "./$v" || exit 2
	go version -m "$tmp/$v" | awk '$1 == "dep" { print $2 }' | sort >"$tmp/$v.deps"
	all=$(wc -l <"$tmp/$v.deps")
	dir=$(comm -12 "$tmp/$v.deps" <(echo "$direct_go") | wc -l)
	lines=$(code $g/$v/main.go)
	[ $v = dataloader ] && lines=$(added $g/standard/main.go $g/dataloader/main.go)
	row go-nethttp $v "$lines" "$gc" "$dir" "$all"
done

# Sluice.go: the program of the configuration R alone (sluice/reference),
# not the study's server beside it, which carries every axis of the study
# behind a flag; the contract it shares with that server is counted apart.
# The framework is a dependency like any other: go.mod requires it,
# replaced by this tree.
sl=comparison/sluice
slc=$(code $sl/internal/contract/contract.go $sl/internal/contract/metrics.go)
GOWORK=off go build -C $sl -o "$tmp/reference" ./reference || exit 2
go version -m "$tmp/reference" | awk '$1 == "dep" { print $2 }' | sort >"$tmp/reference.deps"
direct_sl=$(awk '/^require/ { r = 1; next } r && /^\)/ { r = 0 } r && !/indirect/ { print $1 }' $sl/go.mod | sort)
row sluice reference "$(code $sl/reference/main.go)" "$slc" \
	"$(comm -12 "$tmp/reference.deps" <(echo "$direct_sl") | wc -l)" "$(wc -l <"$tmp/reference.deps")"

# Node: one set of packages for every version (npm ci --omit=dev).
n=comparison/node-express
node_image=$(sed -n 's/^FROM \(node:[^ ]*\) AS standard-doc$/\1/p' $n/Dockerfile)
all_node=$(indocker "$node_image" sh -c "cd $root/$n && npm ls --omit=dev --all --parseable 2>/dev/null" | tail -n +2 | sort -u | wc -l)
[ "$all_node" -gt 0 ] || exit 2
dir_node=$(jq '.dependencies | length' $n/package.json)
nc=$(code $n/contract.js)
for v in standard optimised fastify-standard fastify-optimised; do
	row node-express $v "$(code $n/$v/server.js)" "$nc" "$dir_node" "$all_node"
done
row node-express dataloader "$(added $n/standard/server.js $n/dataloader/server.js)" "$nc" "$dir_node" "$all_node"
row node-express dataloader-optimised "$(added $n/optimised/server.js $n/dataloader-optimised/server.js)" "$nc" "$dir_node" "$all_node"

# Rust: the crates cargo tree links (proc macros left out: they run at
# build time and are not linked), one set for every version: cargo tree
# reads the package, and its two binaries (server for the standard and
# optimised images, dataloader for the variant) share its dependencies.
r=comparison/rust-axum
rust_image=$(sed -n 's/^FROM \(rust:[^ ]*\) AS build-opt$/\1/p' $r/Dockerfile)
rc=$(code $r/src/lib.rs)
tree=$(indocker "$rust_image" sh -c "cd $root/$r && CARGO_HOME=/tmp/cargo cargo tree -q --locked -e normal,no-proc-macro --prefix depth -f '{p}' 2>/dev/null")
all_rust=$(echo "$tree" | awk 'NR > 1 { sub(/^[0-9]+/, ""); print $1, $2 }' | sort -u | wc -l)
dir_rust=$(echo "$tree" | grep -c '^1[^0-9]')
[ "$all_rust" -gt 0 ] || exit 2
for v in standard optimised; do
	row rust-axum $v "$(code $r/src/main.rs)" "$rc" "$dir_rust" "$all_rust"
done
row rust-axum dataloader "$(added $r/src/main.rs $r/src/bin/dataloader.rs)" "$rc" "$dir_rust" "$all_rust"

# Python: the image's packages, one set for every version; direct ones are
# those requirements.txt marks "# direct" (a freeze names every package
# alike, and pip's --not-required counts what extras pull in, uvicorn's
# among them, as chosen).
p=comparison/python-fastapi
py_image=python-fastapi:optimised-opt
docker build -q -f $p/Dockerfile --target optimised-opt -t $py_image . >/dev/null || exit 2
all_py=$(indocker $py_image /venv/bin/pip list --format=freeze --exclude pip | wc -l)
[ "$all_py" -gt 0 ] || exit 2
dir_py=$(grep -c "  # direct$" $p/requirements.txt)
pc=$(code $p/contract.py)
for v in standard optimised; do
	row python-fastapi $v "$(code $p/$v/main.py)" "$pc" "$dir_py" "$all_py"
done
row python-fastapi dataloader-optimised "$(added $p/optimised/main.py $p/dataloader-optimised/main.py)" "$pc" "$dir_py" "$all_py"

# Java: the jars inside the Spring Boot jar, one set for every image; the
# direct ones are the pom's dependencies outside the test scope.
j=comparison/java-spring
docker build -q -f $j/Dockerfile --target jar -t java-spring:jar . >/dev/null || exit 2
all_java=$(indocker java-spring:jar jar tf /build/application.jar | grep -c '^BOOT-INF/lib/.*\.jar$')
[ "$all_java" -gt 0 ] || exit 2
dir_java=$(awk '/<dependency>/ { d = 1; t = 0 } d && /<scope>test</ { t = 1 } /<\/dependency>/ { if (d && !t) n++; d = 0 } END { print n }' $j/pom.xml)
s=$j/src/main/java
jc=$(code $s/study/contract)
for v in standard optimised native; do
	row java-spring $v "$(code $s/study/Application.java $s/study/Orders.java)" "$jc" "$dir_java" "$all_java"
done
row java-spring dataloader "$(code $s/dataloader)" "$jc" "$dir_java" "$all_java"

# .NET: the project's packages, top-level and transitive, one set for every
# image; the shared framework (Microsoft.AspNetCore.App) is the runtime's.
d=comparison/dotnet-minimal
sdk_image=$(sed -n 's/^FROM \(mcr.microsoft.com\/dotnet\/sdk:[^ ]*\) AS build$/\1/p' $d/Dockerfile)
pkgs=$(indocker "$sdk_image" sh -c "cp -r $root/$d /tmp/p && cd /tmp/p && DOTNET_CLI_TELEMETRY_OPTOUT=1 dotnet list package --include-transitive 2>/dev/null")
dir_net=$(echo "$pkgs" | awk '/Top-level Package/ { s = 1; next } /Transitive Package/ { s = 2; next } s == 1 && $1 == ">" { n++ } END { print n + 0 }')
all_net=$(echo "$pkgs" | awk '$1 == ">" { n++ } END { print n + 0 }')
[ "$all_net" -gt 0 ] || exit 2
dc=$(code $d/Contract.cs)
for v in standard optimised multiplexing; do
	row dotnet-minimal $v "$(code $d/Program.cs)" "$dc" "$dir_net" "$all_net"
done
row dotnet-minimal dataloader "$(code $d/Dataloader.cs)" "$dc" "$dir_net" "$all_net"
