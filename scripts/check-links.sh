#!/bin/sh
# Fetches every external address the documents and the Go comments cite and
# fails on any that does not answer: a reader who follows a link to a page
# that is gone has lost the evidence the sentence rests on.
#
#   scripts/check-links.sh
#
# Relative links and anchors are repo_test.go's (TestDocumentationLinksResolve);
# this is the half that needs the network, so it runs weekly rather than on
# every push, where a site's bad hour would fail a change that touched no
# link. An address answers when curl, following redirects, ends on a 2xx.
#
# Some sites refuse a client that is not a browser whatever the page; they
# are listed in skipped() with the reason, and reported rather than checked.
# An address whose host has no dot (http://h, in a comment's example) or
# names an example domain is not one anybody follows.
set -eu
cd "$(dirname "$0")/.."

url_pattern='https?://[^] )>"`<]+'

skipped() {
  case "$1" in
    https://dl.acm.org/*) echo "answers 403 to any client that is not a browser" ;;
  esac
}

{
  grep -rohE "$url_pattern" --include='*.md' README.md CONTRIBUTING.md SECURITY.md docs
  git grep -hE "^[[:space:]]*//.*https?://" -- '*.go' | grep -oE "$url_pattern"
} | sed "s/[.,;:']*\$//" | grep -E '^https?://[^/:?#]+\.[^/:?#]+' |
  grep -vE '^https?://(127\.0\.0\.1|example\.com|example\.org)[:/?#]' | sort -u >"${TMPDIR:-/tmp}/check-links.$$"

status=0
checked=0
while IFS= read -r url; do
  reason=$(skipped "$url")
  if [ -n "$reason" ]; then
    echo "skipped $url: $reason"
    continue
  fi
  code=$(curl -sSL -o /dev/null -w '%{http_code}' --max-time 20 --retry 2 --retry-delay 3 \
    -A 'sluice-link-check (+https://github.com/mlagarrigue/sluice)' "$url" 2>/dev/null) || code=000
  checked=$((checked + 1))
  case "$code" in
    2??) ;;
    *)
      echo "check-links: $url answered $code" >&2
      status=1
      ;;
  esac
done <"${TMPDIR:-/tmp}/check-links.$$"
rm -f "${TMPDIR:-/tmp}/check-links.$$"
echo "check-links: $checked addresses fetched"
exit $status
