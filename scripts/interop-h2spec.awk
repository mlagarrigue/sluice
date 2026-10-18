# Reads the text report of h2spec v2.6.0 and prints every red case as
# <group>/<section>/<case>, the form net/httpstream/testdata/h2spec-exclusions.txt
# uses. The JUnit report of that version records no failures, so the
# "Failures:" section at the end of the text output — which repeats every
# red case under its section heading — is what gets parsed.
# scripts/interop-http.sh runs it.
/^Failures:/ { on = 1; next }
!on { next }
/^Generic tests/ { group = "generic"; next }
/^Hypertext Transfer/ { group = "http2"; next }
/^HPACK/ { group = "hpack"; next }
/^ +[0-9]+(\.[0-9]+)*\. / {
  match($0, /[0-9]+(\.[0-9]+)*/); section = substr($0, RSTART, RLENGTH); next
}
/\303\227/ {
  match($0, /[0-9]+:/); print group "/" section "/" substr($0, RSTART, RLENGTH - 1)
}
