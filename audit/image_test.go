package audit

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

// fakeRegistry serves one image's manifest under /v2/demo/app, behind an
// anonymous bearer token, and whatever paths extra names beside it.
// Referrers answers its own index when the registry has the API, 404
// otherwise; every other path is 404.
func fakeRegistry(t *testing.T, manifest string, referrers *string, extra map[string]string) (string, string) {
	t.Helper()
	sum := sha256.Sum256([]byte(manifest))
	digest := "sha256:" + hex.EncodeToString(sum[:])
	paths := map[string]string{} // DIGEST stands for the tag form, sha256-<hex>
	for k, v := range extra {
		paths[strings.Replace(k, "DIGEST", strings.Replace(digest, ":", "-", 1), 1)] = v
	}
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			if r.URL.Query().Get("scope") != "repository:demo/app:pull" {
				http.Error(w, "scope", http.StatusBadRequest)
				return
			}
			_, _ = w.Write([]byte(`{"token": "anon"}`))
			return
		}
		if r.Header.Get("Authorization") != "Bearer anon" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="`+srv.URL+`/token",service="fake",scope="repository:demo/app:pull"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch p := r.URL.Path; {
		case p == "/v2/demo/app/manifests/v1" || p == "/v2/demo/app/manifests/"+digest:
			w.Header().Set("Docker-Content-Digest", digest)
			_, _ = w.Write([]byte(manifest))
		case p == "/v2/demo/app/referrers/"+digest && referrers != nil:
			w.Header().Set("Content-Type", "application/vnd.oci.image.index.v1+json")
			_, _ = w.Write([]byte(*referrers))
		default:
			body, ok := paths[p]
			if !ok {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write([]byte(body))
		}
	}))
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://") + "/demo/app:v1", digest
}

func imageRules(t *testing.T, ref string) []string {
	t.Helper()
	return fired(t, Selection{Components: {"docker"}, Sources: {"network"}}, Spec{Image: ref})
}

// TestAnUnsignedImageIsFound: a registry that holds nothing beside the
// image — an empty Referrers answer, no cosign tag — gives both findings,
// the anonymous token fetched on the way.
func TestAnUnsignedImageIsFound(t *testing.T) {
	empty := `{"schemaVersion": 2, "manifests": []}`
	ref, _ := fakeRegistry(t, `{"schemaVersion": 2, "layers": []}`, &empty, nil)
	got := imageRules(t, ref)
	if !slices.Equal(got, []string{"docker/image-signature", "docker/image-provenance"}) {
		t.Errorf("an image with nothing beside it: %v", got)
	}
}

// TestSignaturesAndProvenanceAreFoundEachWay: a Sigstore bundle and an
// in-toto statement as referrers; cosign's tags with a BuildKit
// attestation in the index; a Notation signature under the referrers tag
// schema of a registry without the API. Each is enough.
func TestSignaturesAndProvenanceAreFoundEachWay(t *testing.T) {
	referred := `{"schemaVersion": 2, "manifests": [
		{"digest": "sha256:aa", "artifactType": "application/vnd.dev.sigstore.bundle.v0.3+json"},
		{"digest": "sha256:bb", "artifactType": "application/vnd.dev.sigstore.bundle.v0.3+json",
		 "annotations": {"dev.sigstore.bundle.predicateType": "https://slsa.dev/provenance/v1"}}]}`
	ref, _ := fakeRegistry(t, `{"schemaVersion": 2, "layers": []}`, &referred, nil)
	if got := imageRules(t, ref); len(got) != 0 {
		t.Errorf("Sigstore bundles as referrers: %v", got)
	}

	index := `{"schemaVersion": 2, "manifests": [{"digest": "sha256:cc"},
		{"digest": "sha256:dd", "annotations": {"vnd.docker.reference.type": "attestation-manifest"}}]}`
	ref, _ = fakeRegistry(t, index, nil, map[string]string{"/v2/demo/app/manifests/DIGEST.sig": "{}"})
	if got := imageRules(t, ref); len(got) != 0 {
		t.Errorf("a cosign tag and a BuildKit attestation: %v", got)
	}

	notation := `{"schemaVersion": 2, "manifests": [{"digest": "sha256:ee", "artifactType": "application/vnd.cncf.notary.signature"}]}`
	ref, _ = fakeRegistry(t, `{"schemaVersion": 2, "layers": []}`, nil, map[string]string{"/v2/demo/app/manifests/DIGEST": notation})
	if got := imageRules(t, ref); !slices.Equal(got, []string{"docker/image-provenance"}) {
		t.Errorf("a Notation signature under the referrers tag schema, no provenance: %v", got)
	}
}

// TestARegistryThatLiesAboutTheDigestIsRefused: the digest the audit
// reports is the one it computed; a registry whose header disagrees is
// not believed.
func TestARegistryThatLiesAboutTheDigestIsRefused(t *testing.T) {
	ref, digest := fakeRegistry(t, `{"schemaVersion": 2}`, nil, nil)
	if _, err := Open(Spec{Image: strings.TrimSuffix(ref, ":v1") + "@sha256:" + strings.Repeat("0", 64)}); err == nil {
		t.Error("a manifest asked by a digest it does not hash to was accepted")
	}
	target, err := Open(Spec{Image: strings.TrimSuffix(ref, ":v1") + "@" + digest})
	if err != nil {
		t.Fatal(err)
	}
	if target.Image.Digest != digest {
		t.Errorf("digest %s, want %s", target.Image.Digest, digest)
	}
}

func TestImageReferencesAreReadAsDockerDoes(t *testing.T) {
	d := "sha256:" + strings.Repeat("a", 64)
	for in, want := range map[string]imageRef{
		"nginx":                        {"registry-1.docker.io", "library/nginx", "latest"},
		"bitnami/postgresql:17":        {"registry-1.docker.io", "bitnami/postgresql", "17"},
		"ghcr.io/org/app:v1@" + d:      {"ghcr.io", "org/app", d},
		"localhost/app":                {"localhost", "app", "latest"},
		"127.0.0.1:5000/team/app:edge": {"127.0.0.1:5000", "team/app", "edge"},
	} {
		got, err := parseImageRef(in)
		if err != nil || got != want {
			t.Errorf("%s: %+v, %v; want %+v", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "app@sha512:" + strings.Repeat("a", 128), "app@sha256:short"} {
		if _, err := parseImageRef(bad); err == nil {
			t.Errorf("%q: read", bad)
		}
	}
}

func TestAuthParamsKeepQuotedCommas(t *testing.T) {
	got := authParams(`realm="https://auth.example/token",service="registry",scope="repository:a/b:pull,push"`)
	if got["realm"] != "https://auth.example/token" || got["service"] != "registry" || got["scope"] != "repository:a/b:pull,push" {
		t.Errorf("%v", got)
	}
}
