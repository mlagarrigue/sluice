package audit

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// What a registry holds beside an image: its signatures and its build
// provenance. Both are artifacts stored next to the image and pointing at
// its digest, found three ways — the OCI 1.1 Referrers API
// (/v2/<name>/referrers/<digest>) or, on a registry without it, the
// referrers tag schema (sha256-<hex>), and cosign's own tags,
// sha256-<hex>.sig and .att — plus BuildKit's attestation manifests,
// carried inside the image index itself. The audit says whether there is
// a signature, not whether it verifies: a key, a certificate identity and
// a transparency log are a policy the reader holds, not a setting.

// ImageChain is what a registry says of an image's supply chain.
type ImageChain struct {
	Ref        string
	Digest     string
	Signatures []string // how each signature was found
	Provenance []string // how each attestation was found
}

// imageRef is an image reference cut into the registry's host, the
// repository and the tag or digest.
type imageRef struct {
	host, repo, ref string
}

// parseImageRef reads [host/]repo[:tag][@digest] as Docker does: a first
// component with a dot, a colon or named localhost is a registry; Docker
// Hub's single names live under library/.
func parseImageRef(s string) (imageRef, error) {
	r := imageRef{host: "registry-1.docker.io"}
	rest := s
	if i := strings.Index(rest, "/"); i > 0 {
		if first := rest[:i]; strings.ContainsAny(first, ".:") || first == "localhost" {
			r.host, rest = first, rest[i+1:]
		}
	}
	name, digest, hasDigest := strings.Cut(rest, "@")
	tag := "latest"
	if i := strings.LastIndex(name, ":"); i >= 0 {
		name, tag = name[:i], name[i+1:]
	}
	if name == "" || strings.ContainsAny(name, " \t") {
		return r, fmt.Errorf("%q: not an image reference", s)
	}
	if r.host == "registry-1.docker.io" && !strings.Contains(name, "/") {
		name = "library/" + name
	}
	r.repo, r.ref = name, tag
	if hasDigest {
		if !strings.HasPrefix(digest, "sha256:") || len(digest) != len("sha256:")+64 {
			return r, fmt.Errorf("%q: a digest other than sha256", s)
		}
		r.ref = digest
	}
	return r, nil
}

// registry is a client of one registry's distribution API: anonymous,
// with the bearer token a registry hands out to anyone for a pull.
type registry struct {
	base   string
	client *http.Client
	token  string
}

// maxRegistryBody bounds what is read of any answer: manifests and
// indexes are a few kilobytes, a registry is not trusted to stop.
const maxRegistryBody = 4 << 20

func newRegistry(host string) *registry {
	scheme := "https"
	// Docker's own rule: a registry on the loopback is reached in clear.
	if h, _, err := net.SplitHostPort(host); (err == nil && (h == "localhost" || net.ParseIP(h).IsLoopback())) || host == "localhost" {
		scheme = "http"
	}
	return &registry{base: scheme + "://" + host, client: &http.Client{Timeout: 20 * time.Second}}
}

// get fetches a path, answering one bearer challenge on the way. It
// returns the status, the headers and the body, read to its bound.
func (r *registry) get(method, path string, accept ...string) (status int, header http.Header, body []byte, err error) {
	for attempt := 0; ; attempt++ {
		var req *http.Request
		if req, err = http.NewRequest(method, r.base+path, http.NoBody); err != nil { //nolint:noctx // bounded by the client's timeout
			return 0, nil, nil, err
		}
		for _, a := range accept {
			req.Header.Add("Accept", a)
		}
		if r.token != "" {
			req.Header.Set("Authorization", "Bearer "+r.token)
		}
		var resp *http.Response
		if resp, err = r.client.Do(req); err != nil {
			return 0, nil, nil, err
		}
		body, err = io.ReadAll(io.LimitReader(resp.Body, maxRegistryBody))
		_ = resp.Body.Close()
		if err != nil {
			return 0, nil, nil, err
		}
		if resp.StatusCode == http.StatusUnauthorized && attempt == 0 {
			if aerr := r.authenticate(resp.Header.Get("WWW-Authenticate")); aerr != nil {
				return 0, nil, nil, aerr
			}
			continue
		}
		return resp.StatusCode, resp.Header, body, nil
	}
}

// authenticate answers a Bearer challenge (RFC 6750 §3) by asking its
// realm for an anonymous token, as Docker does for a public pull.
func (r *registry) authenticate(challenge string) error {
	scheme, params, ok := strings.Cut(challenge, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return fmt.Errorf("the registry asks for %q, not an anonymous bearer token", scheme)
	}
	p := authParams(params)
	realm, err := url.Parse(p["realm"])
	if err != nil || (realm.Scheme != "https" && realm.Scheme != "http") {
		return fmt.Errorf("the registry's token realm %q is not a URL", p["realm"])
	}
	q := realm.Query()
	for _, k := range []string{"service", "scope"} {
		if p[k] != "" {
			q.Set(k, p[k])
		}
	}
	realm.RawQuery = q.Encode()
	resp, err := r.client.Get(realm.String()) //nolint:noctx // bounded by the client's timeout
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	var tok struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("the token realm answered %s", resp.Status)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxRegistryBody)).Decode(&tok); err != nil {
		return fmt.Errorf("the token realm's answer: %w", err)
	}
	if r.token = tok.Token; r.token == "" {
		r.token = tok.AccessToken
	}
	if r.token == "" {
		return errors.New("the token realm handed no token")
	}
	return nil
}

// authParams reads a challenge's auth-params, key=value or key="value"
// separated by commas (RFC 9110 §11.2). Quoted commas stay in the value.
func authParams(s string) map[string]string {
	out := map[string]string{}
	for s != "" {
		s = strings.TrimLeft(s, " ,")
		key, rest, ok := strings.Cut(s, "=")
		if !ok {
			break
		}
		var value string
		if strings.HasPrefix(rest, `"`) {
			end := strings.Index(rest[1:], `"`)
			if end < 0 {
				break
			}
			value, s = rest[1:1+end], rest[2+end:]
		} else {
			value, s, _ = strings.Cut(rest, ",")
		}
		out[strings.ToLower(strings.TrimSpace(key))] = value
	}
	return out
}

// The media types a manifest is asked for in, the indexes first.
var manifestTypes = []string{
	"application/vnd.oci.image.index.v1+json",
	"application/vnd.docker.distribution.manifest.list.v2+json",
	"application/vnd.oci.image.manifest.v1+json",
	"application/vnd.docker.distribution.manifest.v2+json",
}

// index is what the audit reads of an image index or manifest.
type index struct {
	Manifests []struct {
		Digest       string            `json:"digest"`
		ArtifactType string            `json:"artifactType"`
		Annotations  map[string]string `json:"annotations"`
	} `json:"manifests"`
}

// openImage reads an image's signatures and provenance from its registry.
func openImage(ref string) (*ImageChain, error) {
	r, err := parseImageRef(ref)
	if err != nil {
		return nil, err
	}
	reg := newRegistry(r.host)
	status, h, body, err := reg.get(http.MethodGet, "/v2/"+r.repo+"/manifests/"+r.ref, manifestTypes...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ref, err)
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("%s: the registry answered %d for the manifest", ref, status)
	}
	sum := sha256.Sum256(body)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	if strings.HasPrefix(r.ref, "sha256:") && r.ref != digest {
		return nil, fmt.Errorf("%s: the manifest served does not hash to the digest asked for", ref)
	}
	if d := h.Get("Docker-Content-Digest"); d != "" && d != digest {
		return nil, fmt.Errorf("%s: the registry says %s, the manifest hashes to %s", ref, d, digest)
	}
	c := &ImageChain{Ref: ref, Digest: digest}
	var top index
	if json.Unmarshal(body, &top) == nil {
		for _, m := range top.Manifests {
			if m.Annotations["vnd.docker.reference.type"] == "attestation-manifest" {
				c.Provenance = append(c.Provenance, "BuildKit attestation manifest "+m.Digest)
			}
		}
	}

	// The Referrers API, or its tag schema on a registry without it.
	tagged := "sha256-" + strings.TrimPrefix(digest, "sha256:")
	status, _, body, err = reg.get(http.MethodGet, "/v2/"+r.repo+"/referrers/"+digest, "application/vnd.oci.image.index.v1+json")
	how := "referrer"
	if err == nil && status == http.StatusNotFound {
		status, _, body, err = reg.get(http.MethodGet, "/v2/"+r.repo+"/manifests/"+tagged, "application/vnd.oci.image.index.v1+json")
		how = "referrers tag " + tagged
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ref, err)
	}
	if status == http.StatusOK {
		var refs index
		if err := json.Unmarshal(body, &refs); err != nil {
			return nil, fmt.Errorf("%s: the referrers: %w", ref, err)
		}
		for _, m := range refs.Manifests {
			kind, found := classifyReferrer(m.ArtifactType, m.Annotations)
			switch kind {
			case "signature":
				c.Signatures = append(c.Signatures, how+" "+found)
			case "provenance":
				c.Provenance = append(c.Provenance, how+" "+found)
			}
		}
	}

	// cosign's tags, which every cosign release still writes by default.
	for suffix, list := range map[string]*[]string{".sig": &c.Signatures, ".att": &c.Provenance} {
		status, _, _, err := reg.get(http.MethodHead, "/v2/"+r.repo+"/manifests/"+tagged+suffix, manifestTypes...)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", ref, err)
		}
		if status == http.StatusOK {
			*list = append(*list, "cosign tag "+tagged+suffix)
		}
	}
	return c, nil
}

// classifyReferrer says what an artifact pointing at the image is, by its
// artifactType: a signature (cosign, Notation, a Sigstore bundle holding
// one), an attestation (in-toto, DSSE, a Sigstore bundle holding a
// predicate), or neither.
func classifyReferrer(artifactType string, annotations map[string]string) (kind, what string) {
	switch {
	case strings.HasPrefix(artifactType, "application/vnd.dev.sigstore.bundle"):
		if p := annotations["dev.sigstore.bundle.predicateType"]; p != "" {
			return "provenance", artifactType + " (" + p + ")"
		}
		return "signature", artifactType
	case artifactType == "application/vnd.dev.cosign.artifact.sig.v1+json",
		artifactType == "application/vnd.cncf.notary.signature":
		return "signature", artifactType
	case strings.HasPrefix(artifactType, "application/vnd.in-toto"),
		artifactType == "application/vnd.dsse.envelope.v1+json",
		artifactType == "application/vnd.dev.cosign.artifact.att.v1+json":
		return "provenance", artifactType
	}
	return "", ""
}

func init() {
	Register(Rule{
		Name:  "docker/image-signature",
		Needs: Selection{Components: {"docker"}, Goals: {"security"}, Sources: {"network"}},
		Doc:   "https://docs.sigstore.dev/cosign/signing/signing_with_containers/",
		Check: func(t *Target, _ Answers) []Finding {
			c := t.Image
			if c == nil || len(c.Signatures) > 0 {
				return nil
			}
			return []Finding{{
				Current: fmt.Sprintf("%s (%s) has no signature in its registry: no referrer, no cosign tag", c.Ref, c.Digest),
				Proposed: "sign the image by its digest at the end of the build (cosign sign, or notation sign), " +
					"and check the signature where it is deployed",
				Why: "an image's tag can be moved by anyone who can push to the repository: without a " +
					"signature the machine that pulls it cannot tell the image that was built from one " +
					"put in its place",
			}}
		},
	})

	Register(Rule{
		Name:  "docker/image-provenance",
		Needs: Selection{Components: {"docker"}, Goals: {"security"}, Sources: {"network"}},
		Doc:   "https://slsa.dev/spec/v1.1/provenance",
		Check: func(t *Target, _ Answers) []Finding {
			c := t.Image
			if c == nil || len(c.Provenance) > 0 {
				return nil
			}
			return []Finding{{
				Current: fmt.Sprintf("%s (%s) has no provenance attestation in its registry: no BuildKit "+
					"attestation, no in-toto referrer, no cosign .att tag", c.Ref, c.Digest),
				Proposed: "docker buildx build --provenance=mode=max --sbom=true, or cosign attest with a SLSA predicate",
				Why: "provenance says which source, which builder and which steps made the image: without " +
					"it a signature proves who pushed the image, not what it was built from",
			}}
		},
	})
}
