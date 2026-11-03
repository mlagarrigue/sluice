# Web reference

The settings of the token verifier `web.NewHMACVerifier` builds, each with
what its zero value means and what is refused. The
[web guide](../guide/web.md#authenticating-authorizing) says when to reach
for it.

## Verifier settings

`NewHMACVerifier` refuses what cannot be safe.

| Field | Zero | Bounds | What it sets |
|---|---|---|---|
| `Secret` | — (required) | ≥ 32 bytes, from `crypto/rand` | the HMAC key |
| `Previous` | none | each ≥ 32 bytes | retired keys still accepted during a rotation |
| `Audience` | none: a token carrying `aud` is refused | | the `aud` value required |
| `Issuer` | any issuer | | the `iss` value required |
| `TenantClaim` | none | | the claim holding the tenant, then required |
| `Leeway` | none | ≥ 0 | clock skew allowed on `exp`, `nbf`, `iat` |
| `MaxTokenBytes` | 8 KiB | ≥ 0 | the largest token read |
| `Now` | `time.Now` | | the clock, for tests |
