# Web reference

The settings of the token verifiers `web.NewHMACVerifier` and
`web.NewPublicKeyVerifier` build, each with what its zero value means and
what is refused. The
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

## Public-key verifier settings

`NewPublicKeyVerifier` refuses what cannot be safe, and verifies the
signature before it reads a claim.

| Field | Zero | Bounds | What it sets |
|---|---|---|---|
| `Keys` | — (required) | RSA ≥ 2048 bits, ECDSA on P-256, P-384 or P-521, Ed25519 | the issuer's public keys by `kid`; without `kid`, the only key |
| `Algorithms` | — (required) | among RS256/384/512, PS256/384/512, ES256/384/512, EdDSA | the `alg` values accepted; each must fit the key its token names |

`Audience`, `Issuer`, `TenantClaim`, `Leeway`, `MaxTokenBytes` and `Now`
are the HMAC verifier's, in the table above.
