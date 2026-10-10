# Replication transport security

`/internal/replication/*` on the ingestion service streams the full WAL and
snapshot of every tenant to followers, and followers prove themselves with a
shared token (`x-replication-token`). This page says what protects that
channel and how to configure it. The TLS settings themselves are described in
[`tls.md`](tls.md).

## What the code does

* **Authentication.** The endpoints answer 403 unless the token matches
  (constant-time compare); an ingestion follower refuses to start without a
  token outside dev mode. See `DASH_INGEST_REPLICATION_TOKEN`.
* **Native TLS on the leader.** With `DASH_INGEST_TLS_CERT_FILE` and
  `DASH_INGEST_TLS_KEY_FILE` the ingestion listener serves HTTPS only (TLS
  1.2/1.3, rustls). The handshake runs off the worker pool and is bounded by
  the first-byte timeout and the connection caps.
* **Mutual TLS and per-follower identity.** With
  `DASH_INGEST_TLS_CLIENT_CA_FILE` the leader verifies client certificates.
  `DASH_INGEST_REPLICATION_REQUIRE_CLIENT_CERT=1` makes a verified client
  certificate mandatory on `/internal/replication/*` (in addition to the
  token) while the public API keeps accepting clients without one, and
  `DASH_INGEST_REPLICATION_ALLOWED_CLIENT_CERTS` pins the allowed follower
  certificates by SHA-256 fingerprint, so a single follower can be cut off
  without rotating the token. The fingerprint reaches the handler through a
  header the transport overwrites, so a client cannot forge it.
* **Followers speak `https://` and verify the leader.** Both followers
  (retrieval, and an ingestion node following another one) use the same client
  (`services/common/src/replication_client.rs`, `ureq` with `rustls`). An
  `https://` source URL is verified against the bundled public roots plus an
  optional PEM bundle in `DASH_REPLICATION_CA_FILE`; verification cannot be
  turned off. `DASH_REPLICATION_CLIENT_CERT_FILE` and
  `DASH_REPLICATION_CLIENT_KEY_FILE` present the follower certificate.
  Redirects are never followed, so the token cannot be forwarded to another
  host, and response bodies are read through a hard size cap.
* **The token is not sent over plaintext to other machines.** A follower
  whose source URL is `http://` to a non-loopback host refuses to send the
  token and every poll fails with a message naming
  `DASH_REPLICATION_ALLOW_INSECURE_HTTP`; an ingestion follower refuses to
  start. Setting `DASH_REPLICATION_ALLOW_INSECURE_HTTP=1` acknowledges the
  exposure and allows it. Loopback (`localhost`, `127.0.0.0/8`, `::1`) and
  `https://` are always allowed.
* **Startup warnings.** A follower logs a warning when its source is
  `http://` to a non-loopback host (even with the acknowledgement set). A
  leader logs a warning when a replication token is configured, its listener
  is bound to a non-loopback address and TLS is off.

## Shipped configurations

| Artifact | Default | With TLS |
|---|---|---|
| Helm | plain HTTP, `DASH_REPLICATION_ALLOW_INSECURE_HTTP=1` | `tls.enabled=true`, `tls.secretName=...`: https source, mutual TLS, certificate required on replication, acknowledgement removed |
| Kustomize | `deploy/k8s`: plain HTTP with the acknowledgement | `deploy/k8s-tls` overlay: same as Helm with TLS |
| Compose | plain HTTP on the compose network with the acknowledgement | `docker-compose.tls.yml` (+ `scripts/generate-dev-tls.sh` for development material) |
| systemd | loopback source | commented TLS block in the env examples |

The plain-HTTP defaults remain so that a first install works without a PKI;
the acknowledgement variable in them is an explicit statement that the WAL
stream crosses the pod or compose network unencrypted, not protection.

## Recommended setup

1. Issue the ingestion certificate and the follower client certificates from
   a private CA (cert-manager CA issuer, Vault PKI, or your internal CA).
2. On ingestion: `DASH_INGEST_TLS_CERT_FILE`, `DASH_INGEST_TLS_KEY_FILE`,
   `DASH_INGEST_TLS_CLIENT_CA_FILE`, and
   `DASH_INGEST_REPLICATION_ALLOWED_CLIENT_CERTS` (or
   `DASH_INGEST_REPLICATION_REQUIRE_CLIENT_CERT=1`).
3. On each follower: `https://` source URL, `DASH_REPLICATION_CA_FILE`,
   `DASH_REPLICATION_CLIENT_CERT_FILE`, `DASH_REPLICATION_CLIENT_KEY_FILE`;
   unset `DASH_REPLICATION_ALLOW_INSECURE_HTTP`.
4. Keep NetworkPolicy (`deploy/k8s/50-networkpolicy.yaml`) restricting who
   can reach the ingestion port; TLS protects the content, the policy limits
   exposure. Never route `/internal/*` through a public ingress.

A service mesh with strict mTLS is still a valid alternative (keep the
application URL `http://` and the acknowledgement set), but it is no longer
required to encrypt replication.

## Remaining limits

* The token is still one shared secret for all followers; the certificate
  allowlist is what gives per-follower identity.
* No CRL or OCSP checking; use short-lived certificates and the fingerprint
  allowlist to revoke a follower.
* NetworkPolicy in `deploy/k8s` also allows the ingress controller namespace to
  reach the ingestion port; the Ingress resource never routes `/internal/*`,
  but another pod in that namespace could connect directly (with TLS and the
  certificate requirement on, it cannot replicate without a follower
  certificate and the token).
* Frames are not signed: integrity in flight comes from TLS, not from the
  replication protocol.

Tests: `pkg/http/tests/tls.rs` (handshake, mTLS accept/reject, plaintext
refusal, stalled handshakes, per-IP cap, rotation),
`services/common/tests/replication_client.rs` (https with a private CA,
untrusted certificate refused, mutual TLS accepted and refused, token refused
over remote plain http, no redirects, size caps),
`services/ingestion/src/transport/replication.rs` (`client_cert_policy_tests`),
`services/ingestion/src/transport/authz.rs` and
`services/retrieval/src/replication.rs` (startup warnings and refusal), and
the end-to-end scenario `tests/e2e/tests/s10_tls_replication.rs` (real
binaries, retrieval following ingestion over mutual TLS).
