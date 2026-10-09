# TLS and mutual TLS

Every DASH listener (ingestion, retrieval, control plane) can serve HTTPS
itself, verify client certificates, and run replication over mutual TLS. TLS
is **off by default**: a service with no certificate configured serves plain
HTTP exactly as before. This page covers the settings, how the server behaves,
how to deploy certificates with each shipped artifact, and what is not
covered.

Implementation: `pkg/http/src/tls.rs` (server and client configuration,
non-blocking handshake), `services/common/src/tls.rs` (per-service settings),
`services/common/src/replication_client.rs` (follower),
`services/metadata-router/src/lib.rs` (placement client). The setting
reference is generated from the registry: see
`docs-site/docs/reference/configuration.md`.

## Listener settings

`<P>` is `INGEST`, `RETRIEVAL` or `CONTROL_PLANE`.

| Variable | Meaning |
|---|---|
| `DASH_<P>_TLS_CERT_FILE` | PEM certificate chain, leaf first. |
| `DASH_<P>_TLS_KEY_FILE` | PEM private key (PKCS#8, PKCS#1 or SEC1). |
| `DASH_<P>_TLS_CLIENT_CA_FILE` | PEM bundle client certificates must chain to. Turns on client-certificate verification. |
| `DASH_<P>_TLS_REQUIRE_CLIENT_CERT` | Refuse clients without a certificate (needs the client CA). |

* Certificate and key together switch the listener to **HTTPS only**. One
  without the other, a client CA or the require flag without a certificate,
  an unreadable file, a PEM file with no certificate, or a key that does not
  match the certificate stop the service at startup with exit code 2 and a
  message naming the variable and the file (never key material).
* Protocols: TLS 1.2 and TLS 1.3 with the rustls safe defaults (ring
  provider, forward-secret AEAD cipher suites only; no TLS 1.0/1.1, no
  renegotiation, no compression). ALPN offers `http/1.1` only; a client that
  asks for `h2` alone is refused by the handshake, one that offers both gets
  `http/1.1`.
* Client certificates: with a client CA and without the require flag, a
  client may connect without a certificate, but a certificate it presents must
  verify (an untrusted one fails the handshake). With the require flag every
  client, probes included, must present one; see [Probes](#probes).
* The verified client certificate is identified by the lowercase hex SHA-256
  of its DER encoding. Handlers receive it as `Request::tls`
  (`pkg/http`); the ingestion and retrieval adapters copy it into the internal
  header `x-dash-verified-client-cert-sha256` after removing any copy the
  client sent, so it cannot be forged.

### Requiring a certificate on the replication routes only

Ingestion serves the public write API and the replication routes on one
port. To require client certificates only for replication:

| Variable | Meaning |
|---|---|
| `DASH_INGEST_REPLICATION_REQUIRE_CLIENT_CERT` | `/internal/replication/*` answers 403 unless the request came with a verified client certificate. |
| `DASH_INGEST_REPLICATION_ALLOWED_CLIENT_CERTS` | Comma-separated SHA-256 fingerprints (64 hex digits, colons allowed) of the follower certificates allowed; implies the requirement. |

Both come **on top of** `DASH_INGEST_REPLICATION_TOKEN`. They need
`DASH_INGEST_TLS_CLIENT_CA_FILE` (startup is refused otherwise), and a
malformed fingerprint refuses startup. The allowlist gives every follower its
own identity: removing a fingerprint cuts one follower off without rotating
the shared token. Compute a fingerprint with

```bash
openssl x509 -in follower.crt -outform der | sha256sum
```

## Client settings

### Replication followers

Retrieval, and an ingestion node that follows another one, use one client.

| Variable | Meaning |
|---|---|
| `DASH_RETRIEVAL_REPLICATION_SOURCE_URL` / `DASH_INGEST_REPLICATION_SOURCE_URL` | `https://host:port` of the leader. |
| `DASH_REPLICATION_CA_FILE` | Extra CAs trusted for the leader (a private CA). |
| `DASH_REPLICATION_CLIENT_CERT_FILE` / `DASH_REPLICATION_CLIENT_KEY_FILE` | Client certificate presented to the leader (mutual TLS). |

The leader certificate is **always verified**: against the bundled Mozilla
root set (webpki-roots) plus `DASH_REPLICATION_CA_FILE`, with the host name
(or IP address) of the source URL. There is no switch to disable verification.
Redirects are never followed. The files are read on every poll, so rotating
the follower certificate needs no restart. An ingestion follower with a
half-configured or unreadable client identity refuses to start; a retrieval
follower logs `every replication poll will fail: ...` at startup and reports
`token_transport_refused` or `source_unreachable` on `/ready`.

### Plaintext refusal

A follower whose source URL is `http://` to a non-loopback host refuses to
send the replication token unless `DASH_REPLICATION_ALLOW_INSECURE_HTTP=1`
acknowledges it (an ingestion follower refuses to start), and logs a warning
even then. A leader with a replication token, a non-loopback bind and **no**
TLS logs a warning at startup. Loopback and `https://` are always allowed.

### Placement client (ingestion and retrieval to the control plane)

`DASH_ROUTER_CONTROL_PLANE_URL` accepts `https://`. Trust and identity:
`DASH_ROUTER_CONTROL_PLANE_CA_FILE`,
`DASH_ROUTER_CONTROL_PLANE_CLIENT_CERT_FILE`,
`DASH_ROUTER_CONTROL_PLANE_CLIENT_KEY_FILE`; same verification rules as the
follower. Like the follower, the placement client refuses to send its bearer
token over plain `http://` to a non-loopback control plane unless
`DASH_ROUTER_ALLOW_INSECURE_HTTP=1`; `https://` is always allowed.

## How the server handles TLS connections

* **No worker is spent on a handshake.** The accept thread drives every
  handshake on a non-blocking socket inside the connection frontend. A
  connection reaches a worker only after the handshake finished (and, on
  services with a health lane, after its request line was decrypted and
  classified, so `/health`, `/ready` and `/metrics` keep their reserved lane
  over TLS).
* **Timeouts and caps apply to the handshake.** The handshake (plus the
  request line, with a health lane) must complete within
  `DASH_HTTP_FIRST_BYTE_TIMEOUT_MS` (default 2 s) or the connection is closed
  unanswered. Pending handshakes count against the per-IP cap
  (`DASH_HTTP_MAX_CONNS_PER_IP`) and the pending-connection bound; a
  connection over the cap is closed without a TLS answer. The whole-request
  deadline (`DASH_HTTP_REQUEST_TIMEOUT_MS`) still runs from accept.
* **Plaintext on a TLS port** gets a plaintext `400 this port requires TLS;
  use https://` and the connection is closed. It never reaches a worker.
* **Overload.** An admitted connection shed because the worker queue is full
  gets the 503 encrypted; a connection refused at admission (before its
  handshake) is closed.
* **Responses** end with a TLS `close_notify`.

## Certificate rotation

The certificate, key and client CA files are re-read at most once per second
(when a connection arrives) and the TLS configuration is rebuilt when their
**content** changed. New connections use the new certificate; established
ones keep theirs. A rotation that leaves the files unusable (a half-written
file, a key that does not match) is logged
(`tls: reload of '...' failed, keeping the previous certificate`) and the
previous configuration keeps serving. Write the key before the certificate,
or replace both atomically (Kubernetes Secret volumes and cert-manager do).
SIGHUP is not needed and does not reload certificates; a restart is never
required for rotation.

## Probes

Kubernetes `httpGet` probes with `scheme: HTTPS` do not verify the server
certificate and do not present a client certificate. They work with TLS and
with optional client certificates. With `DASH_<P>_TLS_REQUIRE_CLIENT_CERT=1`
they fail; use exec or TCP probes, or (recommended for ingestion) leave the
listener optional and use `DASH_INGEST_REPLICATION_REQUIRE_CLIENT_CERT`.

The container healthcheck (`dash-healthcheck`) switches to `https://` when
the service's `DASH_*_TLS_CERT_FILE` is set and verifies against
`DASH_HEALTHCHECK_CA_FILE` when given (the certificate must then name
`127.0.0.1`); without it the loopback probe skips verification because it
sends no credentials.

## Deploying certificates

All artifacts use the same file names: `tls.crt` (chain), `tls.key`, `ca.crt`
(the private CA that signs the service certificates and the follower client
certificates). The service certificate must name every DNS name clients use
and carry the `server auth` and `client auth` extended key usages when the
same certificate is also the follower's client certificate.

* **Helm**: `--set tls.enabled=true --set tls.secretName=<secret>`
  (`tls.mutual=true` by default). Mounts the Secret at `/etc/dash/tls`, sets
  the listener, follower and replication-route settings, removes
  `DASH_REPLICATION_ALLOW_INSECURE_HTTP`, switches probes, Service
  `appProtocol` and the ingress backend protocol to HTTPS. Rendering fails
  without `tls.secretName`. See `deploy/helm/dash/README.md`.
* **Kustomize**: `kubectl apply -k deploy/k8s-tls` (overlay of `deploy/k8s`,
  Secret `dash-internal-tls`). `deploy/k8s-tls/certificate.example.yaml` is a
  cert-manager `Certificate` producing that Secret from a private CA issuer.
* **Compose**: `scripts/generate-dev-tls.sh` writes a development CA and
  certificate to `deploy/container/tls`; add
  `-f deploy/container/docker-compose.tls.yml`. Use your own PKI outside
  development.
* **systemd**: uncomment the TLS block in `deploy/systemd/*.env.example`.
  Keep the files under `/etc/dash/tls` (directory `0750 root:dash`, keys
  `0640 root:dash`); `ProtectSystem=strict` leaves `/etc` readable.

### Verifying

```bash
curl --cacert ca.crt https://ingestion.internal:8081/live
curl --cacert ca.crt --cert follower.crt --key follower.key \
  -H "x-replication-token: $TOKEN" \
  "https://ingestion.internal:8081/internal/replication/wal?from_offset=0&max_records=1"
openssl s_client -connect ingestion.internal:8081 -CAfile ca.crt -alpn http/1.1 </dev/null
curl http://ingestion.internal:8081/live    # 400: this port requires TLS
```

## Not covered

* No certificate revocation checking (CRL or OCSP) for client or server
  certificates; revoke a follower by removing its fingerprint from
  `DASH_INGEST_REPLICATION_ALLOWED_CLIENT_CERTS` or by reissuing the client
  CA. Use short-lived certificates.
* One certificate per listener (no SNI-based selection).
* Client identity is the certificate fingerprint; subject names and SANs of
  client certificates are not parsed or matched.
* Handshake cryptography runs on the single accept thread: a flood of
  handshakes is bounded by the per-IP cap, the pending bound and the
  first-byte timeout, but it competes with accepting new connections. Rate
  limit handshakes upstream if that matters.
* The follower trusts the bundled Mozilla roots, not the operating system
  store; pass private or corporate CAs with `DASH_REPLICATION_CA_FILE`.
* HTTP/2 is not supported.
