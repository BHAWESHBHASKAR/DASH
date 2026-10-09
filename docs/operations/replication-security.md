# Replication transport security

`/internal/replication/*` on the ingestion service streams the full WAL and
snapshot of every tenant to followers, and followers prove themselves with a
shared token (`x-replication-token`). This page says what protects that
channel today, what does not, and how to close the gap.

## What the code does

* **Authentication.** The endpoints answer 403 unless the token matches
  (constant-time compare); an ingestion follower refuses to start without a token outside
  dev mode. See `DASH_INGEST_REPLICATION_TOKEN`.
* **The std server does not terminate TLS.** The ingestion listener speaks
  plain HTTP only. There is no TLS option on it and none is planned in the
  server itself.
* **Followers speak `http://` and `https://`.** Both followers (retrieval,
  and an ingestion node following another one) use the same client
  (`services/common/src/replication_client.rs`, `ureq` with `rustls`). An
  `https://` source URL is verified against the public web roots plus an
  optional PEM bundle in `DASH_REPLICATION_CA_FILE` (use it for a private or
  mesh CA). Redirects are never followed, so the token cannot be forwarded to
  another host, and response bodies are read through a hard size cap.
* **The token is not sent over plaintext to other machines.** A follower
  whose source URL is `http://` to a non-loopback host refuses to send the
  token and every poll fails with a message naming
  `DASH_REPLICATION_ALLOW_INSECURE_HTTP`; an ingestion follower refuses to
  start. Setting `DASH_REPLICATION_ALLOW_INSECURE_HTTP=1` acknowledges the
  exposure and allows it. Loopback (`localhost`, `127.0.0.0/8`, `::1`) and
  `https://` are always allowed.
* **Startup warnings.** A follower logs a warning when its source is
  `http://` to a non-loopback host (even with the acknowledgement set). A
  leader logs a warning when a replication token is configured while its
  listener is bound to a non-loopback address.

The shipped Compose, Kubernetes and Helm configuration sets
`DASH_REPLICATION_ALLOW_INSECURE_HTTP=1` for the retrieval follower, because
their source URL is plain `http://` on the cluster or compose network. That is
an explicit acknowledgement, not protection.

## Deployment options

Pick one and make the retrieval/ingestion follower talk to it with an
`https://` source URL.

1. **Service mesh with mTLS** (Istio, Linkerd, Consul). The sidecars encrypt
   and authenticate pod-to-pod traffic transparently, so the application URL
   can stay `http://` (keep the acknowledgement variable set) while the wire
   is encrypted. Enforce strict mTLS mode for the namespace and add an
   authorization policy that only lets the retrieval workload identity reach
   the ingestion port.
2. **TLS-terminating sidecar or ingress in front of ingestion.** Run
   nginx, Envoy or stunnel next to the ingestion container: it listens on a
   TLS port, forwards to `127.0.0.1:8081`, and ingestion stays bound to
   loopback. Set `DASH_RETRIEVAL_REPLICATION_SOURCE_URL=https://<host>:<tls
   port>` and, for a private CA, `DASH_REPLICATION_CA_FILE`. Do not route
   `/internal/*` through a public ingress.
3. **NetworkPolicy / firewall only.** `deploy/k8s/50-networkpolicy.yaml`
   restricts which pods may reach the ingestion port (default deny, then
   retrieval to ingestion). It limits who can connect; it does not encrypt.
   Use it together with option 1 or 2, not instead of them, unless the network
   itself is trusted. On Compose, keep ingestion on a private network
   with no published port for replication.

## The remaining gap

* Without option 1 or 2 the token and the multi-tenant WAL stream cross the
  network in plaintext. A passive observer on that path reads all tenants'
  data; an active one can replay the token to export it.
* The token is one shared secret for all followers. There is no per-follower
  identity or revocation short of rotating it everywhere.
* The ingestion listener cannot present a certificate or require client
  certificates itself. Mutual TLS must come from the mesh or the sidecar.
* NetworkPolicy in `deploy/k8s` also allows the ingress controller namespace to
  reach the ingestion port; the Ingress resource never routes `/internal/*`,
  but another pod in that namespace could connect directly.
* `DASH_REPLICATION_ALLOW_INSECURE_HTTP=1` silences the refusal only; the
  startup warning stays.

Tests: `services/common/tests/replication_client.rs` (https against a local TLS
server with a generated certificate, untrusted certificate refused, token
refused over remote plain http, no redirects, size caps),
`services/ingestion/src/transport/authz.rs` and
`services/retrieval/src/replication.rs` (startup warnings and refusal).
