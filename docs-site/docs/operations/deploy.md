# Deploy

DASH is shipped as two Linux binaries (`dash-ingestion` and `dash-retrieval`) inside a single container image. The image is multi-arch (`linux/amd64`, `linux/arm64`) and runs as a non-root user with a healthcheck. This page describes the four supported deployment surfaces: Docker, docker-compose, Kubernetes (raw YAML), and Helm.

## Docker

The image is published to `ghcr.io/bhaweshbhaskar/dash` with the tag of the release.

```bash
# Pull a specific version
docker pull ghcr.io/bhaweshbhaskar/dash:0.1.0

# Run the ingestion service
docker run -d \
  --name dash-ingest \
  -p 8081:8081 \
  -e DASH_INGEST_PORT=8081 \
  -e DASH_INGEST_PERSISTENCE_PATH=/var/lib/dash/ingest.redb \
  -e DASH_INGEST_WAL_PATH=/var/lib/dash/ingest.wal \
  -v dash-data:/var/lib/dash \
  ghcr.io/bhaweshbhaskar/dash:0.1.0 \
  /usr/local/bin/dash-ingestion

# Run the retrieval service
docker run -d \
  --name dash-retrieval \
  -p 8080:8080 \
  -e DASH_RETRIEVAL_PORT=8080 \
  -e DASH_EMBEDDING_PROVIDER=hash \
  -e DASH_RETRIEVAL_PERSISTENCE_PATH=/var/lib/dash/retrieval.redb \
  -v dash-data:/var/lib/dash \
  ghcr.io/bhaweshbhaskar/dash:0.1.0 \
  /usr/local/bin/dash-retrieval
```

The image's default `ENTRYPOINT` is a small init script that selects the binary from the `DASH_SERVICE` env var. Setting `DASH_SERVICE=ingestion` (default) or `DASH_SERVICE=retrieval` picks the right one.

## docker-compose

The canonical compose file is at `deploy/container/docker-compose.yml`. It brings both services up on a shared bridge network, with a healthcheck-gated dependency ordering.

```yaml
# deploy/container/docker-compose.yml
services:
  ingestion:
    image: ghcr.io/bhaweshbhaskar/dash:0.1.0
    environment:
      DASH_INGEST_PORT: 8081
      DASH_INGEST_PERSISTENCE_PATH: /var/lib/dash/ingest.redb
      DASH_INGEST_WAL_PATH: /var/lib/dash/ingest.wal
    ports:
      - "8081:8081"
    volumes:
      - dash-data:/var/lib/dash
    healthcheck:
      test: ["CMD", "/usr/local/bin/healthcheck.sh", "ingestion"]
      interval: 10s
      timeout: 3s
      retries: 3

  retrieval:
    image: ghcr.io/bhaweshbhaskar/dash:0.1.0
    depends_on:
      ingestion:
        condition: service_healthy
    environment:
      DASH_RETRIEVAL_PORT: 8080
      DASH_EMBEDDING_PROVIDER: hash
      DASH_RETRIEVAL_PERSISTENCE_PATH: /var/lib/dash/retrieval.redb
    ports:
      - "8080:8080"
    volumes:
      - dash-data:/var/lib/dash
    healthcheck:
      test: ["CMD", "/usr/local/bin/healthcheck.sh", "retrieval"]
      interval: 10s
      timeout: 3s
      retries: 3

volumes:
  dash-data:
```

A dev overlay with hot-reload is at `deploy/container/docker-compose.dev.yml`. Run it with:

```bash
docker compose -f deploy/container/docker-compose.yml \
               -f deploy/container/docker-compose.dev.yml \
               up
```

## Kubernetes (raw YAML)

A minimal raw-YAML deployment is at `deploy/k8s/`. The shape:

```yaml
# deploy/k8s/dash-ingestion.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: dash-ingestion
spec:
  serviceName: dash-ingestion
  replicas: 2
  selector:
    matchLabels:
      app: dash-ingestion
  template:
    metadata:
      labels:
        app: dash-ingestion
    spec:
      containers:
        - name: ingestion
          image: ghcr.io/bhaweshbhaskar/dash:0.1.0
          env:
            - name: DASH_INGEST_PORT
              value: "8081"
            - name: DASH_INGEST_PERSISTENCE_PATH
              value: /var/lib/dash/ingest.redb
            - name: DASH_INGEST_WAL_PATH
              value: /var/lib/dash/ingest.wal
          ports:
            - containerPort: 8081
          readinessProbe:
            httpGet:
              path: /v1/health
              port: 8081
            initialDelaySeconds: 5
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /v1/health
              port: 8081
            initialDelaySeconds: 15
            periodSeconds: 30
          volumeMounts:
            - name: dash-data
              mountPath: /var/lib/dash
  volumeClaimTemplates:
    - metadata:
        name: dash-data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 100Gi
```

The retrieval service is similar, with `DASH_RETRIEVAL_PORT=8080` and a `Service` of `type: LoadBalancer` (or an `Ingress` if the cluster supports it) on port 80 → 8080.

A `NetworkPolicy` that allows only the retrieval service to talk to the ingestion service is recommended in multi-tenant clusters.

## Helm

A Helm chart is at `deploy/helm/dash/`. It is a thin wrapper over the raw YAML, parameterized for image tag, replica counts, persistence, and an `Ingress` for the retrieval service.

```bash
helm repo add dash https://BHAWESHBHASKAR.github.io/dash-charts
helm install dash dash/dash \
  --namespace dash --create-namespace \
  --set image.tag=0.1.0 \
  --set retrieval.replicas=4 \
  --set persistence.size=200Gi \
  --set ingress.enabled=true \
  --set ingress.host=dash.example.com
```

The chart's `values.yaml` is the source of truth for all the knobs. The non-obvious ones:

- `auth.jwtPublicKey` — the PEM-encoded public key for JWT verification. Required.
- `embedding.provider` — one of `hash`, `ollama`, `openai`. Required.
- `embedding.ollama.baseUrl` — required if `provider=ollama`.
- `embedding.openai.apiKey` — required if `provider=openai` (consider using a `Secret` and `valueFrom`).
- `rateLimit.rps` and `rateLimit.burst` — per-tenant, applied to the retrieval service.
- `audit.retentionDays` — default 2555 (7 years).

A `helm template` against the default `values.yaml` produces a complete set of manifests that can be `kubectl apply -f`-ed.
