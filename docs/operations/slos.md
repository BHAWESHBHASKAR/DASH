# Service level objectives

Proposed SLIs, SLOs and error budgets for a production DASH deployment, tied to
the metrics the services export. The recording rules that compute the SLIs are
in [`deploy/observability/prometheus/dash-recording.rules.yml`](../../deploy/observability/prometheus/dash-recording.rules.yml),
the burn-rate alerts in [`dash-alerts.rules.yml`](../../deploy/observability/prometheus/dash-alerts.rules.yml)
(alert `DashErrorBudgetBurn`, [runbook](runbooks/dash-error-budget-burn.md)), and
the *DASH / Overview* dashboard has an SLO row. These are starting targets:
review them after a month of production data and adjust to what users need.

## SLIs

All request SLIs come from the server-side request metrics recorded for every
request by the shared HTTP layer (`dash_http_server_requests_total` and the
`dash_http_server_request_duration_seconds` histogram, labelled by
`component`, `route`, `method` and `code`; latency is measured from the parsed
request to the rendered response).

"API requests" are client traffic: every route except `health`, `live`,
`ready`, `metrics`, `debug` and the internal `replication_*` routes.

| SLI | Definition | Recording rule |
|---|---|---|
| Availability | Share of API requests not answered with a 5xx, per component | `1 - dash:slo_availability_errors:ratio_rate{5m,30m,1h,6h}` |
| Retrieve latency | Share of `retrieve` requests answered within 0.5 s | `dash:slo_retrieve_latency_good:ratio_rate30m` |
| Ingest latency | Share of `ingest`, `ingest_raw` and `ingest_batch` requests answered within 2.5 s (includes the WAL fsync) | `dash:slo_ingest_latency_good:ratio_rate30m` |
| Freshness | Share of time a retrieval follower is within 60 s of its leader | `avg_over_time((dash_retrieval_replication_lag_seconds <= bool 60)[30d:1m])` |

4xx answers (bad input, authentication, rate limiting) do not count against
availability: they are the client's error or an intended limit. 5xx answers
include `503` from overload, a poisoned or full WAL, fail-closed audit and an
unavailable embedding provider: from the client's point of view these are all
outages.

## SLOs and error budgets

Window: rolling 30 days.

| SLO | Target | Error budget (30 days) |
|---|---|---|
| Retrieval availability | 99.9% | 0.1% of API requests (43 minutes of full outage) |
| Ingestion availability | 99.9% | 0.1% of API requests |
| Retrieve latency | 99% within 0.5 s | 1% of retrieve requests may be slower |
| Ingest latency | 99% within 2.5 s | 1% of ingest requests may be slower |
| Freshness | 99.5% of time within 60 s | 3.6 hours of lag above 60 s per follower |
| Control plane | no SLO | Placement is read at startup and on reload; the data path keeps serving without a leader (`DashControlPlaneNoLeader` is a warning) |

Durability is not an SLO with a budget: an acknowledged write must never be
lost. The alerts `DashWalPoisoned`, `DashWalWriteFailing` and
`DashDiskUnavailable` page immediately.

## Burn-rate alerting

Multi-window, multi-burn-rate alerts on the availability SLOs (budget
`0.001`):

| Severity | Long window | Short window | Burn rate | Budget spent when it fires | Time to exhaustion |
|---|---|---|---|---|---|
| critical (page) | 1 h | 5 m | 14.4 | 2% | about 2 days |
| warning (ticket) | 6 h | 30 m | 6 | 5% | about 5 days |

The short window makes the alert stop soon after the error source is fixed.
Latency and freshness SLOs are watched through `DashHighLatencyP99` and
`DashReplicationFollowerLagging`; add burn-rate alerts for them once the
targets are validated.

## Measuring attainment

Monthly availability per component:

```promql
1 - (
  sum by (component) (increase(dash_http_server_requests_total{code=~"5..",route!~"health|live|ready|metrics|debug|replication_.*"}[30d]))
  / sum by (component) (increase(dash_http_server_requests_total{route!~"health|live|ready|metrics|debug|replication_.*"}[30d]))
)
```

Remaining error budget (1 = untouched, 0 = spent, negative = SLO missed):

```promql
1 - (
  (sum by (component) (increase(dash_http_server_requests_total{code=~"5..",route!~"health|live|ready|metrics|debug|replication_.*"}[30d]))
   / sum by (component) (increase(dash_http_server_requests_total{route!~"health|live|ready|metrics|debug|replication_.*"}[30d])))
  / 0.001
)
```

Retrieve latency attainment:

```promql
sum(increase(dash_http_server_request_duration_seconds_bucket{route="retrieve",le="0.5"}[30d]))
  / sum(increase(dash_http_server_request_duration_seconds_count{route="retrieve"}[30d]))
```

## Error budget policy

* Budget remaining: ship as usual.
* Fast burn (page): the on-call engineer mitigates first (roll back, fail over,
  shed load) and opens an incident.
* Budget exhausted for the window: releases are limited to reliability fixes
  until the budget recovers, and the top contributors (from the per-route and
  per-code breakdown) get a postmortem.

## Limits of these SLIs

* They are measured at the server. Connections refused before a request is
  read (accept-queue overload, per-IP connection cap, TLS handshake failures)
  never reach the request metrics; they are counted by
  `dash_*_transport_queue_full_reject_total` and alerted on by
  `DashRequestsShed`. Measure at the ingress or load balancer as well for the
  user's view.
* A process that is down produces no samples, so the ratio has no errors in it
  for that instance; `DashTargetDown` covers that case.
* Counters reset on restart; `increase()` and `rate()` handle the reset, but a
  crash loses the requests in flight at the moment of the crash.
