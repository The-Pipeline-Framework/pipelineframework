---
search: false
---

# Observability Overview

Observability in The Pipeline Framework is designed for distributed pipelines: you should be able to see what each step did, how long it took, and where failures occurred.

## What You Get Out of the Box

- [Metrics](/versions/v26.2.5/guide/operations/observability/metrics): Step timings, throughput, and failure counts
- [Tracing](/versions/v26.2.5/guide/operations/observability/tracing): End-to-end request visibility across steps
- [Logging](/versions/v26.2.5/guide/operations/observability/logging): Structured logs with correlation identifiers
- [Health Checks](/versions/v26.2.5/guide/operations/observability/health-checks) and [In-flight Probe](/versions/v26.2.5/guide/operations/in-flight-probe): Liveness, readiness and killswitch for orchestration
- [Alerting](/versions/v26.2.5/guide/operations/observability/alerting): Dashboards and alert rules tuned for pipeline behavior
- [Security Notes](/versions/v26.2.5/guide/operations/observability/security): Prevent accidental leakage of sensitive information
- [Working with NewRelic OTel](/versions/v26.2.5/guide/operations/observability/newrelic): Enabling OTel export to use NewRelic
- [Test locally using LGTM](/versions/v26.2.5/guide/operations/observability/lgtm): Enabling Prometheus metrics for Grafana dashboards on Quarkus LGTM stack
