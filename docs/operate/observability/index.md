# Observability Overview

Observability in The Pipeline Framework is designed for distributed pipelines: you should be able to see what each step did, how long it took, and where failures occurred.

## Signal Ownership And Effective Instrumentation

Three independent layers decide what you can observe. Keep them separate when diagnosing a missing signal:

| Layer | Question | Owner |
|---|---|---|
| Build capability | Was this Quarkus artifact built with the signal capability? | The deployable application's Quarkus extensions and build-time settings |
| TPF telemetry policy | Should framework instrumentation emit this signal? | `pipeline.telemetry.*` |
| Export and runtime | Where should an enabled signal go, and is a backend available? | Deployment Quarkus, OpenTelemetry, Micrometer, and backend configuration |

For metrics and tracing, effective framework instrumentation is the intersection of the
artifact capability and TPF policy. An OpenTelemetry API dependency alone does not make an
artifact tracing- or metrics-capable, and enabling a TPF policy cannot add a signal that was
disabled during Quarkus augmentation. Conversely, an enabled signal with no exporter is a
valid non-failing configuration: exporter routing and backend health remain deployment-owned.

TPF uses metrics for low-cardinality operational aggregates. Execution, interaction,
correlation, and request identities belong in traces, replay, or logs rather than metric labels.

## What You Get Out of the Box

- [Metrics](/operate/observability/metrics): Step timings, throughput, and failure counts
- [Tracing](/operate/observability/tracing): End-to-end request visibility across steps
- [Replay & Live Topology](/operate/observability/replay): Separate the offline replay viewer from live Tempo and Prometheus surfaces
- [Logging](/operate/observability/logging): Structured logs with correlation identifiers
- [Health Checks](/operate/observability/health-checks) and [In-flight Probe](/operate/in-flight-probe): Liveness, readiness and killswitch for orchestration
- [Alerting](/operate/observability/alerting): Dashboards and alert rules tuned for pipeline behavior
- [Security Notes](/operate/observability/security): Prevent accidental leakage of sensitive information
- [Working with NewRelic OTel](/operate/observability/newrelic): Enabling OTel export to use NewRelic
- [Test locally using LGTM](/operate/observability/lgtm): Enabling Prometheus metrics for Grafana dashboards on Quarkus LGTM stack

Managed external boundaries appear as first-class nodes. Await steps expose suspend/resume lifecycle events. Command steps appear as command nodes in replay topology and participate in normal step spans and metrics while their effect lifecycle is recorded by the command effect store.
