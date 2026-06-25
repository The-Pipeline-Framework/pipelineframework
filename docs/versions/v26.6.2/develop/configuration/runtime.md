---
title: Runtime Settings
search: false
---

# Runtime Settings

Runtime settings control how a generated application starts, connects, stores, observes, and responds to work.

## Main Surfaces

| Surface | Examples | Full reference |
| --- | --- | --- |
| Generated orchestrator clients | downstream service locations, client wiring | [Orchestrator Client Wiring](/versions/v26.6.2/develop/configuration/all-settings#orchestrator-client-wiring-generated) |
| REST client endpoints | generated REST client base URLs | [REST Client Endpoints](/versions/v26.6.2/develop/configuration/all-settings#rest-client-endpoints) |
| Pipeline execution | synchronous/background execution mode and worker behavior | [Pipeline Execution](/versions/v26.6.2/develop/configuration/all-settings#pipeline-execution) |
| Telemetry | generated telemetry export and runtime telemetry behavior | [Telemetry](/versions/v26.6.2/develop/configuration/all-settings#telemetry) |
| In-flight probe | operator-facing pause/kill switch | [In-flight Probe](/versions/v26.6.2/develop/configuration/all-settings#in-flight-probe-kill-switch) |
| Health checks | startup/runtime health validation | [Startup Health Checks](/versions/v26.6.2/develop/configuration/all-settings#startup-health-checks) |
| Global defaults | defaults shared across generated components | [Global Defaults](/versions/v26.6.2/develop/configuration/all-settings#global-defaults) |

## Design Guidance

Runtime settings should not change the business contract. They should change deployment behavior around an already-generated pipeline:

- where adapters call,
- which providers are selected,
- how work is scheduled,
- how failures are retried or surfaced,
- how telemetry and health are exposed.

