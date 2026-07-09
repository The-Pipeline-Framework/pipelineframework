---
search: false
---

# Replay And Live Topology

TPF exposes three different observability surfaces:

1. **Prometheus/Grafana metrics** for live throughput, latency, inflight, queue pressure, and retry pressure
2. **Tempo tracing** for live topology, trace drill-down, and span relationships
3. **Replay viewer** for deterministic post-run playback

They answer different questions. Do not treat them as interchangeable.

Metrics are the aggregate SLO layer. Replay is the high-cardinality diagnostic layer. Object keys, execution ids, await unit ids, interaction ids, correlation ids, and connector lifecycle details belong in spans/replay events; metric labels should stay limited to low-cardinality dimensions such as source, target, provider, step, status, and transport.

## Replay artifact

The offline artifact is **replay JSON**.

It is emitted by the framework replay exporter and contains:

- pipeline topology from `META-INF/pipeline/replay-topology.json`
- ordered execution events
- a curated runtime configuration snapshot under `runParameters`
- lineage fields such as `itemId` and `parentItemIds`
- trace correlation fields such as `traceId`, `spanId`, and `parentSpanId`
- viewer-oriented semantics such as:
  - `start`
  - `emit`
  - `retry`
  - `error`
  - `success`
  - `cache_hit`
  - `reject`
  - `await_interaction_dispatched`
  - `await_unit_dispatch_complete`
  - `await_execution_waiting`
  - `await_unit_item_completed`
  - `await_unit_completed`
  - `await_resume_released`
  - `await_unit_terminal`

Replay JSON is the supported offline input for the TPF replay viewer.

Command steps are included in the topology as authored pipeline nodes with `renderRole: "command"` and `actorKind` set to the command name. This keeps managed external effects visible in playback even when the connector hides provider details such as endpoint, credentials, index name, or SDK configuration.

## Live versus offline

### Live

- OTLP trace/span export into Tempo or another tracing backend
- Prometheus metric scraping
- Grafana dashboards and Explore views backed by Tempo or Prometheus

### Offline

- replay JSON written by the framework replay exporter

Replay JSON is not a collector input and it is not a live transport. It is a deterministic playback artifact.

For `csv-payments`, the repo keeps those concerns in separate modular E2E lanes:

- replay lane: writes replay JSON and validates offline playback inputs
- Tempo lane: exports live spans to a dedicated LGTM stack and queries Tempo directly

## Prometheus polling versus traces

Prometheus polling only affects metrics freshness.

- **Tracing** is push-based: spans and span events are exported in real time to the configured tracing backend.
- **Metrics** are scrape-based: dashboard freshness depends on the Prometheus scrape interval and query window.

So:

- live topology comes from Tempo/tracing
- live heat and pressure come from Prometheus metrics
- post-run playback comes from replay JSON

## Framework outputs

TPF emits:

- `META-INF/pipeline/replay-topology.json`
- `tpf.pipeline.run` spans
- `tpf.step` spans
- step span events:
  - `tpf.step.start`
  - `tpf.step.emit`
  - `tpf.step.retry`
  - `tpf.step.success`
  - `tpf.step.error`
  - `tpf.step.cache_hit`
  - `tpf.step.reject`
- branch applicability replay event:
  - `skip`

Replay JSON is written from the same runtime semantics by the framework replay exporter.

Await boundaries record durable await unit and interaction events even when the live path keeps work flowing. Replay events include await unit ids, execution ids, interaction ids, step ids, unit status, and expected/completed item counts where the runtime knows them. For operations, see [Await Boundary Operations](/versions/v26.7.1/operate/await-boundaries); for the implementation model, see [Await Unit Runtime](/versions/v26.7.1/evolve/await-unit-runtime/).

Connector-first pipelines add framework-owned nodes that are not user-authored business steps. In CSV Payments, replay should show Object Ingest as source admission, `Await Payment Provider` as the external Kafka boundary, the approved or unapproved payment-status step consuming each completion, `Finalize Payment Output` as the explicit terminal merge, and Object Publish as terminal object output. The healthy live path is interleaved: parser dispatch, provider completions, branch-specific status processing, merge, and publish progress should overlap. The old folder, single `Process Payment Status`, and output-file services should only appear when the legacy file-step config is being replayed.

Telemetry impact for live itemized await:

1. await interaction and unit events still describe durable admission and recovery state;
2. step spans and object-publish events show whether completed items are flowing downstream;
3. `await_resume_released` mainly describes durable fallback release, not every live item handoff;
4. a replay that shows the await step waiting for every item before status/publish starts is showing batch-like behavior, not the intended live queue-async path.

Connector replay events include:

- `object_ingest_listed`
- `object_ingest_submitted`
- `object_ingest_duplicate`
- `object_ingest_failed`
- `object_publish_grouped`
- `object_publish_published`
- `object_publish_failed`
- `object_publish_skipped`

Use these events to debug a single object key or output object. Use `tpf.object_ingest.*`, `tpf.object_publish.*`, and `tpf.await.*` metrics to alert on aggregate health.

Branch-aware replay also uses the normal event stream:

- the replay `skip` event means the current item did not match a step's accepted type set, so TPF passed it through unchanged;
- the replay `skip` event is node-local, not a transit edge, and should be visible in the replay viewer as branch applicability rather than failure;
- terminal branch mismatches are runtime errors, not skips.

## CSV Payments Built-In Proof

The built-in CSV Payments replay is a captured proof run, not a benchmark promise. It is useful because it shows the intended connector-first shape and the timing relationship between parser dispatch, await completions, status processing, and Object Publish.

The current dataset was captured from the 1k replay lane with provider rejects enabled so both branch alternatives appear in the proof:

| Field | Value |
| --- | --- |
| Input records | `1000` |
| Payment provider permits | `250/s` |
| Provider reject probability | `0.08` |
| Replay duration | `15.621s` |
| Effective throughput | `64.0 records/s` |
| Replay events | `16008` |
| Unapproved branch items | `93` |

Treat the replay timing as boundary timing, not pure provider CPU time: it includes permit wait, Kafka transit, completion admission, and downstream branch processing overlap.

Key timing checks:

| Signal | Time from start |
| --- | --- |
| First `Process Approved Payment Status` / `Process Unapproved Payment Status` event | `2.001s` |
| Last input parser event | `6.901s` |
| Last await dispatch | `10.713s` |
| Last await completion | `15.585s` |
| Object Publish | `15.612s` - `15.621s` |

The important operational signal is the overlap: status processing starts before the parser has finished and before all await completions have arrived. That means the parser is being paced by reactive demand and the await in-flight window, not by a forced sleep. Object Publish runs at the terminal boundary after status output exists and before success is committed.

The repo proof also runs a 10k monolith lane with the same connector-first path. In the captured run, 10k records completed in `80s` of pipeline time with 10k output records and output checksum `cf8996e7d593202dd6f9f9405b82e95e69d48544c3edec830f64afabc703a023`.

Command telemetry has two layers:

1. the pipeline layer, where the command appears in `tpf.step` spans, step metrics, and replay topology like other authored steps;
2. the effect layer, where `tpf.command.effect.*` metrics and the command effect store record pending, dispatching, succeeded, retryable failure, duplicate handling, or terminal DLQ state.

That split is intentional. The step span shows where the pipeline spent time. Command effect metrics support dashboards and SLOs. The effect record preserves command-id detail for investigation and replay. Use provider telemetry for external backlog and provider-side latency.

Example command replay checks:

- The topology contains `renderRole: "command"` for the command step.
- `actorKind` is the authored command name, such as `opensearch-index-document`.
- For `RETURN_RECORDED`, a replayed duplicate should return the recorded output and should not require another provider write.

Example branch-routing replay checks:

- branch-specific steps show `skip` events for non-applicable alternatives instead of synthetic no-op business executions;
- only the matching branch step shows normal `start`/`success` item flow for a given item;
- the terminal merge step receives only valid branch-end types.

## Replay exporter configuration

Replay export is gated behind framework configuration:

```properties
pipeline.telemetry.enabled=true
pipeline.telemetry.tracing.enabled=true
pipeline.telemetry.tracing.per-item=true
pipeline.telemetry.replay.enabled=true
pipeline.telemetry.replay.exporter=file
pipeline.telemetry.replay.file.path=/absolute/path/to/replay.json
```

If any prerequisite is missing, replay export stays off and the runtime logs one warning instead of partially activating replay work on the hot path.

## Replay viewer

The supported replay viewer is published from the docs site at:

- `/replay-viewer/`

It ships with built-in datasets and also accepts imported replay JSON files generated from your own runs.

The viewer exposes:

- `CSV Payments built-in`
- `Search built-in pre-warm`
- `Search built-in`
- `Custom replay`

The viewer uses a video-first layout:

- the replay canvas as the main player surface
- a persistent shell link back to `/operate/observability/replay`
- a metadata strip below the player for dataset, pipeline, duration, topology, and event summary
- hover or tap-revealed player chrome over the canvas for transport controls, the scrubber, and inline speed radios
- bottom-right utility icons for replay source, replay info, and player fullscreen
- modal panes for source selection/import and `Run parameters` plus `Legend`

`Custom replay` is only exposed inside the replay-source modal. Selecting a source there stages the choice, and the viewer only switches when `Load dataset` is pressed.

Closing the modal or re-entering the page from browser history resets staged source state back to the active replay. Older replay files without `runParameters` remain loadable and show `Run parameters unavailable`.

The canonical viewer source lives in:

- `tools/replay-viewer/`

## Example-specific generation

Example commands and artifact locations are intentionally kept out of this page.

Use the example READMEs for concrete generation flows:

- `examples/csv-payments/README.md`
- `examples/search/README.md`

## LGTM / Grafana discovery

LGTM Dev Services discovers dashboards stored under `META-INF/grafana/` with the `grafana-dashboard-*.json` naming convention.

That discovery rule applies to example dashboards, not to replay JSON. Replay is a framework capability and the viewer is a separate docs-owned surface.
