# Replay And Live Topology

TPF exposes three different observability surfaces:

1. **Prometheus/Grafana metrics** for live throughput, latency, inflight, queue pressure, and retry pressure
2. **Tempo tracing** for live topology, trace drill-down, and span relationships
3. **Replay viewer** for deterministic post-run playback

They answer different questions. Do not treat them as interchangeable.

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

Replay JSON is the supported offline input for the TPF replay viewer.

## Live versus offline

### Live

- OTLP trace/span export into Tempo or another tracing backend
- Prometheus metric scraping
- Grafana dashboards and Explore views backed by Tempo or Prometheus

### Offline

- replay JSON written by the framework replay exporter

Replay JSON is not a collector input and it is not a live transport. It is a deterministic playback artifact.

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

Replay JSON is written from the same runtime semantics by the framework replay exporter.

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

Compatibility entrypoint:

- `/replay-viewer/index.html`

It ships with built-in datasets and also accepts imported replay JSON files generated from your own runs.

The viewer exposes:

- `CSV Payments built-in`
- `Search built-in pre-warm`
- `Search built-in`
- `Custom replay`

The viewer uses a video-first layout:

- the replay canvas as the main player surface
- a persistent shell link back to `/guide/operations/observability/replay`
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
