# TPF Replay Viewer

Standalone Three.js viewer for framework replay JSON.

The canonical replay viewer source lives in `tools/replay-viewer/`. The docs site exposes `/replay-viewer/` as a VitePress route and hosts the standalone viewer assets from `docs/public/replay-viewer-app/` during the docs build.

## Supported input

The viewer expects a replay document emitted by the framework replay exporter.

Replay JSON already embeds:

- generated replay topology
- ordered replay events
- a curated runtime `runParameters` snapshot when exported by current framework versions

So the viewer only needs one file at import time.

## What it renders

- primary pipeline steps
- plugin nodes
- transition edges
- replay metadata in the info modal
- hover/tap player chrome over the viewport for transport, scrubber, replay title, inline speed radios, and utility icons
- a persistent shell link back to the replay docs page outside the player chrome
- modal source and info surfaces opened from the bottom-right utility icons
- semantic effects for:
  - `start`
  - `emit`
  - `retry`
  - `error`
  - `success`
  - `cache_hit`
  - `reject`
  - Await interaction, admission, durable fallback, and resume lifecycle events
  - object-ingest and object-publication lifecycle events

The `runParameters` telemetry section records the TPF instrumentation policy captured with the run. It does not prove that the application was built with a particular Quarkus signal capability, that an exporter was configured, or that a backend was available. Use the live metrics and tracing surfaces for those questions.

## Run locally

Serve the viewer directory over HTTP:

```bash
cd tools/replay-viewer
python3 -m http.server 4173
```

Then open `http://localhost:4173`.

You can either:

1. open the replay-source icon and load one of the built-in datasets, or
2. switch the selector to `Custom replay`, choose a replay JSON file, and click `Load dataset`

The local replay file picker is shown only while `Custom replay` is selected.

If an imported replay predates `runParameters`, the viewer keeps working and shows `Run parameters unavailable`.

When the viewer is hosted from the docs site, the shell exposes a persistent `Back to docs` link to `/operate/observability/replay`. It stays visible even when the player chrome is hidden.

## Built-in datasets

The viewer ships with:

- `CSV Payments built-in`
- `CSV Payments 1k slow provider`
- `CSV Payments 10k paged`
- `Search built-in pre-warm`
- `Search built-in`
- `Custom replay`

These are curated viewer datasets. They are not the source of truth for replay semantics.

## CSV Payments capture provenance

The original `CSV Payments built-in` fixture is a captured 1k live-path execution from before
paging. It remains the homepage-video source and preserves the deterministic `907` approved /
`93` unapproved split. Its analysis sidecar records the historical capture. Do not replace it
with a current paged run without regenerating the homepage video, poster, proof manifest, and
documentation together.

The two new compressed datasets are full replay captures from the standalone
`csv-kafka-payments` application, not sampled viewer data. The 1k capture uses one page and a
10-per-second payment provider. The 10k capture submits one file, uses ten 1,000-record pages,
and configures the same provider for 100 permits per second. Both retain all Await, source,
branch, and Object Publish events. Their analysis sidecars record the measured results and
SHA-256 hashes.

## Generate the paged 10k replay

Use JDK 21, a Docker-compatible container engine, and a coherent set of released or candidate
TPF artifacts. In the standalone `csv-kafka-payments` checkout, keep
`config/pipeline.yaml` at `paging.maxRecords: 1000` and build the LOCAL monolith. The E2E harness
derives paths from `orchestrator-svc` and runs the selected source as one user-visible input.
Always use that checkout's isolated Maven repository.
The capture disables the demo retry-amplification kill switch for this high-volume proof;
the provider rate limiter and TPF Await admission still govern demand. With Podman, point
`DOCKER_HOST` at its Docker-compatible socket and set `TESTCONTAINERS_RYUK_DISABLED=true`.
Podman can report a teardown broken pipe after the test method passes; keep that Maven run
classified as failed and inspect the test report separately.

```bash
APP_ROOT="$(git rev-parse --show-toplevel)"
./build-monolith.sh -DskipTests

JAVA_TOOL_OPTIONS='-Dpipeline.kill-switch.retry-amplification.enabled=false' \
./mvnw -f pom.xml -pl orchestrator-svc -am \
  -Dit.test=CsvPaymentsProviderRejectEndToEndIT#providerRejectsStillProduceOutputRows \
  -Dcsv.runtime.layout=monolith \
  -Dcsv.e2e.telemetry.enabled=true \
  -Dcsv.e2e.telemetry.happy-path-only=false \
  -Dcsv.e2e.input.file=../input-csv-file-processing-svc/csv/payments_10k.csv \
  -Dcsv-payments.payment-provider.permits-per-second=100.0 \
  -Dcsv-payments.payment-provider.timeout-millis=60000 \
  -Dcsv-payments.payment-provider.provider-reject-probability=0.08 \
  -Dcsv.e2e.pipeline.wait.seconds=1200 \
  -Dcsv.e2e.orchestrator.wait.seconds=1200 \
  -Dmaven.repo.local="$APP_ROOT/.m2/repository" \
  verify
```

The merged replay is written to
`orchestrator-svc/target/test-e2e/replay/csv-payments-replay.json`. The test checks exactly
10,000 output rows, the provider branch split, and live Await progress. Copy or compress this
file before another capture: the harness clears its replay directory at the start of each run.
For the paired slow 1k capture, use the same command with `payments_1k.csv` and
`permits-per-second=10.0`. The provider permit timeout remains 60 seconds so queued requests
wait for permits instead of producing artificial timeout failures.

Before promoting the 10k capture, verify that it is completed and has all of the following:

- 10 source starts and completions, 10,000 source emits, and no `PagedSourceStepAdapter` events;
- 10,000 `await_interaction_dispatched`, `await_admission_acquired`, and
  `await_admission_released` events;
- all deferred-completion lifecycle events use `ProcessCsvPaymentsInput`, and no replay event or
  topology transition references the removed `AwaitPaymentProvider` step;
- exactly 9,170 `ProcessApprovedPaymentStatus` and 830 `ProcessUnapprovedPaymentStatus` starts,
  with downstream status processing beginning before the final `ProcessCsvPaymentsInput` `emit`;
- no `await_unit_dispatch_complete`, `await_execution_waiting`,
  `await_unit_item_completed`, `await_unit_completed`, or `await_resume_released` events.

Compress each complete capture with `gzip -n` into its canonical dataset file before running
the next capture. For example, from the application checkout after the 10k run:

```bash
APP_ROOT="$(git rev-parse --show-toplevel)"
TPF_ROOT="$(dirname "$APP_ROOT")/pipelineframework"
gzip -n -c orchestrator-svc/target/test-e2e/replay/csv-payments-replay.json \
  > "$TPF_ROOT/tools/replay-viewer/datasets/csv-payments-10k-paged.json.gz"
```

Use `csv-payments-1k-slow.json.gz` for the paired 1k run. Update each analysis sidecar from
measured counts and times, then validate and sync the viewer:

```bash
cd /path/to/pipelineframework
npm --prefix docs run sync-replay-viewer
npm --prefix docs run check-replay-datasets
npm --prefix docs run build
```

The docs sync copies canonical datasets and sidecars to `docs/public/replay-viewer-app/`; do not
edit that copy directly. These monolith captures prove paging, source backpressure, and local
final-object composition. They do not establish remote gRPC/REST paged-source support or
mid-transition takeover. Treat their timing as proof of these runs, not a performance promise.

## Source layout

- application source: `tools/replay-viewer/`
- public usage README source: `tools/replay-viewer/public-README.md`
- published docs copy: `docs/public/replay-viewer-app/`
- vendored Three.js runtime: `tools/replay-viewer/vendor/`
