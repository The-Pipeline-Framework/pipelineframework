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
- `Search built-in pre-warm`
- `Search built-in`
- `Custom replay`

These are curated viewer datasets. They are not the source of truth for replay semantics.

## Refreshing CSV Payments built-in replay

The CSV Payments fixture is a captured 1k live-path execution, not hand-authored viewer data.
Refreshing it changes several checked-in derived surfaces. Use JDK 21 and a Docker-capable
environment.

Run the capture from `orchestrator-svc`: the E2E harness derives mounted test paths from its
working directory. Point Maven at the worktree-local repository explicitly.

```bash
REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT/examples/csv-payments/orchestrator-svc"

../../../mvnw \
  -Dit.test=CsvPaymentsEndToEndIT \
  -Dcsv.e2e.telemetry.enabled=true \
  -Dcsv.e2e.input.file=../input-csv-file-processing-svc/csv/payments_1k.csv \
  -Dmaven.repo.local="$REPO_ROOT/.m2/repository" \
  verify
```

The test rebuilds the telemetry-tagged modular service images, captures replay fragments, merges
them, and verifies 1,000 output and database records. Its merged capture is written to:

```text
examples/csv-payments/orchestrator-svc/target/test-e2e/replay/csv-payments-replay.json
```

Promote that file to `datasets/csv-payments-built-in.json`. Update
`datasets/csv-payments-built-in-analysis.json` from the capture's measured counts and times; do
not carry forward timing, branch, or durable-fallback facts from an older capture.

Before syncing derived assets, verify that the promoted replay is completed and has all of the
following:

- 1,000 `await_interaction_dispatched`, `await_admission_acquired`, and
  `await_admission_released` events;
- downstream status processing starts before the final `ProcessCsvPaymentsInput` `emit`;
- no `await_unit_dispatch_complete`, `await_execution_waiting`,
  `await_unit_item_completed`, `await_unit_completed`, or `await_resume_released` events.

Then update the current replay facts in
`docs/operate/observability/replay.md`,
`docs/deploy/concurrency-and-backpressure.md`, and
`docs/design/object-ingest.md`, and regenerate the published viewer and homepage assets:

```bash
cd "$REPO_ROOT"
npm --prefix docs run sync-replay-viewer
npm --prefix docs run check-replay-datasets
./tools/homepage-replay-video/build_homepage_replay_video.sh
```

The last command regenerates `data/csv-payments-cinematic.json`, the homepage video and poster,
and `docs/public/home/replay-proof-manifest.json`. The docs sync copies the canonical viewer
dataset and analysis sidecar to `docs/public/replay-viewer-app/`; do not edit that copy directly.
Treat the captured timing as proof of that run, not a performance promise.

## Source layout

- application source: `tools/replay-viewer/`
- published docs copy: `docs/public/replay-viewer-app/`
- vendored Three.js runtime: `tools/replay-viewer/vendor/`
