# TPF Replay Viewer

Standalone Three.js viewer for framework replay JSON.

The canonical replay viewer source lives in `tools/replay-viewer/`. The docs site publishes the app at `/replay-viewer/` by copying these files into `docs/public/replay-viewer/` during the docs build. `/replay-viewer/index.html` stays valid as a compatibility entrypoint.

## Supported input

The viewer expects a replay document emitted by the framework replay exporter.

Replay JSON already embeds:

- generated replay topology
- ordered replay events
- a curated runtime `runParameters` snapshot when exported by current framework versions

So the viewer only needs one file at import time.

## What it renders

- primary pipeline steps
- explicit await / broker / external-provider / DB actors when replay topology provides them
- plugin nodes as a fallback for legacy or generic replay topologies
- transition edges
- replay metadata in a strip below the player
- player chrome over the viewport for transport, scrubber, inline speed radios, and utility icons
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

When the viewer is hosted from the docs site, the shell exposes a persistent `Back to docs` link to `/guide/operations/observability/replay`. It stays visible even when the player chrome is hidden.

## Built-in datasets

The viewer ships with:

- `CSV Payments built-in`
- `Search built-in pre-warm`
- `Search built-in`
- `Custom replay`

These are curated viewer datasets. They are not the source of truth for replay semantics.

On the await/Kafka `csv-payments` branch, the built-in CSV dataset is expected to surface:

- the main pipeline chain
- `Await Payment Provider`
- the Kafka broker boundary
- the external mock provider
- the shared DB actor for persistence activity

## Source layout

- application source: `tools/replay-viewer/`
- published docs copy: `docs/public/replay-viewer/`
- vendored Three.js runtime: `tools/replay-viewer/vendor/`
