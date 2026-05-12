# TPF Replay Viewer

Standalone Three.js viewer for framework replay JSON.

This is the canonical source for the replay viewer. The docs site publishes it at `/replay-viewer/index.html` by copying this app into `docs/public/replay-viewer/` during the docs build.

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
- replay metadata in a strip below the player
- player chrome over the viewport for transport, scrubber, inline speed radios, and utility icons
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

## Built-in datasets

The viewer ships with:

- `CSV Payments built-in`
- `Search built-in pre-warm`
- `Search built-in`
- `Custom replay`

These are curated viewer datasets. They are not the source of truth for replay semantics.

## Source layout

- application source: `tools/replay-viewer/`
- published docs copy: `docs/public/replay-viewer/`
- vendored Three.js runtime: `tools/replay-viewer/vendor/`
