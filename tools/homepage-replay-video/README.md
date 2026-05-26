# Homepage Replay Video

This directory contains the reproducible source for the docs homepage cinematic replay feature.

The Blender path has been retired. The homepage asset is now generated from replay-derived data through a
purpose-built browser-native WebGL scene that emphasizes the async boundary, Kafka/provider handoff, and shared
persistence/store actor without reusing the replay viewer UI itself.

## Source of truth

- Replay source: [tools/replay-viewer/datasets/csv-payments-built-in.json](/Users/mari/tpf4/tools/replay-viewer/datasets/csv-payments-built-in.json)
- Simplified cinematic data: `data/csv-payments-cinematic.json`
- WebGL scene entrypoint: `render_scene.html` + `render_scene.js`
- Frame capture script: `capture_frames.cjs`
- Homepage exports:
  - [docs/public/home/replay-proof.webm](/Users/mari/tpf4/docs/public/home/replay-proof.webm)
  - [docs/public/home/replay-proof.mp4](/Users/mari/tpf4/docs/public/home/replay-proof.mp4)
  - [docs/public/home/replay-proof-poster.jpg](/Users/mari/tpf4/docs/public/home/replay-proof-poster.jpg)

## Regenerate everything

```bash
./tools/homepage-replay-video/build_homepage_replay_video.sh
```

This script:

1. derives cinematic JSON from the CSV replay,
2. renders deterministic WebGL frames through Playwright,
3. captures the poster image, and
4. encodes the homepage `webm` and `mp4` exports.

## Editing workflow

- Change replay simplification and timing windows in `prepare_replay_data.cjs`.
- Change scene composition, geometry, materials, and camera behavior in `render_scene.js`.
- Re-run `build_homepage_replay_video.sh` after either change.
