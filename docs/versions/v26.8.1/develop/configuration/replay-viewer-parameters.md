---
search: false
---

# Replay Viewer Parameter Snapshot

When replay export is enabled, the replay JSON embeds a curated snapshot of execution-affecting runtime parameters from the actual run.

The replay viewer surfaces that subset in its `Run parameters` pane. The initial scope includes:

- execution knobs such as `pipeline.parallelism` and `pipeline.max-concurrency`
- step default and step override retry/backpressure settings
- cache provider/policy settings
- telemetry and replay-export toggles
- retry-amplification guardrail settings
- item-reject sink provider

The full key reference still lives in the build configuration reference. Replay export does not mirror arbitrary application configuration or secrets into the artifact.
