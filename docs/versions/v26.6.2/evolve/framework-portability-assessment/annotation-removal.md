---
search: false
---

# Annotation Removal

Current posture: YAML authority is rising, but annotations remain part of extraction.

| Annotation | Current role | Portability stance |
| --- | --- | --- |
| `@PipelineStep` | Build-time marker and metadata source | Make optional; migrate metadata to YAML or neutral descriptors |
| `@PipelineOrchestrator` | Orchestrator endpoint and generation trigger | Replace with YAML orchestrator declaration |
| `@PipelinePlugin` | Plugin host marker | Replace with service descriptor metadata |
| `@GeneratedRole` | Internal generated marker | Keep internally |
| `@ParallelismHint` | Runtime/compiler hint | Keep as neutral internal metadata or descriptor fields |

Suggested migration path:

1. Keep annotations supported.
2. Make YAML service declarations authoritative.
3. Validate service signatures directly from YAML class names.
4. Warn on annotation/YAML conflicts.
5. Move operational metadata (cache keys, ordering, virtual thread hints, side-effects) to YAML.
6. End with annotations optional, then remove Quarkus-only public annotation coupling.
