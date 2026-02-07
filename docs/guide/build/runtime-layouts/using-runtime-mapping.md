# Using Runtime Mapping

Runtime mapping is optional. If you do nothing, scaffold defaults are used.

## File and scope

- File name: `pipeline.runtime.yaml`
- Typical location in generated apps: `config/pipeline.runtime.yaml`
- Scope: whole app (resolved by annotation processing per module, applied globally)

## Minimal workflow

1. Keep the scaffold defaults working first.
2. Add runtime mapping with `validation: auto`.
3. Move to `validation: strict` after all intended mappings are explicit.

## Baseline template

```yaml
enabled: true
layout: pipeline-runtime
validation: auto

runtimes:
  orchestrator: {}
  pipeline: {}
  persistence: {}

modules:
  orchestrator-svc:
    runtime: orchestrator
  pipeline-svc:
    runtime: pipeline
  persistence-svc:
    runtime: persistence

defaults:
  module: pipeline-svc
  synthetic:
    module: persistence-svc
```

## Layout meaning

- `modular`: closest to classic one-step-per-module scaffolds.
- `pipeline-runtime`: orchestrator isolated; service steps grouped; plugins usually grouped by aspect.
- `monolith`: orchestrator + services + plugins in one runtime/module (requires matching Maven topology).

## Important for plugins/synthetics

Runtime mapping applies to regular steps and synthetic side-effect steps. If plugin aspects are enabled, ensure synthetic placement is intentional (`defaults.synthetic.module` or explicit synthetic mappings), especially when switching from modular to grouped layouts.

## Transport note

Transport remains a pipeline decision (`GRPC`, `REST`, or `LOCAL`) and is orthogonal to runtime mapping. Runtime mapping chooses *where* steps run; transport chooses *how* they are called.
