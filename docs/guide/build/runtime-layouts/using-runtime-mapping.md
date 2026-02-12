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

## FUNCTION Invocation Contract Baseline

For `pipeline.platform=FUNCTION` (serverless/function runtime target; see [Configuration Reference](/guide/build/configuration/)), runtime mapping and function transport contract metadata work together:

- runtime mapping still decides placement (`orchestrator`, step modules, plugin/synthetic modules)
- function invocation metadata in `FunctionTransportContext` expresses invocation intent. `FunctionTransportContext` is the per-invocation metadata object (invocation id, caller/stage, attributes, routing intent) passed through the function transport path:
  - `tpf.function.invocation.mode=LOCAL|REMOTE`
  - optional target metadata: runtime/module/handler

Concrete example:

```yaml
# Runtime mapping placement (annotation-processor input)
enabled: true
layout: pipeline-runtime
validation: auto

modules:
  orchestrator-svc:
    runtime: orchestrator
  index-document-svc:
    runtime: pipeline
```

Example invocation attributes (carried by `FunctionTransportContext`, not parsed by `pipeline.runtime.yaml` loader):

```java
// LOCAL: default generated wiring
Map<String, String> localAttrs = Map.of(
    "tpf.function.invocation.mode", "LOCAL");

// REMOTE: adapter-routed invocation metadata
Map<String, String> remoteAttrs = Map.of(
    "tpf.function.invocation.mode", "REMOTE",
    "tpf.function.target.runtime", "pipeline",
    "tpf.function.target.module", "index-document-svc",
    "tpf.function.target.handler", "ProcessIndexDocumentFunctionHandler");
```

The runtime reads these values via `FunctionTransportContext` accessors (`invocationMode()`, `targetRuntime()`, `targetModule()`, `targetHandler()`), while the runtime-mapping loader ignores them.
The default behaviour is local (`LOCAL`) generated wiring, while stable contract fields are preserved for cross-runtime remote adapters.
