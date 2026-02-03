# Schema and Semantics

The runtime mapping file is optional and opt-in. If it is absent, the current scaffold defaults apply unchanged.

Suggested file name: `pipeline.runtime.yaml`

## Top-level schema

```yaml
version: 1

# Architectural layout for the pipeline runtime.
# - modular: existing multi-module behavior
# - pipeline-runtime: one runtime per pipeline (orchestrator remains separate)
# - monolith: single runtime + single module containing all steps and plugins
# Default: modular
layout: modular | pipeline-runtime | monolith

# Validation strictness for mapping completeness.
# - auto: allow partial mapping and use defaults
# - strict: require explicit placement for all steps/synthetics
# Default: auto
validation: auto | strict

defaults:
  # Default runtime name used when mapping does not specify one.
  runtime: local

  # Default module placement for regular steps.
  # - per-step: one module per step (current default)
  # - shared: place all steps in a single shared module
  # - <moduleName>: explicit module name
  module: per-step | shared | <moduleName>

  # Default placement for synthetic steps (aspects).
  synthetic:
    # - plugin: keep synthetic steps in existing plugin modules
    # - per-step: align synthetic to step module
    # - <moduleName>: explicit module name
    module: plugin | per-step | <moduleName>

runtimes:
  <runtimeName>:
    description: "optional"

modules:
  <moduleName>:
    runtime: <runtimeName>

steps:
  <stepId>:
    module: <moduleName>

synthetics:
  <syntheticId>:
    module: <moduleName>
```

## Default behavior

- If `pipeline.runtime.yaml` is missing, current scaffold defaults apply.
- If `validation: auto`, unmapped steps and synthetics fall back to defaults.
- If `validation: strict`, all steps and synthetics must be mapped.
- If `defaults.module: shared`, declare exactly one module (the shared module), or use an explicit module name instead.
- Transport is configured globally in `pipeline.yaml` and applies to all runtimes in the pipeline.

## Layout behavior

If you are unsure, pick **pipeline-runtime** first. It gives you fewer deployables without fully collapsing isolation, and it keeps the orchestrator in its own runtime.

### Choosing a layout

- **modular** is safest when you want strict isolation between services or teams.
- **pipeline-runtime** is a good default when you want fewer containers but still keep the orchestrator isolated.
- **monolith** is the simplest operationally when you value single-deploy artifacts over isolation.

### Decision matrix

| Goal | Recommended layout |
| --- | --- |
| Independent deploys per service | modular |
| Fewer containers, same pipeline cadence | pipeline-runtime |
| Single deployable, the lowest ops overhead | monolith |
| Uncertain, want low risk | pipeline-runtime |

### Trade-offs at a glance

- **modular**: best isolation and ownership boundaries; highest deploy/test overhead.
- **pipeline-runtime**: good ops simplification without full collapse; limited blast radius.
- **monolith**: simplest operations and lowest container count; widest blast radius.

### modular

- Multiple runtimes and modules are allowed.
- Placement is resolved using explicit mappings and defaults.

### pipeline-runtime

- One runtime per pipeline (orchestrator remains separate).
- All steps in a pipeline must resolve to modules that share the same runtime.
- Synthetics default to the pipeline module (or plugin grouping if configured).
- For multiple pipelines, use explicit step mappings per pipeline or define distinct modules per pipeline.
  This is the "fewer containers" compromise: one runtime per pipeline, but no sprawling service fleet.
  It also keeps a minimal client/server boundary, so if you later split services you already have
  networking, routing, and exposure patterns in place (often with only the orchestrator exposed).

### monolith

- The pipeline is compiled as a single runtime and single module.
- All steps, synthetics, and the orchestrator are placed into the monolith module.
- In `validation: auto`, unmapped steps/synthetics fall back to the monolith module.
- Explicit placements are allowed but must all target the monolith module.
  This is the "everything in one place" option. You trade isolation for operational simplicity.
  Orchestrator calls are in-process (no REST/gRPC hop), which reduces latency but increases blast radius.

### Native image note

Native images still use a managed heap and GC (SubstrateVM). Baseline memory is usually lower than the JVM, but it is not heapless, and native/off-heap memory is still in play. Compile time and build complexity are the main trade-offs.

Recommended monolith defaults:

```yaml
layout: monolith
validation: auto
runtimes:
  monolith: {}
modules:
  monolith:
    runtime: monolith
```

## Pipeline-runtime preset example

```yaml
version: 1
layout: pipeline-runtime
validation: auto

defaults:
  module: payments-pipeline
  synthetic:
    module: per-step

runtimes:
  payments: {}
  orchestrator: {}

modules:
  payments-pipeline:
    runtime: payments
  orchestrator:
    runtime: orchestrator
```

## Minimal config examples

### Keep current defaults

```yaml
version: 1
validation: auto
```

### Monolith mode

```yaml
version: 1
layout: monolith
validation: auto
runtimes:
  monolith: {}
modules:
  monolith:
    runtime: monolith
```
