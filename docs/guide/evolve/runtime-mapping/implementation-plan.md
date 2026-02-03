# Implementation Plan (Phase 2)

This plan turns `pipeline.runtime.yaml` into build-time behavior without changing Maven module structure.
The approach is intentionally incremental: reuse existing modules and generation outputs, and let the
annotation processor filter artifacts per module.

## 1) Inputs and wiring

### New input file

- File: `pipeline.runtime.yaml` (optional)
- If missing: preserve current scaffold behavior.

### Existing build flag

- Maven modules already pass a module name: `-Apipeline.module=<moduleName>`
- This becomes the binding between logical module names in the YAML and actual Maven modules.

## 2) Runtime mapping resolver

Add a small resolver component that runs before generation:

- Parse YAML and build a mapping index:
  - `stepId -> moduleName`
  - `syntheticId -> moduleName`
  - `moduleName -> runtimeName`
- Apply defaults based on `layout` and `validation`:
  - `auto`: allow partial mapping
  - `strict`: require full placement

## 3) Annotation processor changes

### Discovery

- Build the full step/synthetic model as today.

### Placement resolution

- Use the resolver to decide if each step/synthetic belongs to the current module
  (`-Apipeline.module=<moduleName>`).
- If not, skip generation for that step.

### Client wiring

- For cross-module calls, generate REST/gRPC clients based on the **pipeline transport**.
- For in-module calls (same module), generate in-process wiring.

### Monolith and pipeline-runtime

- `layout: monolith`: everything maps to one module, orchestrator included.
- `layout: pipeline-runtime`: all steps for a pipeline must resolve to the same runtime;
  orchestrator remains separate.

## 4) Maven layout (no restructuring required)

- Do not create or move Maven modules.
- Each Maven module simply identifies itself via `-Apipeline.module=...`.
- The generator only emits sources for the current module.

## 5) POM simplification (recommended)

To reduce POM complexity, align each Maven module to a single runtime role:

- `orchestrator-svc`: orchestrator runtime only
- `pipeline-runtime-svc`: pipeline server runtime only
- `plugin-runtime-svc`: plugin host runtime only (optional)

This removes most multi-role compile/merge steps seen in `csv-payments`.

## 6) Tests (TDD outline)

- Resolver unit tests:
  - default behavior when file missing
  - modular vs pipeline-runtime vs monolith
  - strict validation errors
- AP integration tests:
  - only generate steps for current module
  - in-process wiring when module matches
  - REST/gRPC wiring when module differs

## 7) Milestones

1. Implement YAML parser + resolver with unit tests.
2. Wire resolver into AP filtering.
3. Add pipeline-runtime and monolith tests.
4. Update example or reference project with `pipeline.runtime.yaml`.
