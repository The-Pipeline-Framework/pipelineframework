---
search: false
---

# Pipeline Aspect Application Semantics

## Overview
Pipeline aspects are cross-cutting semantic concerns (e.g. persistence, metrics, tracing) that apply around pipeline steps without altering the pipeline's functional shape.

## Core Principles
- Aspects do not change:
  - Pipeline inputs/outputs
  - Step cardinality
  - Streaming shape
- Aspects are logically expanded into identity side-effect steps
- Expansion is semantic only (not user-visible)

## Application Scopes

### GLOBAL Scope
- Aspects with GLOBAL scope apply to all steps present in the pipeline definition at generation time
- Future steps added later require regeneration
- They are applied consistently across every step in the pipeline
- This is useful for concerns like global metrics collection, tracing, or persistence

### STEPS Scope
- STEPS scope targets an explicit subset of steps
- Steps are selected via `targetSteps` in the aspect config
- This is useful for concerns that should only apply to specific steps (e.g., cache invalidation before a replayed step)

## Logical Expansion
Aspects are conceptually expanded into identity side-effect steps in the pipeline:
- Aspects execute either BEFORE_STEP (on the input type) or AFTER_STEP (on the output type)
- These expansions are purely semantic and not visible to the user
- The pipeline's functional contract remains unchanged

Exception: the `cache` aspect is applied at the orchestrator client step (before remote invocation) and does not expand into a side-effect step.

## Execution model

Expanded side-effect steps are **chained**, not fanned out:

- Each aspect becomes a step in the pipeline order
- Each step receives the same element type and returns it unchanged
- Multiple aspects on the same position execute sequentially according to ordering rules

Parallelism follows the pipeline-level policy (`pipeline.parallelism`, `pipeline.max-concurrency`). There is no implicit fan-out or parallel execution introduced by aspects.

## Failure semantics

Side-effect steps follow the same failure semantics as regular steps:

- **Fail-fast by default**: a side-effect failure aborts the pipeline
- **Configurable recovery**: if `recoverOnFailure` is enabled for the side-effect step, failures route to DLQ and the pipeline continues

This keeps aspect behavior consistent and observable using the standard step configuration model.

Best-effort execution:

To make an aspect best-effort, enable recovery for the corresponding side-effect step so failures are routed to DLQ and the pipeline continues. This should be an explicit per-step decision, not a default.

## Side-effect transport contract
Side-effect plugins observe stream elements and never alter the stream shape. For transport, each observation is modeled
as a unary call for the configured transport (gRPC or REST) using a deterministic, type-indexed service derived from
the element type:

- Shape: `T -> T` (always unary)
- Service name: `Observe<T>SideEffectService`
- Injection point: BEFORE or AFTER each step, based on aspect position

For gRPC transport, the protobuf descriptor set used during compilation must include these
`Observe<T>SideEffectService` definitions or side-effect adapter generation will be skipped.

## Aspect Invariants
- Aspects do not change pipeline types or topology
- Aspects may have side effects
- Aspects may observe or persist data
- Aspects are not allowed to alter pipeline control flow
- Any aspect that does require data transformation must be modeled as a Step, not an Aspect

## Deployment Roles
- Deployment roles are resolved by binding/aspect expansion only; they must not affect generated code.
- Renderers generate identical code and only vary output directories and packaging inputs.
