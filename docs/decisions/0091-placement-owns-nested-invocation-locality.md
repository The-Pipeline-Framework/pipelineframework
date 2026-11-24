---
title: Placement owns nested invocation locality
status: accepted
refines: [ADR-0004, ADR-0011, ADR-0086]
---

# ADR-0091: Placement Owns Nested Invocation Locality

## Context

[ADR-0004](./0004-nested-composition-and-bounded-recursion.md) and
[ADR-0086](./0086-explicit-self-invocation-and-nested-await.md) allow a pipeline definition to
invoke named nested pipelines or `self` and to suspend on durable Await inside those calls.
Earlier compiler validation tied both capabilities to root `transport: LOCAL`, forcing any
application with nested composition or nested Await to declare `LOCAL`/`COMPUTE` even when it is
packaged and deployed as a `REST`/`FUNCTION` worker under `QUEUE_ASYNC` coordination.

At the same time, [ADR-0011](./0011-runtime-deployment-configuration-and-telemetry.md) separates
deployment transport (`GRPC`, `REST`, `LOCAL`), platform (`COMPUTE`, `FUNCTION`), runtime layout
(`modular`, `pipeline-runtime`, `monolith`), and build topology. Inventing a new `execution-segment`
layout or relabeling a `LOCAL` contract as `REST`/`FUNCTION` would blur those boundaries further.

## Decision

Compiled physical placement — not root `transport: LOCAL` — owns whether a step call, nested
pipeline invocation, or `pipeline: self` invocation executes locally within an execution segment
(`LOCAL_IN_SEGMENT`) or crosses a deployed boundary (`REMOTE_BOUNDARY`).

1. **Truthful deployment contract**: Root `platform.json`, `pipeline-contract.json`, and generated
   orchestrator adapters continue to declare the configured deployment `platform` and `transport`
   (for example `REST` / `FUNCTION`). Local in-segment implementation never relabels a deployment
   contract to `LOCAL` / `COMPUTE`.
2. **Existing layouts only**: `PipelineRuntimeMapping.Layout` remains strictly `{ modular,
   pipeline-runtime, monolith }`. No `execution-segment` layout, Maven profile, or runtime flag
   substitutes another source universe.
3. **Explicit colocation rule**:
   - `transport: LOCAL` resolves all steps and nested/self invocations to `LOCAL_IN_SEGMENT`.
   - Under `REST` or `GRPC`, an explicit `pipeline.runtime.yaml` admits `LOCAL_IN_SEGMENT` when
     the resolved `(module, runtime)` matches the compiled module and runtime under
     `pipeline-runtime`, or under `monolith` when `platform: FUNCTION`.
   - `monolith` under `COMPUTE` preserves existing loopback transport adapters.
   - `modular` and unmapped `REST`/`GRPC` compilations resolve cross-boundary invocations to
     `REMOTE_BOUNDARY`, and fail closed until remote nested pipeline invocation is implemented.
4. **Graph-derived placement metadata**: When a pipeline contains nested/self invocations or
   colocated `REST`/`GRPC` placement, `pipeline-contract.json` emits an additive, value-only
   `placement` section (omitted when absent so existing contract fingerprints remain unchanged).
   Execution segments are derived from the actual reachable Await boundaries across branches and
   bounded recursion rather than by counting textual `await:` declarations.
5. **Release agreement**: When a Release carries `deployment` facts
   ([ADR-0090](./0090-release-packaging-facts-share-the-immutable-descriptor.md)) and the compiled
   contract carries `placement`, release validation enforces agreement between deployed entry
   modules and compiled placement modules directly. Release schema-2 deployment packaging is not
   a backwards-compatibility surface, so no legacy schema-2 compatibility shim is retained when
   `placement` is present.

## Rationale

An application's external deployment contract (`REST`/`FUNCTION`, `QUEUE_ASYNC` coordination,
immutable Release packaging) is distinct from whether two compiled definitions share one physical
worker segment. Making placement own invocation locality lets nested composition, bounded self-call,
colocated aspects, and nested Await compile cleanly as `REST`/`FUNCTION` without flattening the
pipeline, bypassing validators, or inventing a fourth layout mode.

Keeping `placement` factual and additive inside `pipeline-contract.json` also avoids coupling this
change to a broader multi-environment release-targeting redesign (for example targeting different
`platform`/`transport`/`layout` combinations per environment from one definition), which remains a
separate future decision.

## Consequences

- `pipelineframework-contracts` owns the immutable `PipelinePlacementDescriptor` contract, its
  additive JSON inclusion in `PipelineContractDescriptor`, and release-descriptor cross-checking
  against deployed modules.
- `pipelineframework-compiler` resolves per-step and per-invocation locality after runtime mapping,
  validates nested composition and nested Await against `LOCAL_IN_SEGMENT` placement, generates
  in-segment `LocalClientStep` and `PipelineInvocation_*` classes alongside truthful root
  `REST`/`FUNCTION` orchestrator adapters, and derives the branch-/recursion-aware segment graph.
- Unmapped `REST`/`FUNCTION` or cross-module nested/self invocations continue to fail compilation
  explicitly rather than silently flattening or emitting broken remote calls.
- `pipelineframework-runtime` executes compiled segments through the admitted Coordinator and
  transition-worker path without a separate placement-specific runtime mode.
