# Option B Prompt: First-Class Blocking Support (Future Work)

Use this prompt when we decide to reintroduce blocking/non-reactive support in TPF.

## Prompt

You are an AI coding agent working on The Pipeline Framework (TPF). Blocking step support and `runOnVirtualThreads` were removed in Feb 2026 to simplify the runtime. The system is now Mutiny-first: `PipelineRunner` supports `StepOneToOne`, `StepOneToMany`, `StepManyToOne`, `StepManyToMany`, and `StepOneToOneCompletableFuture`. The `@PipelineStep` annotation no longer exposes `runOnVirtualThreads`, and there is no `org.pipelineframework.step.blocking` package.

We want to reintroduce a **first-class blocking path** for teams that can’t or won’t write Mutiny, while being explicit about tradeoffs (loss of automatic backpressure, potential throughput limits, and event-loop blocking risks).

### Goals

1. **Blocking step APIs** that are *truly* synchronous (no Mutiny in the user-facing method signature).
2. **Safe execution** by offloading blocking work onto worker/virtual threads by default.
3. **Clear separation** between reactive and blocking step semantics.
4. **Documented tradeoffs** and guidance, including when to refactor to reactive.
5. **E2E validation** in the csv-payments example (e.g., a blocking CSV input step).

### Required Changes (High-Level)

- **Runtime APIs**
  - Recreate `org.pipelineframework.step.blocking` interfaces:
    - `StepOneToOneBlocking<I,O>` with `O applyBlocking(I in)`
    - `StepOneToManyBlocking<I,O>` with `List<O> applyBlocking(I in)`
    - `StepManyToOneBlocking<I,O>` with `O applyBatchBlocking(List<I> inputs)`
    - `StepManyToManyBlocking<I,O>` with `List<O> applyBatchBlocking(List<I> inputs)`
  - Provide default adapters that wrap these into Mutiny (`Uni`/`Multi`) on a worker or virtual-thread executor.
  - Ensure retry/backoff, DLQ handling, and telemetry are applied consistently.

- **PipelineRunner**
  - Add switch cases for each blocking step type.
  - Offload blocking work to the configured executor; avoid running blocking code on event-loop threads.
  - Preserve ordering/parallelism semantics (SEQUENTIAL/AUTO/PARALLEL) and max concurrency.

- **Annotation & Codegen**
  - Reintroduce `runOnVirtualThreads` in `@PipelineStep` with clear semantics.
  - Update the deployment processor to capture it, and re-enable `@RunOnVirtualThread` in generated adapters when true.
  - Update docs to show how to configure execution context.

- **gRPC/REST Adapters**
  - Option A: Keep service interfaces reactive, but wrap blocking implementations into reactive adapters.
  - Option B: Add explicit blocking service interfaces plus adapters. Choose one and document why.
  - Ensure server-streaming and bidirectional streaming behave correctly under blocking execution.

- **Examples & Tests**
  - Add a blocking step in `examples/csv-payments` (e.g., CSV input parsing without Mutiny).
  - Wire it into the pipeline and update the E2E test to exercise it.
  - Add unit tests for each blocking step type and runner integration.

### Guardrails

- Do **not** block event-loop threads.
- Make tradeoffs explicit in docs (no automatic backpressure for blocking steps).
- Keep reactive path unchanged and performant.
- Update existing tests and docs to avoid mixed semantics ambiguity.

### Suggested Starting Points

- `framework/runtime/src/main/java/org/pipelineframework/PipelineRunner.java`
- `framework/runtime/src/main/java/org/pipelineframework/annotation/PipelineStep.java`
- `framework/deployment/src/main/java/org/pipelineframework/processor/extractor/PipelineStepIRExtractor.java`
- `framework/deployment/src/main/java/org/pipelineframework/processor/renderer/*`
- `examples/csv-payments`

### Definition of Done

- Blocking steps compile and run without Mutiny in user code.
- PipelineRunner handles blocking steps in all execution policies.
- Docs clearly explain the tradeoffs and usage.
- E2E test covers at least one blocking service/step.
