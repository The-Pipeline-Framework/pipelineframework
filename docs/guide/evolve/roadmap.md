# Roadmap: Checkpoint Pipelines vs FTGO (Pessimist's Notebook)

This guide captures the ongoing architectural exploration of checkpoint-style pipelines as an alternative to FTGO's saga-first model. It is written for the engineer who just entered the meeting room: quick context, core principles, and the risks we are explicitly tracking. The goal is to be intentionally pessimistic: list what can go wrong, what we believe is already covered, and what still needs design work.

For broader context, see:
- [TPF and DDD Alignment](/guide/evolve/ddd-alignment)
- [Application Design Spectrum](/guide/evolve/design-spectrum)

## TL;DR (why this exists)

- We want a **better-than-FTGO** architecture that avoids rollbacks and avoids hiding domain decisions in ops.
- We treat a pipeline as a **checkpoint**: when it finishes, the state is valid and stable.
- We prefer **sync, reactive piping** between pipelines (backpressure preserved) over async fan-out.
- We accept that failures are **operational** unless the business explicitly models an unhappy path.
- We assume a **single tech stack** and a single leadership/business, so "autonomy for its own sake" is not a goal.

## Working Model (current stance)

- **Pipeline = checkpoint**: a pipeline produces a stable, valid state. No status fields, no updates, no rollbacks.
- **Steps own persistence**: 1 step = 1 table/entity type.
- **Success vs failure only**: success flows data; failures are operational and go to an error sink.
- **Business unhappy paths**: modeled as explicit pipelines, not as exceptions.
- **Sync piping**: pipelines can be chained with reactive (non-blocking) request/response.

## Architectural Principles (expanded)

1) **Shipability over modularity**
   The primary boundary is how we **ship/release**. The orchestrator and step runtimes exist to make deployment safe and observable. Code modularity is useful, but secondary.

2) **Immutable checkpoints, no in-place updates**
   Status fields imply updates and mutable state. We avoid them. Each step persists a new, immutable type and hands it forward.

3) **Pipelines are composite steps**
   A pipeline is a higher-order step with clear input and output types. This lets us chain pipelines as a workflow without pretending they are a monolith.

4) **Operational failures are not domain logic**
   A failure (exception) is operational. Domain unhappy paths are modeled as explicit pipelines.

5) **Backpressure is a first-class contract**
   If pipelines are chained, demand must flow end-to-end. We avoid unbounded buffering and treat backpressure as part of the contract.

## Visuals (how it works)

### 1) Checkpoint pipeline (single pipeline, immutable steps)

```mermaid
flowchart LR
  A[OrderRequestProcess\npersist OrderRequest + LineItem] --> B[OrderCreate\npersist InitialOrder]
  B --> C[OrderReady\npersist ReadyOrder]
  C --> D((Checkpoint))
```

### 2) Pipeline-to-pipeline piping (sync, reactive)

```mermaid
flowchart LR
  subgraph P1[Pipeline A: CreateOrder]
    A1[Step1] --> A2[Step2] --> A3[Step3]
  end
  subgraph P2[Pipeline B: DeliverOrder]
    B1[Step1] --> B2[Step2]
  end
  A3 -- pipe (sync, backpressure) --> B1
```

### 3) Failure handling separation

```mermaid
flowchart LR;
  X[Step Failure] --> E[Error Sink];
  X --> U[Business Unhappy Pipeline];
  E --> U;
```

### 4) Workflow fan-out (pipeline as workflow)

```mermaid
flowchart LR
  S[Step Output] --> P[Primary Branch Pipeline]
  S --> A[Aux Branch Pipeline A]
  S --> B[Aux Branch Pipeline B]
  P --> T((Primary Terminal))
  A --> TA((Aux Terminal A))
  B --> TB((Aux Terminal B))
```

### 5) Observers vs mid-step taps

```mermaid
flowchart LR;
  M[Mid-Step Output] --> T[Observer Tap];
  C[Checkpoint Output] --> O[Observer Pipeline];
```

### 6) Lifecycle evolution (early vs mature)

```mermaid
flowchart LR
  subgraph Early[Early system]
    E1((Checkpoint A)) --> E2((Checkpoint B))
  end
  subgraph Mature[Mature system]
    M1((Checkpoint A)) --> M2[More steps between] --> M3((Checkpoint B))
  end
```

## Example (CreateOrder, checkpoint model)

**Input DTO**: `OrderRequest`
**Output DTO**: `ReadyOrder`

Steps (each step persists its own type):
- `OrderRequestProcess` -> `OrderRequest` + `LineItem`
- `OrderCreate` -> `InitialOrder`
- `OrderReady` -> `ReadyOrder`

**Outcome**: if the pipeline completes, `ReadyOrder` is valid and stable. No rollback. If a failure occurs, it goes to the error sink unless it has been explicitly classified as a business unhappy path, in which case it is routed to a dedicated unhappy pipeline.

## Pain-Point Matrix

Status legend: RESOLVED, DECIDED, PROPOSED, PARTIAL, OPEN

1) **Checkpoint invariants**
- **Problem**: What makes a pipeline output "valid"?
- **Stance**: The pipeline process itself guarantees invariants; no status fields, no validators needed.
- **Status**: RESOLVED

2) **Failure classification**
- **Problem**: Distinguish business unhappy paths vs operational failures.
- **Stance**: TPF uses a single failure channel; exceptions are operational and go to the error sink. Business unhappy paths are separate pipelines.
- **Status**: DECIDED

3) **Partial progress across pipelines**
- **Problem**: Pipeline A completes, Pipeline B fails.
- **Stance**: Treated as an ops failure; A's checkpoint remains valid. Optional ops pipelines may handle remediation.
- **Status**: OPEN

4) **Idempotency / duplicate handoff**
- **Problem**: Connector retries can duplicate downstream processing.
- **Stance**: Needs a connector-level idempotency key and de-dup policy.
- **Status**: OPEN

5) **Traceability / lineage**
- **Problem**: Track the lineage of items through steps and pipelines.
- **Stance**: Implement "russian doll" tracing in the runtime (TraceEnvelope with previous-item reference or inline payload).
- **Status**: PROPOSED

6) **Type compatibility between pipelines**
- **Problem**: Pipeline B should not depend on Pipeline A internals.
- **Stance**: Use Pipeline B input DTO as the handoff contract; add build-time compatibility checks.
- **Status**: PARTIAL

7) **Backpressure across pipelines**
- **Problem**: Piping should preserve backpressure end-to-end.
- **Stance**: Needs a connector policy that propagates demand (no unbounded buffers).
- **Status**: OPEN

8) **Branching outputs (multi-out steps)**
- **Problem**: A step may need to emit different output types based on business decisions.
- **Stance**: Allow workflow fan-out by piping sub-pipelines off a step output; require explicit branch policy (primary vs aux, required vs optional).
- **Status**: PROPOSED

9) **Observers and mid-step taps**
- **Problem**: Optional features (e.g., marketing) may want to observe outputs that are not stable checkpoints.
- **Stance**: Distinguish checkpoint observers (stable) from mid-step taps (weak guarantees); allow explicit opt-in.
- **Status**: PROPOSED

10) **Decision points as checkpoints**
- **Problem**: Adding a new decision step can introduce a new step type or complex branching inside a pipeline.
- **Stance**: Prefer ending the pipeline at a decision and spawning one pipeline per outcome. Over time, checkpoints should remain relatively stable even as steps grow.
- **Status**: PROPOSED

11) **Remote subscription trigger**
- **Problem**: Pipeline-to-pipeline chaining currently relies on external triggers (CLI/HTTP).
- **Stance**: Add a streaming trigger to the orchestrator (subscribe/ingest) with backpressure and buffering.
- **Status**: PROPOSED

## Additional Risks (forward-looking)

- **Cross-pipeline atomicity illusion**: sync chaining can look atomic while still being partial.
- **Schema drift**: handoff DTO versioning can break compatibility without strict rules.
- **Temporal coupling**: downstream slowness collapses upstream throughput.
- **Hotspot steps**: a single heavy step can dominate latency and throughput.
- **Backpressure deadlocks**: mismatched demand signaling can stall a chain.
- **Implicit retries**: connector retries can trigger duplicate side effects.
- **Observability blind spots**: reference-based tracing needs reliable lookup.
- **Fan-out/fan-in complexity**: ordering and timeout handling become tricky.
- **Distributed time assumptions**: ordering based on timestamps becomes ambiguous.
- **Policy leakage into ops**: domain obligations can get pushed into SLOs if not modeled.

## Near-Term Design Work

- **Error Sink**: define a runtime error sink interface with a default StdErrSink and optional gRPC/REST sink service.
- **Connector Policy**: define backpressure strategy and idempotency guarantees.
- **TraceEnvelope**: add optional tracing that wraps step output with previous-item linkage.
- **Build-Time Checks**: verify pipeline-to-pipeline handoff type compatibility.

## Open Questions

- Should the connector enforce strict backpressure by default?
- Should connector idempotency be mandatory or opt-in?
- How should the tracing store be configured (inline vs reference)?
- Should the pipeline definition expose a formal "checkpoint contract"?
- How should multi-out decisions be modeled (discriminated envelopes vs explicit pipelines)?
- How should business exceptions be declared and propagated across steps?

## Intended Outcome

A pragmatic, pessimistic architecture that improves on FTGO by keeping strong, explicit checkpoint semantics, minimal operational ambiguity, and a clear separation of business vs ops concerns.
