# TPF and DDD Alignment (Current + Future)

This guide summarizes how TPF aligns with DDD concepts today, where it diverges, and what we intend to improve. It captures the key ideas from the checkpoint-pipeline discussion so newcomers can quickly orient themselves.

## Executive Summary

- **TPF aligns with DDD at the use-case level**: a pipeline is closest to an application service.
- **TPF diverges at the aggregate level**: instead of a single aggregate with multiple tables, TPF uses a progression of immutable aggregate states, each stored in its own table.
- **DDD layering is possible but not enforced**: it depends on how steps are written.
- **Cross-context orchestration is compatible** but needs explicit contracts and backpressure-aware piping.

## DDD Terms (short and concrete)

- **Application service**: orchestrates a use case; coordinates domain operations and infrastructure.
- **Domain operation**: a business behavior on an aggregate or domain service (invariants live here).
- **Aggregate**: consistency boundary in the domain model.
- **Bounded context**: boundary where a model and language are consistent.
- **Cross-context orchestration**: communication across bounded contexts (typically async).

## TPF Mapping to DDD

### 1) Application Service

**TPF mapping**: A pipeline is a use-case orchestrator. Each pipeline step can be seen as a slice of the application service.

- **Good alignment**: pipelines naturally express use-case flow, sequencing, and side effects.
- **Risk**: steps can accidentally mix domain logic and infrastructure if not disciplined.
- **TPF guidance**: keep steps thin; put business rules inside domain types or domain services called by the step.

### 2) Domain Operation

**TPF mapping**: A step can wrap a domain operation, but the operation should still live in the domain layer.

- **Good alignment**: step calls a domain method (e.g., `Order.create`, `Order.approve`).
- **Risk**: domain logic implemented directly in step classes (layering leak).
- **TPF guidance**: steps should orchestrate; domain operations should enforce invariants.

### 3) Aggregate Boundary

**TPF mapping**: The pipeline becomes the aggregate boundary, not a single class/table.

- TPF uses a **progression of aggregate states** (one per step/table).
- This is a deliberate trade-off: immutable, append-only checkpoints instead of in-place updates.
- **Example**: `OrderRequest` -> `InitialOrder` -> `ReadyOrder` as successive stable states.

### 4) Bounded Context

**TPF mapping**: A pipeline (or set of pipelines) can represent a bounded context.

- Works best when the context owns its data and invariants.
- Cross-context calls should use explicit handoff contracts.
- **TPF guidance**: treat pipeline-to-pipeline contracts as public APIs, not shared internal types.

### 5) Cross-Context Orchestration

**TPF mapping**: Sync, reactive piping is possible; async is optional.

- The main risk is **consistency illusion** across pipelines.
- TPF needs explicit connector semantics (backpressure + idempotency).
- **TPF guidance**: only assume strong consistency inside a single pipeline; be explicit about cross-pipeline guarantees.

## Workflow Shape: Linear/Tree vs DAG

TPF favors **linear or tree-like** flows where possible:

- **Linear/tree workflows** are easier to reason about and preserve pipeline semantics.
- **DAG workflows** introduce joins, fan-in, and potential feedback loops, which complicate ordering, timeouts, and backpressure.

The checkpoint model works best when pipelines are composed in a tree-like fashion, with explicit handoff contracts at each edge.

## Decision as Checkpoint (rule of thumb)

Rather than introducing a special "decision step," a decision can be modeled as a **checkpoint boundary**:

- End the current pipeline at the decision.
- Start one pipeline per outcome branch.
- This keeps step semantics simple and makes outcomes explicit.

Operationally this adds orchestration, but it keeps the design model stable as the system grows.

## Autonomy vs Consistency (TPF stance)

TPF does not treat autonomy as a default good. In a single business with a unified tech stack:

- **Strong checkpoint consistency** is preferred within a pipeline.
- **Sync piping** is preferred between pipelines when practical.
- **Eventual consistency** is only accepted when explicitly chosen.

This differs from FTGO's default assumption of async sagas and autonomy-first boundaries.

## CreateOrder / ApproveOrder Examples (FTGO mapping)

**CreateOrder**
- TPF pipeline steps: OrderRequestProcess -> OrderCreate -> OrderReady
- Each step persists its own immutable state.
- The pipeline output is the checkpoint, not a mutable aggregate.

**ApproveOrder**
- A separate pipeline that consumes the checkpoint and produces a new immutable state.
- If approval fails, it is operational unless modeled as a business pipeline.

## DDD Layering in TPF

TPF does not enforce layering. A suggested discipline:

- **Domain layer**: entities, value objects, domain services.
- **Application layer**: pipeline steps that orchestrate domain operations.
- **Infrastructure layer**: persistence, mappers, transports.

## Current Gaps

- No explicit aggregate boundary declaration.
- No built-in enforcement of layering.
- Pipeline-to-pipeline contracts are implicit (build-time checks needed).

## Planned Directions

- **Handoff contracts** between pipelines (explicit, build-validated).
- **Traceability**: built-in lineage tracking (TraceEnvelope).
- **Backpressure contracts** across piped pipelines.
- **Error sink** as a first-class runtime component.
- **Workflow constraints**: optional guidance to keep pipelines linear or tree-like by default.
- **Design diagnostics**: flag risky patterns (e.g., implicit DAG joins, unbounded buffering).
- **Workflow fan-out**: allow sub-pipelines to branch from a step with explicit branch policies.
- **Observers vs taps**: distinguish stable checkpoint observers from mid-step taps with weak guarantees.
- **Remote subscription trigger**: orchestrators accept streaming input for pipeline-to-pipeline chaining.
