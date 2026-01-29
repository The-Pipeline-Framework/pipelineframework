# Application Design Spectrum (Good vs Bad) and TPF Mitigations

This guide describes the design spectrum we see in pipeline applications and how TPF can reduce risk. The goal is not to police style, but to make failure modes explicit and offer guardrails.

## Spectrum: Good to Bad (behavioral)

### Good (what we want)
- **Clear checkpoints**: each pipeline produces a valid, stable state.
- **Explicit unhappy paths**: business outcomes are modeled as pipelines.
- **Backpressure-aware piping**: demand flows end-to-end.
- **Immutable persistence**: no in-place updates; append-only state.
- **Lean steps**: steps orchestrate, domain logic stays in domain layer.

### Risky (what we should warn about)
- **Implicit business logic in exceptions**: failures are operational, not domain outcomes.
- **Hidden coupling**: downstream assumes upstream internal types.
- **Unbounded buffers**: backpressure is ignored, leading to overload.
- **Silent stalls**: no tracing or stuck-item detection.
- **Cross-pipeline atomicity assumptions**: sync calls mistaken for strong consistency.

### Bad (what we should prevent)
- **Status fields and in-place updates**: undermines immutability.
- **Mixed domain + infrastructure in steps**: layering collapse.
- **Direct DB access across pipelines**: violates ownership.
- **Opaque pipelines**: no lineage, no metrics, no failure routing.

## How TPF Can Mitigate (out-of-the-box)

1) **Checkpoint Contracts**
- Optional declaration of pipeline output invariants.
- Used for documentation and validation.

2) **Traceability (Russian Dolls)**
- Runtime wraps outputs in a TraceEnvelope with previous-item reference.
- Enables lineage without user boilerplate.

3) **Connector Policies**
- Explicit backpressure policy (strict or bounded).
- Idempotency keys to prevent duplicates.

4) **Error Sink**
- A default StdErr sink plus a gRPC/REST sink service.
- Operational failures are centralized and observable.

5) **Build-Time Compatibility**
- Validate that pipeline-to-pipeline contracts match.
- Fail fast before deployment.

6) **Design Lints (optional)**
- Warn on status fields in step-owned entities.
- Warn on in-place update persistence strategies.

7) **Workflow Fan-Out**
- Allow a step output to feed multiple sub-pipelines.
- Require a branch policy (primary vs aux, required vs optional).

8) **Checkpoint Observers vs Mid-Step Taps**
- Observers attached to checkpoints are stable and safe.
- Observers attached to mid-step outputs are explicitly weak-guarantee taps.
- Promote mid-step outputs to checkpoints if you need stable observers.

## Proposed API (fan-out with branch policy)

### 1) Branch policy annotation

```java
public enum BranchPolicy {
  PRIMARY_REQUIRED,
  AUX_REQUIRED,
  AUX_OPTIONAL
}
```

### 2) Step declaration

```java
@PipelineStep
@BranchTo(value = "OrderReadyPipeline", policy = BranchPolicy.PRIMARY_REQUIRED)
@BranchTo(value = "OrderFraudCheckPipeline", policy = BranchPolicy.AUX_REQUIRED)
@BranchTo(value = "OrderNotificationsPipeline", policy = BranchPolicy.AUX_OPTIONAL)
public class OrderCreateStep implements ReactiveService<OrderRequest, OrderCreated> {
  @Override
  public Uni<OrderCreated> process(OrderRequest in) {
    // returns the single happy-path payload
  }
}
```

### 3) Runtime behavior (conceptual)

- Execute all branches on the same emitted item.
- A required branch failure fails the workflow; optional branch failure routes to the error sink.
- Backpressure is governed by the slowest required branch.

## Proposed API (observers and taps)

### 1) Observer policy

```java
public enum ObserverPolicy {
  CHECKPOINT_ONLY,
  MID_STEP_TAP
}
```

### 2) Step declaration

```java
@PipelineStep
@ObserveBy(value = "OrderMarketingPipeline", policy = ObserverPolicy.CHECKPOINT_ONLY)
public class OrderReadyStep implements ReactiveService<InitialOrder, ReadyOrder> {
  @Override
  public Uni<ReadyOrder> process(InitialOrder in) {
    return Uni.createFrom().item(...);
  }
}
```

### 3) Runtime behavior (conceptual)

- CHECKPOINT_ONLY observers subscribe only to persisted, stable outputs.
- MID_STEP_TAP observers can attach to transient outputs and accept weak guarantees.

## Proposed API (gRPC streaming trigger, v1)

### 1) gRPC proto sketch

```proto
service PipelineOrchestrator {
  // Upstream pushes into downstream with backpressure
  rpc Ingest(stream InputType) returns (stream OutputType);
}
```

### 2) Runtime semantics (conceptual)

- **Ingest** is push-style: upstream streams into downstream; flow control is best-effort over gRPC.
- A pipeline can temporarily **stop accepting new items** and keep retrying in-flight items.
- When a dependency is down, the orchestrator **buffers to a durable store** and resumes later.
- Pull-based subscription is planned once a durable buffer is available.

### 3) Build-time considerations

- Generate a **handoff DTO** for each pipeline output (or use the next pipeline input DTO directly).
- Emit **schema ids** to detect version drift at runtime.
- Validate pipeline-to-pipeline compatibility at build time.

### 4) Sharing requirements (how \"friend\" are pipelines)

- **Strong coupling**: shared DTO module and strict version alignment (fast, but tight).
- **Loose coupling**: handoff DTO as public contract + compatibility checks (preferred).
- **Mixed languages**: if a step is in Python, expect weaker backpressure and require a durable buffer.

## Where This Helps vs FTGO

FTGO leans on sagas and eventual consistency. TPF can offer:

- **Stronger checkpoint semantics** within a pipeline.
- **Sync piping with backpressure** as a safe default.
- **Cleaner separation of business vs ops** via explicit pipelines.
