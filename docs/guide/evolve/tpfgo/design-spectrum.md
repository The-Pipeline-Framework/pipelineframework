# Application Design Spectrum (Good vs Bad) and TPF Mitigations

This guide describes the design spectrum we see in pipeline applications and how TPF can reduce risk. The goal is not to police style, but to make failure modes explicit and offer guardrails.

## Spectrum: Good to Bad (behavioural)

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

## How TPF Can Mitigate (planned / opt-in)

1. **Checkpoint Contracts**
- Optional declaration of pipeline output invariants.
- Used for documentation and validation.

1. **Traceability (Russian Dolls)**
- Runtime wraps outputs in a `TraceEnvelope` (planned wrapper carrying payload + previous-item reference; “Russian dolls” refers to nested lineage envelopes).
- Enables lineage without user boilerplate.

1. **Connector Policies**
- Explicit backpressure policy (strict or bounded).
- Idempotency keys to prevent duplicates.

1. **Error Sink**
- A default StdErr sink (planned stdout/stderr logger) plus a gRPC/REST sink service.
- Operational failures are centralized and observable.

1. **Build-Time Compatibility**
- Validate that pipeline-to-pipeline contracts match.
- Fail fast before deployment.

1. **Design Lints (optional)**
- Warn on status fields in step-owned entities (step-owned types persisted by each step).
- Warn on in-place update persistence strategies.

1. **Workflow Fan-Out**
- Allow a step output to feed multiple sub-pipelines.
- Require a branch policy (primary vs aux, required vs optional).

1. **Checkpoint Observers vs Mid-Step Taps**
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

### 3) Runtime behaviour (conceptual)

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

Multiple observers on the same checkpoint output:

```java
@PipelineStep
@ObserveBy(value = "OrderMarketingPipeline", policy = ObserverPolicy.CHECKPOINT_ONLY)
@ObserveBy(value = "OrderAnalyticsTap", policy = ObserverPolicy.MID_STEP_TAP)
public class OrderReadyStep implements ReactiveService<InitialOrder, ReadyOrder> {
  @Override
  public Uni<ReadyOrder> process(InitialOrder in) {
    return Uni.createFrom().item(...);
  }
}
```

### 3) Runtime behaviour (conceptual)

- CHECKPOINT_ONLY observers subscribe to persisted, stable outputs.
- MID_STEP_TAP observers can attach to transient outputs and accept weak guarantees.
- When both are present, the checkpoint observer receives stable outputs; the tap observer receives transient outputs.

## Proposed API (gRPC streaming trigger, v1)

### 1) gRPC proto sketch

```proto
service OrchestratorService {
  // Upstream pushes into downstream with backpressure
  rpc Ingest(stream InputType) returns (stream OutputType);
  // Downstream subscribes to live output (no replay)
  rpc Subscribe(google.protobuf.Empty) returns (stream OutputType);
}
```

### 2) Runtime semantics (conceptual)

- **Ingest** is push-style: upstream streams into downstream; gRPC/HTTP2 provides transport-level flow control, while application-level buffering and retry are best-effort.
- A pipeline can temporarily **stop accepting new items** and keep retrying in-flight items.
- When a dependency is down, the orchestrator **buffers to a durable store** and resumes later.
- Pull-based subscription is planned once a durable buffer is available.
- **Subscribe** is live-only (no replay) in the current in-memory bus implementation.

### 3) Build-time considerations

- Generate a **handoff DTO** for each pipeline output (or use the next pipeline input DTO directly).
- Emit **schema ids** to detect version drift at runtime.
- Validate pipeline-to-pipeline compatibility at build time.

### 4) Sharing requirements (how closely coupled are pipelines)

- **Strong coupling**: shared DTO module and strict version alignment (fast, but tight).
- **Loose coupling**: handoff DTO as public contract + compatibility checks (preferred).
- **Mixed languages**: if a step is in Python, expect weaker backpressure and require a durable buffer.

## Proposed API (REST slow lane, v1)

### 1) Endpoints

- `POST /pipeline/ingest` (NDJSON in, NDJSON out)
- `GET /pipeline/subscribe` (NDJSON stream, live-only)

### 2) Notes

- Intended for low-throughput adoption paths.
- Backpressure is weaker than gRPC and relies on server-side buffering (e.g., persistent DB, message queue, or local disk/embedded store) and client pacing; overflow policies include reject-new, drop-oldest, block/wait, or spill-to-disk, with upstream signals via HTTP 429, queue depth metrics, or application-level ACK/NACK. Mixed-language systems (e.g., Python) often have different event-loop or concurrency semantics, so adapters should account for GIL and runtime differences. (See the TPFGo roadmap for persistence and failure-mode planning.)

## Usage Examples (gRPC + REST)

### 1) gRPC pipeline-to-pipeline chaining (Ingest)

Assume Pipeline B generates an `OrchestratorIngestClient` in its orchestrator client module. Pipeline A depends on that module and injects the client.

Wiring note: add Pipeline B's `orchestrator-client` module as a dependency of Pipeline A.

```xml
<dependency>
  <groupId>org.pipelineframework</groupId>
  <artifactId>dispatch-orchestrator-client</artifactId>
  <version>${project.version}</version>
</dependency>
```

```java
@ApplicationScoped
public class CheckoutToDispatchPipe implements StepManyToMany<ReadyOrder, DispatchReady> {
  @Inject
  org.pipelineframework.dispatch.orchestrator.client.OrchestratorIngestClient dispatchClient;

  @Override
  public Multi<DispatchReady> applyTransform(Multi<ReadyOrder> readyOrders) {
    return dispatchClient.ingest(readyOrders);
  }
}
```

### 2) gRPC live subscription (Subscribe)

```java
@ApplicationScoped
public class DispatchObserver {
  @Inject
  org.pipelineframework.dispatch.orchestrator.client.OrchestratorIngestClient dispatchClient;

  public Multi<DispatchReady> liveStream() {
    return dispatchClient.subscribe(); // live-only, no replay
  }
}
```

### 3) REST slow lane (NDJSON)

Ingest (push):

```bash
curl -N \
  -H "Content-Type: application/x-ndjson" \
  --data-binary @ready-orders.ndjson \
  http://dispatch-svc/pipeline/ingest
```

Subscribe (live-only stream):

```bash
curl -N \
  -H "Accept: application/x-ndjson" \
  http://dispatch-svc/pipeline/subscribe
```

## Where This Helps vs FTGO

FTGO leans on sagas and eventual consistency. TPF can offer:

- **Stronger checkpoint semantics** within a pipeline.
- **Sync piping with backpressure** as a safe default.
- **Cleaner separation of business vs ops** via explicit pipelines.
