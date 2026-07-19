# Circuit-Breaker Invocation Admission

TPF's circuit-breaker seam protects a dependency invocation before it starts. It is an operational runtime concern at a transport boundary, not a business-step API and not a workflow event.

## Scope Is A Guarantee

`LOCAL_PROCESS` and `SHARED_DEPENDENCY` are observable capabilities, not interchangeable storage implementations.

- `LOCAL_PROCESS` protects only calls admitted by one runtime process. It can reduce pressure from that process, but it does not provide fleet-wide dependency protection, coordinated half-open probes, or HA scheduling guarantees.
- `SHARED_DEPENDENCY` is reserved for a future backend that coordinates failure state and half-open permits across every replica protecting the logical dependency.

The required scope belongs to `CircuitPolicy`; it is deliberately separate from `CircuitIdentity`. A backend must refuse a policy whose guarantee it cannot provide. The current in-memory backend therefore accepts only `LOCAL_PROCESS`.

## Admission Result

Acquiring a circuit returns either a permit or `CircuitOpen(identity, scope, notBefore)`. A permit represents one invocation and must be completed as successful, failed, or cancelled.

`notBefore` is a lower-bound retry hint, never a promise that the next acquisition will succeed. A circuit can reopen before then, and another caller can consume a half-open permit. When a local circuit reaches its open deadline but all half-open permits are in use, it returns future, staggered `notBefore` values. This avoids telling a herd of rejected callers to retry immediately at the same instant.

## Current Integration

`PipelineInvocationRuntime` resolves policies from `pipeline.resilience.circuit` using the stable `protocol:target` transport-boundary key. A missing entry is disabled. When enabled, it acquires a permit before calling the transport supplier and returns `CircuitOpenException` without invoking that supplier when admission is denied.

For example, a generated gRPC operator with boundary key `grpc:pricing.remoteProcess` can be enabled with:

```properties
pipeline.resilience.circuit."grpc:pricing.remoteProcess".enabled=true
pipeline.resilience.circuit."grpc:pricing.remoteProcess".scope=LOCAL_PROCESS
pipeline.resilience.circuit."grpc:pricing.remoteProcess".failure-threshold=5
pipeline.resilience.circuit."grpc:pricing.remoteProcess".failure-window=PT1M
pipeline.resilience.circuit."grpc:pricing.remoteProcess".open-duration=PT30S
pipeline.resilience.circuit."grpc:pricing.remoteProcess".half-open-max-permits=1
pipeline.resilience.circuit."grpc:pricing.remoteProcess".half-open-retry-delay=PT1S
```

An optional `identity` property groups compatible boundaries under one logical dependency. Entries that resolve to the same identity must use the same policy. `SHARED_DEPENDENCY` and all durable transition-worker dispatch policies are rejected during startup while only the local backend exists.

The breaker receives only `SUCCESS`, `HEALTH_FAILURE`, or `NEUTRAL` terminal outcomes. The runtime maps TPF's existing transport categories: timeout, unavailable, and remote-server failures affect circuit health; cancellation, authentication, malformed/protocol failures, and unexpected failures are neutral by default. This keeps HTTP/gRPC classification outside `runtime-core`.

Each circuit state transition advances a generation. A permit can update state only when it belongs to the current generation, so a late half-open result cannot close or otherwise corrupt a newer reopened circuit.
Neutral half-open results release their permit but keep the circuit half-open so that a later real dependency probe determines recovery.

The initial backend is intentionally local and is suitable only for non-durable, process-scoped protection. Durable transition-worker dispatch is rejected rather than silently receiving local circuit behavior. It does not feed `QUEUE_ASYNC` retry timing: a local `notBefore` cannot honestly defer work durably across replicas. A future shared backend may supply its `notBefore` to the existing retry scheduler only for executions that encounter an open circuit. It must preserve the distinction between a denied call and an attempted failed call, enforce a finite circuit-deferral lifetime, and must not add bulk parking, circuit events, or dependency-specific queues.
