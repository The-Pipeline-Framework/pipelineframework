# Circuit-Breaker Invocation Admission

TPF's circuit-breaker seam protects a dependency invocation before it starts. It is an operational runtime concern at a transport boundary, not a business-step API and not a workflow event.

## Scope Is A Guarantee

`LOCAL_PROCESS` and `SHARED_DEPENDENCY` are observable capabilities, not interchangeable storage implementations.

- `LOCAL_PROCESS` protects only calls admitted by one runtime process. It can reduce pressure from that process, but it does not provide fleet-wide dependency protection, coordinated half-open probes, or HA scheduling guarantees.
- `SHARED_DEPENDENCY` protects replicas that use the same configured DynamoDB table in one AWS Region. It coordinates state transitions and half-open probe leases across those replicas; it does not claim global-table or cross-region coherence.

The required scope belongs to `CircuitPolicy`; it is deliberately separate from `CircuitIdentity`. A backend must refuse a policy whose guarantee it cannot provide. The current in-memory backend therefore accepts only `LOCAL_PROCESS`.

## Admission Result

Acquiring a circuit is asynchronous and returns either a permit, `CircuitOpen(identity, scope, notBefore)`, or a protection-unavailable rejection. A permit represents one invocation and must be completed as successful, failed, or cancelled. Completion is observational: it never replaces the workload result.

`notBefore` is a lower-bound retry hint, never a promise that the next acquisition will succeed. A circuit can reopen before then, and another caller can consume a half-open permit. For shared OPEN state it is the authoritative open deadline. For saturated shared HALF_OPEN state it is the later of the configured retry delay and the earliest valid probe-lease expiry. This avoids telling a herd of rejected callers to retry immediately at the same instant.

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
pipeline.resilience.circuit."grpc:pricing.remoteProcess".half-open-probe-lease-duration=PT30S
```

An optional `identity` property groups compatible local boundaries under one logical dependency. Entries that resolve to the same identity must use the same policy.

Shared policies require an explicit identity and a configured Dynamo table:

```properties
pipeline.resilience.circuit."grpc:pricing.remoteProcess".enabled=true
pipeline.resilience.circuit."grpc:pricing.remoteProcess".scope=SHARED_DEPENDENCY
pipeline.resilience.circuit."grpc:pricing.remoteProcess".identity=pricing-service
pipeline.resilience.shared.dynamo-table=tpf_shared_circuits
pipeline.resilience.shared.max-state-staleness=PT1S
pipeline.resilience.shared.backend-retry-delay=PT1S
```

Healthy CLOSED admission uses a fresh process-local snapshot, so it does not call DynamoDB for every invocation. OPEN visibility is bounded by `max-state-staleness`, plus already-started calls. Transitions, half-open permits, and probe completion use conditional/transactional Dynamo operations. If the authority is unavailable, a fresh CLOSED snapshot may be used only until it expires; after that the shared policy rejects as protection unavailable and never degrades to local protection. State includes a policy fingerprint, so incompatible thresholds or durations cannot silently share a circuit.

The breaker receives only `SUCCESS`, `HEALTH_FAILURE`, or `NEUTRAL` terminal outcomes. The runtime maps TPF's existing transport categories: timeout, unavailable, and remote-server failures affect circuit health; cancellation, authentication, malformed/protocol failures, and unexpected failures are neutral by default. This keeps HTTP/gRPC classification outside `runtime-core`.

Each circuit state transition advances a generation. A permit can update state only when it belongs to the current generation, so a late half-open result cannot close or otherwise corrupt a newer reopened circuit.
Neutral half-open results release their permit but keep the circuit half-open so that a later real dependency probe determines recovery.

Local circuit protection remains process-scoped. Durable transition-worker dispatch rejects local policies, and local `notBefore` values do not provide durable cross-replica retry deferral.

## Durable Deferral

When queue-async work encounters a shared `CircuitOpen` or protection-unavailable rejection, TPF does not start the dependency call and does not consume the remote-attempt counter. It persists a dedicated circuit deferral with the identity, first-deferral time, count, and `nextDue = max(existing retry decision, notBefore)`. Only the execution that encountered the rejection is deferred; opening a circuit does not scan, park, or wake unrelated executions.

Shared transition-worker dispatch requires a finite `pipeline.orchestrator.max-circuit-deferral`. Once that lifetime is exhausted, the execution fails with `circuit_deferral_exhausted`. This preserves bounded durable failure behavior without treating a denied permission as a failed remote attempt.
