---
search: false
---

# Operate Circuit Protection

Circuit protection prevents a TPF-managed dependency invocation from starting when that dependency is known to be unhealthy. It is not a business-step API and it does not wrap outbound calls hidden inside application code.

## Scope And Runtime Capability

`LOCAL_PROCESS` stores health in one runtime process. It is useful for isolated or cost-sensitive boundaries, but replicas can disagree about state and its `notBefore` hint is not a durable cross-replica deferral signal.

`SHARED_DEPENDENCY` uses one configured DynamoDB table in one AWS Region. It coordinates transitions and half-open probe leases across participating replicas. CLOSED admission uses a fresh local cache, so shared protection is not linearizable fleet-wide admission: a replica can admit from a fresh CLOSED snapshot until `max-state-staleness` expires. It does not provide global-table or cross-region coherence.

The deployment selects `pipeline.resilience.default-circuit-scope`; advanced boundary overrides may choose a compatible different scope. Shared selection requires:

```properties
pipeline.resilience.default-circuit-scope=SHARED_DEPENDENCY
pipeline.resilience.shared.dynamo-table=tpf_shared_circuits
pipeline.resilience.shared.max-state-staleness=PT1S
pipeline.resilience.shared.backend-retry-delay=PT1S
```

If the shared authority is unavailable, a fresh CLOSED cache can be used only until it expires. Afterwards the circuit rejects as protection unavailable; it never silently degrades to local protection.

## Rejections And Durable Work

`CIRCUIT_OPEN` means no remote call was attempted. Its `notBefore` value is a scheduling hint, not a reservation: another caller can consume a half-open permit before that time. Shared saturated half-open circuits return a hint that avoids an immediate probe herd.

For shared queue-async transition-worker dispatch, TPF persists a circuit deferral with `nextDue = max(existing retry decision, notBefore)`. It does not consume the remote-attempt counter and it does not bulk-park unrelated executions. Configure a finite lifetime:

```properties
pipeline.orchestrator.max-circuit-deferral=PT15M
```

When that lifetime is exhausted, TPF terminally fails the execution with `circuit_deferral_exhausted`. Protection-unavailable deferrals retain their distinct reason.

## Signals And Response

Use these metrics with dependency latency, timeout, and availability signals:

| Metric | Meaning |
| --- | --- |
| `tpf.circuit.admissions` | Permitted or rejected admission, tagged with circuit identity, scope, and policy source. |
| `tpf.circuit.transitions` | Circuit state transitions by identity and scope. |
| `tpf.transport.boundary.circuit.rejections` | Transport-boundary calls rejected before supplier invocation. |

If a circuit remains open, first inspect the dependency’s availability and recent timeout/unavailable/remote-server failures. Do not lower thresholds merely to make traffic resume. Confirm that the configured scope matches the topology, that a shared Dynamo table is reachable when required, and that half-open probe lease duration covers the invocation timeout.

Tune threshold and timing from observed dependency behavior and recovery objectives; TPF provides guardrails, not named resilience strategies. See [Execution Safety](/versions/v26.8.1/design/execution-safety) and [All Settings](/versions/v26.8.1/develop/configuration/all-settings#circuit-protection).
