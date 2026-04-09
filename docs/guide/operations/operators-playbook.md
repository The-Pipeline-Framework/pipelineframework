# Operator Runbook

This runbook is for operating and debugging pipelines that execute operator methods.

## Terminology

- `lane` (alias: `command path`): a reproducible build/test/run sequence for one execution mode or scope.
- `parked item`: an item that exhausted configured retries or was classified as non-retryable and moved out of the hot path.
- `parking queue` (or parking area): the operational store/queue where parked items are retained for triage and replay.

## CI-Equivalent Execution Commands

Use the same command families used in validation lanes (command paths):

```bash
# Whole repository verification
./mvnw verify

# Framework-only verification
./mvnw -f framework/pom.xml verify
```

Optional example path (Search reference project):

```bash
./mvnw -f examples/search/pom.xml -pl orchestrator-svc -am \
  -Dpipeline.platform=FUNCTION \
  -Dpipeline.transport=REST \
  -Dpipeline.rest.naming.strategy=RESOURCEFUL \
  -DskipTests compile
```

FTGo reference command paths:

```bash
# Lineage determinism checks (runtime focus)
./mvnw -f framework/pom.xml -pl runtime -Dtest=FunctionTransportAdaptersTest test

# Parity checks (FUNCTION local/remote routing)
./mvnw -f framework/pom.xml -pl runtime -Dtest=FunctionTransportContextTest,InvocationModeRoutingParityTest test

# Checkout bridge parity smoke
./mvnw -f examples/checkout/pom.xml -pl create-order-orchestrator-svc,deliver-order-orchestrator-svc -am test -DskipITs

# Branching lane reliability checks
./mvnw -f examples/search/common/pom.xml install -DskipTests
./mvnw -f examples/search/index-document-svc/pom.xml -Dtest=ProcessIndexDocumentServiceReliabilityTest test
```

## Run Modes and Command Paths

### Compute/REST mode

- Build transport and platform defaults from `pipeline.yaml` (the pipeline manifest, typically at the repo root or a service `config/` directory; see [Configuration Reference](/guide/build/configuration/)).
- Use module-local Quarkus run/test commands for step services and orchestrator.
- Expect generated REST handlers/resources for configured steps.

### Function/REST mode

- Build with:
  - `-Dpipeline.platform=FUNCTION`
  - `-Dpipeline.transport=REST`
  - `-Dpipeline.rest.naming.strategy=RESOURCEFUL`
- Validate handler path with `LambdaMockEventServerSmokeTest`.

## Signals to Watch

### Health

- Quarkus health endpoints (`/q/health`) for service readiness/liveness.
- Generated handler/resource availability in startup logs.

### Metrics and logs

- Step latency trends around fan-out and fan-in boundaries.
- Error-rate spikes grouped by service/step.
- Retry exhaustion and parking events (for example, index reducer parking logs).
- Backpressure symptoms: sustained queue growth or long tail latency in streaming/reduction steps.

### Build artifact integrity

- Confirm `META-INF/pipeline/*` metadata exists in built artifacts.
- Validate generated handlers/adapters are present in expected module outputs.

## Recovery Playbook

### Queue-Async execution triage (`QUEUE_ASYNC`)

Use this flow when orchestrator async executions stall or fail:

1. Check execution status via transport-native status API (`/executions/{id}` or `GetExecutionStatus`).
2. Confirm whether status is `WAIT_RETRY`, `FAILED`, or `DLQ`.
3. Inspect latest retry attempt and error classification.
4. If execution is due but not progressing, verify sweeper activity and dispatcher health.
5. Re-drive only after validating idempotency at downstream operator boundaries.

Fast triage checklist:

1. Confirm provider wiring (`state-provider`, `dispatcher-provider`) and queue URL at runtime.
2. Confirm backlog behavior (queue age/depth) versus execution status progression.
3. Check whether failures are stale-commit races (expected/no-op) or true terminal failures.
4. Check lease expiration/takeover behavior before forcing manual replay.

### Retry exhaustion

1. Identify the failing step and failure type (transient vs non-retryable).
2. Confirm whether the failure is dependency/systemic or payload/data specific.
3. If systemic: stabilise dependency first, then replay.
4. If data-specific: isolate failing payloads and route to item reject sink for single-item/data-level failures, or to execution DLQ for job/task-level or systemic execution failures.

### Checkpoint Publication Backlog and Handoff Failures

1. Treat checkpoint publication backlog as pre-execution pressure: work has not yet been admitted into downstream orchestration.
2. Separate publication rejects and downstream admission failures from execution DLQ and item reject sink incidents.
3. For checkpoint publication incidents, check downstream async ingress health, duplicate-suppression counters, and handoff latency before replaying anything.
4. Use application- or broker-owned replay controls when re-driving published work; the framework does not provide a generic re-drive consumer.

### Execution DLQ Re-drive Guidance

1. Treat execution DLQ entries as at-least-once replays of full execution transitions.
2. Preserve the original transition identity (`executionId:stepIndex:attempt`) when replaying.
3. Re-drive in bounded execution batches and keep ordering by execution context when required by downstream side effects.
4. Validate downstream idempotency controls before bulk replay and monitor duplicate-suppression and stale-commit metrics during replay.

### Item Reject Re-drive Guidance

1. Treat item reject entries as at-least-once replays of item-level processing failures.
2. Preserve the originating execution and step identity; attach transition identity when available.
3. Re-drive with smaller batch sizes than execution DLQ replays, because payload skew and poison records are more common at item level.
4. Validate item-level dedupe/idempotency controls before replay and monitor item reject throughput, duplicate suppression, and repeated-fingerprint rates.

Current boundary:

1. TPF does not provide a built-in generic re-drive consumer that reads item-reject SQS messages and re-submits directly to orchestrator async endpoints.
2. Default reject envelopes are metadata-only (`pipeline.item-reject.include-payload=false`), so queue entries are often insufficient to reconstruct full replay input.
3. Item reject re-drive is application-owned by design and should follow domain-specific replay procedures.
4. Checkpoint publication replay ownership is application- or broker-operated; the framework stops at orchestrator-owned handoff admission.

Example (CSV payments style):

1. Export rejected records from sink evidence and source systems.
2. Build an ad-hoc CSV containing corrected rows.
3. Place the file in the pipeline input folder.
4. Let the normal ingestion path process that file as a controlled re-drive batch.

Recommended transition identity:

1. `executionId:stepIndex:attempt`
2. Propagate it through transport headers/metadata when replaying manually.

### Parking growth

1. Alert on sustained growth in parked failures for the same step.
2. Correlate parked entries with a specific dependency, payload signature, or rollout.
3. Mitigate by rollback/config correction, then replay parked items in controlled batches.

### Timeout pressure

1. Identify whether timeout is upstream IO, operator logic, or downstream persistence.
2. Validate traffic and payload size changes at the same timestamp.
3. Reduce load and/or increase capacity first; only then tune retry/backoff/timeout controls.

## Material Environment and Config Inputs

Only include keys that change behaviour materially:

- `tpf.function.invocation.mode`: controls local vs remote function invoke routing behaviour; invalid values fail fast with an explicit error (no silent fallback).
  - Misconfigured values now fail at startup/validation time. Verify `tpf.function.invocation.mode` against supported modes (`LOCAL`, `REMOTE`) before deployment.
- `pipeline.platform`: selects platform generation mode (For example `FUNCTION`).
- `pipeline.transport`: selects transport generation mode (for example `REST`).
- `pipeline.rest.naming.strategy`: affects generated REST naming and route conventions.
- `quarkus.lambda.handler`: selects explicit lambda handler entrypoint when multiple handlers exist.

## Intentional Limitations (Current)

- Unary operator invocation is the primary supported execution path.
- gRPC delegated/operator paths require descriptors and mapper-compatible bindings.
- No implicit mapper conversion by default; fallback behaviour is configuration-driven.
- Operational controls are service-specific; there is no single global operator circuit-breaker switch.

## Related

- [Operator Troubleshooting Matrix](/guide/operations/operators-troubleshooting)
- [Operator Build Troubleshooting](/guide/development/operators-build-troubleshooting)
- [Error Handling and Recovery](/guide/operations/error-handling)
- [Observability](/guide/operations/observability/)
