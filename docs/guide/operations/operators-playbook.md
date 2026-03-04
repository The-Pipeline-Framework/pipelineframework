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

### Retry exhaustion

1. Identify the failing step and failure type (transient vs non-retryable).
2. Confirm whether the failure is dependency/systemic or payload/data specific.
3. If systemic: stabilise dependency first, then replay.
4. If data-specific: isolate failing payloads and route to DLQ (Dead Letter Queue)/parking investigation.

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
- [Error Handling & DLQ](/guide/operations/error-handling)
- [Observability](/guide/operations/observability/)
