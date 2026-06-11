# Self-Hosted Deployment Recipe

This page describes the current production-ish self-host shape for the durable coordinator. It is not a deployment stack or a managed service contract. It is a recipe for operators who want to run the coordinator with durable stores, explicit worker boundaries, and known manual procedures.

The runnable starting point remains `examples/restaurant-approval/self-host`. That example proves the same control-plane, release, await, result, and failure/DLQ paths in one local process.

## Deployment Shapes

### One Process

Use this shape for local adoption and demos.

One packaged application process enables:

1. generic control-plane API,
2. release admin API,
3. file release registry and artifact store,
4. local in-process transition worker,
5. memory/event/log providers by default.

This is the fastest way to prove the model, but it is not crash-surviving HA. If the process dies, in-memory execution state is gone.

### Coordinator And Worker Processes

Use this shape when you want a real control-plane/data-plane boundary.

The coordinator process enables the control-plane and admin APIs, owns execution/await state, and selects a remote transition worker through configured REST or gRPC target properties. Worker processes host the same pipeline bundle code and expose the default-disabled worker endpoint or service.

REST worker selection example:

```properties
pipeline.orchestrator.worker.rest.base-url=http://restaurant-worker:8181
pipeline.orchestrator.worker.rest.shared-secret-ref=env:TPF_WORKER_SECRET
```

Worker process example:

```properties
pipeline.orchestrator.worker.rest.server-enabled=true
pipeline.orchestrator.worker.rest.shared-secret-ref=env:TPF_WORKER_SECRET
```

gRPC follows the same model with `pipeline.orchestrator.worker.grpc.endpoint`, `grpc.server-enabled`, and the matching shared secret or secret ref.

### Durable Coordinator Baseline

Use this shape for production-style recovery tests and self-host pilots.

The coordinator persists execution and await state in DynamoDB-style tables, dispatches work through SQS-style queues, and publishes terminal execution failures to a durable DLQ. Workers can still be local, REST, gRPC, or SQS request/reply workers, but a separated REST or gRPC worker is the clearer operational boundary.

Minimum coordinator configuration:

```properties
pipeline.orchestrator.mode=QUEUE_ASYNC
pipeline.orchestrator.strict-startup=true
pipeline.orchestrator.idempotency-policy=CLIENT_KEY_REQUIRED
pipeline.orchestrator.execution-ttl-days=7
pipeline.orchestrator.lease-ms=30000
pipeline.orchestrator.max-retries=3
pipeline.orchestrator.retry-delay=PT10S
pipeline.orchestrator.retry-multiplier=2.0
pipeline.orchestrator.sweep-interval=PT30S
pipeline.orchestrator.sweep-limit=100

pipeline.orchestrator.state-provider=dynamo
pipeline.orchestrator.dispatcher-provider=sqs
pipeline.orchestrator.dlq-provider=sqs
pipeline.orchestrator.queue-url=https://sqs.eu-west-1.amazonaws.com/123456789012/tpf-work
pipeline.orchestrator.dlq-url=https://sqs.eu-west-1.amazonaws.com/123456789012/tpf-execution-dlq

pipeline.orchestrator.dynamo.execution-table=tpf_execution
pipeline.orchestrator.dynamo.execution-key-table=tpf_execution_key
pipeline.orchestrator.dynamo.await-interaction-table=tpf_await_interaction
pipeline.orchestrator.dynamo.await-interaction-key-table=tpf_await_interaction_key
pipeline.orchestrator.dynamo.region=eu-west-1
pipeline.orchestrator.sqs.region=eu-west-1

pipeline.orchestrator.control-plane.enabled=true
pipeline.orchestrator.control-plane.admin-token-ref=env:TPF_CONTROL_PLANE_TOKEN
pipeline.orchestrator.admin.enabled=true
pipeline.orchestrator.admin.admin-token-ref=env:TPF_ADMIN_TOKEN

pipeline.orchestrator.bundles.registry.provider=file
pipeline.orchestrator.bundles.storage.root=/var/lib/tpf/bundles
```

The await interaction table must provide these ALL-projected GSIs:

1. `await-interaction-by-unit`,
2. `await-interaction-pending-by-tenant`,
3. `await-interaction-pending-by-assignee`,
4. `await-interaction-pending-by-group`,
5. `await-interaction-pending-by-step`,
6. `await-interaction-pending-by-deadline`.

SQS request/reply worker protocol has one additional v1 constraint: `pipeline.orchestrator.worker.sqs.response-queue-url` must be dedicated per coordinator instance or shard. Shared response queues can route a worker response to the wrong process because response demultiplexing is not implemented.

## Startup Checklist

Before accepting work:

1. Build the pipeline artifact and confirm it contains `META-INF/pipeline/pipeline-contract.json` and `META-INF/pipeline/bundle-manifest.json`.
2. Start durable substrates first: execution tables, await tables and indexes, work queue, DLQ queue, and any worker protocol queues.
3. Start worker processes with the matching pipeline code and worker protocol secret.
4. Start the coordinator with `strict-startup=true`.
5. Produce a `pipeline-release.json` that pins the built artifacts by digest.
6. Register and activate the release for the tenant and pipeline.
7. Submit one canary execution and verify status, pending await interaction, completion, and result.

The current coordinator does not dynamically load registered JAR code. Release registration validates the release descriptor and, for local executable JAR artifacts, validates and stores the artifact. Workers must already host matching code. Worker availability checks verify the active release identity plus the current executable manifest identity before hosted execution submission.

## Operator Runbooks

### Register And Activate

1. Register the release descriptor with `POST /tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/releases/register`.
2. Read the returned `releaseVersion`.
3. Activate with `POST /tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/releases/{releaseVersion}/activate`.
4. Confirm `GET /active` returns the expected release, contract version, and artifact identity.
5. Keep workers for that release running before submitting executions.

Activation affects new executions only. Existing executions remain pinned to the contract/release identity stored on their execution record.

### Submit And Complete Await

1. Submit through `POST /tpf/control-plane/tenants/{tenantId}/executions`.
2. Poll `GET /executions/{executionId}`.
3. If the execution parks on await, query `GET /interactions/pending`.
4. Complete with `POST /interactions/complete`.
5. Poll terminal status and then read `GET /executions/{executionId}/result`.

The restaurant self-host client demonstrates this flow through `run-self-hosted-demo.sh --ci`.

### Incident Triage

Use the restaurant incident demo as the current failure-visibility proof:

```bash
./examples/restaurant-approval/self-host/run-self-hosted-incident.sh --ci
```

For real incidents:

1. Check execution status and read `errorCode`, `errorMessage`, `attempt`, and `stepIndex`.
2. Confirm whether the execution is retrying, failed, or terminally DLQ'd.
3. Inspect coordinator logs or the configured execution DLQ for the matching execution id.
4. Confirm downstream idempotency before any manual re-drive.
5. Re-submit or re-drive through an application-owned procedure with stable business idempotency keys.

There is no built-in generic DLQ replay endpoint yet. The DLQ proves durable failure publication; replay ownership remains application/operator-owned.

### Manual Upgrade And Drain

The current safe upgrade procedure is conservative:

1. Deploy workers that host the new bundle version.
2. Register and activate the new release.
3. Submit canary executions and verify worker availability and results.
4. Leave old workers running until executions pinned to the old bundle drain.
5. Stop old workers only after status queries show no active executions for the old bundle.

There is no worker lifecycle registry yet. Operators must track healthy, stale, draining, and unavailable worker states outside the framework for now.

## What To Monitor

At minimum:

1. work queue depth and oldest message age,
2. execution status distribution: running, waiting external, retrying, failed, DLQ,
3. lease takeover and sweeper activity,
4. await pending count and oldest pending deadline,
5. DLQ publication count and repeated failure fingerprints,
6. worker protocol latency and transport failures,
7. release activation events and active release id per tenant/pipeline.

For metric names and observability surfaces, use the operations guides for [Metrics](/guide/operations/observability/metrics), [Replay & Live Topology](/guide/operations/observability/replay), and the [Operator Playbook](/guide/operations/operators-playbook).

## Current Limits

This recipe intentionally does not include:

1. Kubernetes manifests or Docker Compose files,
2. dynamic JAR loading in the coordinator,
3. multi-instance HA bundle registry metadata,
4. built-in generic DLQ replay,
5. worker registration, heartbeat, or drain state,
6. production tenancy, RBAC, or org/principal management.

The file-backed bundle registry is suitable for local/dev and single-coordinator self-host pilots. A serious multi-coordinator deployment still needs a durable registry metadata store before the file registry can be considered HA-ready.
