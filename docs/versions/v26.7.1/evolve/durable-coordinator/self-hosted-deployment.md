---
search: false
---

# Self-Hosted Deployment Recipe

This page describes the current production-ish self-host shape for the durable coordinator. It is not a deployment stack or a managed service contract. It is a recipe for operators who want to run a compute-first coordinator with durable stores, explicit worker boundaries, and known manual procedures.

The runnable starting point remains `examples/restaurant-approval/self-host`. That example proves the same control-plane, release, await, result, and failure/DLQ paths in one local process. The containerized HA reference in `examples/restaurant-approval/self-host/container` runs the same flow with a coordinator container, REST worker container, and LocalStack-backed DynamoDB/SQS/S3-compatible services.

`examples/csv-payments/self-host/container` is the advanced container reference. It adds stream input, app persistence, a REST transition worker, and a grouped `pipeline-runtime-svc` gRPC step runtime on top of the same durable coordinator pattern. The default lane uses SQS to stay within the LocalStack-backed AWS-shaped substrate; `TPF_CSV_AWAIT_TRANSPORT=kafka` runs the same self-host topology with Kafka await completions.

For the role split behind coordinator, transition worker, step/runtime services, and the historical `orchestrator-svc` name, see [Coordinator And Worker Topology](/versions/v26.7.1/evolve/durable-coordinator/coordinator-worker-topology).

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

The coordinator process enables the control-plane and admin APIs, owns execution/await state, and selects a remote transition worker through configured REST or gRPC target properties. Worker processes host the same pipeline release code and expose the default-disabled worker endpoint or service.

The same image can run in both roles when it contains the required pipeline code. The role is determined by config, not by the Maven module name.

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

This is the TPF-owned HA path. Current `FUNCTION` builds are serverless invocation artifacts; they do not own durable execution records, await units, leases, DLQ/re-drive, or release pinning inside the function runtime. An all-serverless durable coordinator would require a different architecture backed by durable services such as DynamoDB, SQS, and EventBridge-style scheduling; see [All-Serverless Durable Coordinator](/versions/v26.7.1/evolve/durable-coordinator/all-serverless-coordinator).

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
pipeline.orchestrator.dynamo.release-table=tpf_release_registry
pipeline.orchestrator.dynamo.worker-table=tpf_worker_registry
pipeline.orchestrator.dynamo.region=eu-west-1
pipeline.orchestrator.sqs.region=eu-west-1

pipeline.orchestrator.control-plane.enabled=true
pipeline.orchestrator.control-plane.admin-token-ref=env:TPF_CONTROL_PLANE_TOKEN
pipeline.orchestrator.control-plane.require-remote-worker=true
pipeline.orchestrator.admin.enabled=true
pipeline.orchestrator.admin.admin-token-ref=env:TPF_ADMIN_TOKEN

pipeline.orchestrator.releases.registry.provider=dynamo
pipeline.orchestrator.releases.storage.provider=s3
pipeline.orchestrator.releases.storage.s3.bucket=tpf-release-artifacts
pipeline.orchestrator.releases.storage.s3.prefix=tpf/releases
pipeline.orchestrator.releases.storage.s3.region=eu-west-1

pipeline.orchestrator.worker.lifecycle.provider=dynamo
pipeline.orchestrator.worker.lifecycle.stale-after=PT2M
```

The await interaction table must provide these ALL-projected GSIs:

1. `await-interaction-by-unit`,
2. `await-interaction-pending-by-tenant`,
3. `await-interaction-pending-by-assignee`,
4. `await-interaction-pending-by-group`,
5. `await-interaction-pending-by-step`,
6. `await-interaction-pending-by-deadline`.

SQS request/reply worker protocol has one additional v1 constraint: `pipeline.orchestrator.worker.sqs.response-queue-url` must be dedicated per coordinator instance or shard. Shared response queues can route a worker response to the wrong process because response demultiplexing is not implemented.

The Dynamo release registry stores immutable release records plus append-only activation events. Active release lookup reads the latest activation event for the tenant and pipeline; it does not update a mutable active pointer. The Dynamo worker lifecycle registry follows the same rule with append-only registration, heartbeat, and drain events. Existing execution and await Dynamo stores still use conditional updates for leases and state transitions until that storage model is redesigned.

For one-process local development, use `pipeline.orchestrator.releases.storage.provider=local` with `pipeline.orchestrator.releases.storage.root=/var/lib/tpf/releases`.

For multi-coordinator self-host deployments, choose the artifact backing system by artifact form:

| Artifact form | Recommended backing system |
| --- | --- |
| Container worker image, including Jib output | OCI registry such as ECR, GHCR, JFrog, Harbor, or Docker registry. Reference by digest in `pipeline-release.json`; do not copy image layers into the coordinator artifact store. |
| JVM JAR | Maven/JFrog/Nexus when it is a published JVM artifact, or S3-compatible blob storage when the coordinator must manage the blob directly. |
| Native binary | OCI generic artifact, local managed filesystem for single-host, or S3-compatible blob storage for shared self-host access. |
| Lambda ZIP | S3 or S3-compatible object storage. |
| Lambda container image | OCI registry, usually ECR on AWS. |
| External endpoint | Existing deployment/service discovery; the descriptor pins endpoint identity and digest/provenance metadata where available. |

The S3-compatible provider is therefore a shared blob-store option, not the default artifact repository strategy. AWS S3, MinIO, and LocalStack-style endpoints fit this model; use `endpoint-override` and `path-style-access=true` for non-AWS S3-compatible stores.

The restaurant container reference demonstrates this baseline locally:

```bash
./examples/restaurant-approval/self-host/container/run-container-ha-demo.sh --ci
```

It uses LocalStack to create the required DynamoDB tables, SQS work/DLQ queues, and S3-compatible release artifact bucket. Treat that as local verification of the topology, not production AWS provisioning.

The same reference includes a process-restart recovery proof:

```bash
./examples/restaurant-approval/self-host/container/run-container-ha-recovery.sh --ci
```

That script submits an execution, waits until it is parked on an await unit, restarts the coordinator, verifies status and pending await state are still readable, completes the await, and verifies the terminal result. It then repeats the flow with a worker restart before await completion. This proves recovery at a deterministic await boundary; it does not claim arbitrary mid-transition crash injection.

The CSV Payments container reference demonstrates the same baseline with broker-backed await completions and the example persistence path:

```bash
./examples/csv-payments/self-host/container/run-container-ha-demo.sh --ci
```

The SQS lane is the default AWS-shaped proof. The Kafka lane proves the same await abstraction against a second provider:

```bash
TPF_CSV_AWAIT_TRANSPORT=kafka ./examples/csv-payments/self-host/container/run-container-ha-demo.sh --ci
```

CSV await item continuations use the same bounded transition-worker seam as normal queue-async work. The worker executes each item continuation segment until the itemized unit reaches the next aggregate or terminal boundary. In the connector-first CSV path, terminal Object Publish owns output object writes before execution success is committed, while generated step clients target the runtime and persistence containers.

## Startup Checklist

Before accepting work:

1. Build the pipeline artifact and confirm it contains `META-INF/pipeline/pipeline-contract.json`.
2. Start durable substrates first: execution tables, await tables and indexes, work queue, DLQ queue, and any worker protocol queues.
3. Start worker processes with the matching pipeline code and worker protocol secret.
4. Start the coordinator with `strict-startup=true`.
5. Produce a `pipeline-release.json` that pins the built artifacts by digest.
6. Register and activate the release for the tenant and pipeline.
7. Register or heartbeat at least one worker for the active contract/release identity.
8. Submit one canary execution and verify status, pending await interaction, completion, and result.

The current coordinator does not dynamically load registered code. Release registration validates the release descriptor and, for local executable artifacts, validates and stores the artifact in the configured release artifact store. Container images remain in the OCI registry; the coordinator records their immutable reference and uses worker capability checks to verify that deployed workers host the active release. Workers must already host matching code. Worker availability checks verify the active contract/release identity before hosted execution submission, artifact id/digest when both sides provide it, and a matching `HEALTHY` worker lifecycle record. Stale, draining, and unavailable workers reject new hosted submissions with `503`.

`pipeline.orchestrator.control-plane.require-remote-worker=true` is recommended for separated self-host deployments. It prevents a coordinator process from silently falling back to the local in-process worker when no REST, gRPC, or SQS worker target is configured. Leave it disabled for the one-process local demo. See [Runtime Boundaries And Performance](/versions/v26.7.1/evolve/durable-coordinator/runtime-boundaries-performance).

## Operator Runbooks

### Register And Activate

1. Register the release descriptor with `POST /tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/releases/register`.
2. Read the returned `releaseVersion`.
3. Activate with `POST /tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/releases/{releaseVersion}/activate`.
4. Confirm `GET /active` returns the expected release, contract version, and artifact identity.
5. Register or heartbeat a worker with `POST /tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/workers/register`.
6. Keep workers for that release healthy before submitting executions.

Activation affects new executions only. Existing executions remain pinned to the contract/release identity stored on their execution record.

### Submit And Complete Await

1. Submit through `POST /tpf/control-plane/tenants/{tenantId}/executions`.
2. Poll `GET /executions/{executionId}`.
3. If the execution parks on await, query `GET /interactions/pending`.
4. Complete with `POST /interactions/complete`.
5. Poll terminal status and then read `GET /executions/{executionId}/result`.

The restaurant self-host client demonstrates this flow through `run-self-hosted-demo.sh --ci`.

The containerized HA reference demonstrates the same flow through `self-host/container/run-container-ha-demo.sh --ci`.

### Incident Triage

Use the restaurant incident demo as the current failure-visibility proof:

```bash
./examples/restaurant-approval/self-host/run-self-hosted-incident.sh --ci
```

For the containerized HA reference:

```bash
./examples/restaurant-approval/self-host/container/run-container-ha-incident.sh --ci
```

For real incidents:

1. Check execution status and read `errorCode`, `errorMessage`, `attempt`, and `stepIndex`.
2. Confirm whether the execution is retrying, failed, or terminally DLQ'd.
3. Inspect coordinator logs or the configured execution DLQ for the matching execution id.
4. Correct the downstream dependency or input path that caused the failure.
5. Confirm downstream idempotency before any re-drive.
6. Re-drive a terminal execution with `POST /tpf/admin/tenants/{tenantId}/executions/{executionId}/redrive`.

Re-drive reads the durable execution record and re-enqueues the original execution id. The DLQ message is evidence for triage and alerting; it is not consumed as the replay source. `FAILED` execution re-drive is opt-in (`allowFailed=true`) because those failures may not have exhausted the DLQ path.

### Process Restart Recovery

Use the restaurant recovery proof as the current restart runbook:

```bash
./examples/restaurant-approval/self-host/container/run-container-ha-recovery.sh --ci
```

For coordinator restarts:

1. Confirm execution status is still readable through `GET /tpf/control-plane/tenants/{tenantId}/executions/{executionId}`.
2. Confirm pending await interaction is still queryable.
3. Complete the await and verify the execution resumes to `SUCCEEDED`.

For worker restarts:

1. Wait for the worker health endpoint.
2. Register or heartbeat the worker lifecycle record for the active release.
3. Complete or re-drive work only after a matching `HEALTHY` worker is visible.

The current recovery proof intentionally parks at an await boundary before restarting processes. Mid-transition crash/lease-takeover campaigns are a separate hardening track because they need deterministic failure injection and stricter assertions around in-flight side effects.

### Manual Upgrade And Drain

The current safe upgrade procedure is conservative:

1. Deploy workers that host the new release version.
2. Register or heartbeat the new workers for the new release.
3. Register and activate the new release.
4. Submit canary executions and verify worker availability and results.
5. Mark old workers draining when they should stop accepting new hosted submissions.
6. Leave old workers running until executions pinned to the old release drain.
7. Stop old workers only after status queries show no active executions for the old release.

The lifecycle model is intentionally small. It records `HEALTHY`, `STALE`, `DRAINING`, and `UNAVAILABLE` views for submit admission. It does not autoscale workers, choose among worker pools, manage Kubernetes deployments, or move in-flight executions between workers.

## What To Monitor

At minimum:

1. work queue depth and oldest message age,
2. execution status distribution: running, waiting external, retrying, failed, DLQ,
3. lease takeover and sweeper activity,
4. await pending count and oldest pending deadline,
5. DLQ publication count and repeated failure fingerprints,
6. worker protocol latency and transport failures,
7. release activation events and active release id per tenant/pipeline,
8. worker lifecycle records by tenant, pipeline, release, and state.

For metric names and observability surfaces, use the operations guides for [Metrics](/versions/v26.7.1/operate/observability/metrics), [Replay & Live Topology](/versions/v26.7.1/operate/observability/replay), and the [Operator Playbook](/versions/v26.7.1/operate/operators-playbook).

## Current Limits

This recipe intentionally does not include:

1. production Kubernetes, Helm, Terraform, or IAM packaging,
2. dynamic JAR loading in the coordinator,
3. append-only execution/await state storage,
4. bulk DLQ-message consumers or automated replay campaigns,
5. worker autoscaling, fleet routing, or deployment orchestration,
6. production tenancy, RBAC, or org/principal management.

The file-backed release registry is suitable for local/dev and single-coordinator self-host pilots. Use the Dynamo release registry for multi-coordinator release metadata. Use the S3-compatible release artifact store only for artifacts that should be coordinator-managed blobs; use OCI or Maven-style repositories for artifacts that already have native repository semantics.
