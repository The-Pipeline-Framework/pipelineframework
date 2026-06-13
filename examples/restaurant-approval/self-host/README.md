# Restaurant Approval Self-Hosted Coordinator

This directory is the local self-hosted coordinator reference path for `restaurant-approval`.

The default demo runs one packaged `monolith-svc` process with the generic control-plane and release-admin APIs enabled. That process also uses the local in-process transition worker, so the first self-hosted run is batteries-included: one Java process owns execution state, await state, release activation, worker availability checks, result APIs, and restaurant-order business transitions.

The separate REST worker script remains available for protocol experiments, but it is not the default adoption path.

For a compute-first HA-shaped reference with containers, DynamoDB/SQS-compatible backing services, S3-compatible release artifact storage, and a separate REST worker, use [container/](./container/README.md).

## Quick Start

From the repository root:

```bash
./examples/restaurant-approval/self-host/run-self-hosted-demo.sh --ci
```

The script packages `monolith-svc`, starts the coordinator, creates a local `pipeline-release.json`, registers and activates that release, submits accepted and declined orders through `/tpf/control-plane/...`, completes the await interaction, and verifies terminal results.

The demo client submits the generated REST input DTO as a `RAW` generic control-plane payload. The release descriptor points at the local executable JAR, and the JAR carries `pipeline-contract.json` as the worker compatibility metadata.

By default it first installs the current `framework` SNAPSHOT so the example build uses the runtime code from this checkout. For faster reruns after the local SNAPSHOT is current:

```bash
TPF_SKIP_FRAMEWORK_INSTALL=true ./examples/restaurant-approval/self-host/run-self-hosted-demo.sh --ci
```

## Incident Demo

Run the failure-visibility path:

```bash
./examples/restaurant-approval/self-host/run-self-hosted-incident.sh --ci
```

This starts the same one-process coordinator, registers and activates the release, submits an order, waits for the restaurant approval await interaction, and then completes that interaction with a deliberately invalid but API-valid restaurant decision. The resumed execution fails through the normal queue-async path, publishes through the log DLQ provider, and prints an operator triage summary.

The script also calls the admin re-drive endpoint for the failed execution. Because the demo intentionally re-drives the same invalid payload, it fails again. That is deliberate: the command proves the built-in operator control exists without pretending an unrecoverable bad payload can heal itself.

The incident script sets `TPF_ORCHESTRATOR_MAX_RETRIES=0` by default so the failure reaches the terminal path immediately. Override that variable if you want to observe retry state first.

## Local Defaults

| Variable | Default |
| --- | --- |
| `TPF_TENANT_ID` | `restaurant-demo` |
| `TPF_PIPELINE_ID` | `org.pipelineframework.restaurantapproval` |
| `TPF_AWAIT_STEP_ID` | `ProcessAwaitRestaurantDecisionService` |
| `TPF_COORDINATOR_PORT` | `8081` |
| `TPF_WORKER_PORT` | `8181` |
| `TPF_CONTROL_PLANE_TOKEN` | `restaurant-control-plane-admin-token` |
| `TPF_ADMIN_TOKEN` | `restaurant-control-plane-admin-token` |
| `TPF_RELEASE_STORE_ROOT` | `examples/restaurant-approval/monolith-svc/target/tpf-self-host/releases` |

These defaults are local/dev only. Real self-host deployments should use secret references, durable stores, and explicit operational runbooks.

For the production-ish topology, durable provider choices, secret refs, and manual upgrade/drain procedure, see [Self-Hosted Deployment](/guide/evolve/durable-coordinator/self-hosted-deployment).

For the local containerized HA reference:

```bash
./examples/restaurant-approval/self-host/container/run-container-ha-demo.sh --ci
```

The container reference keeps the same restaurant flow, but runs the coordinator and REST worker as separate containers and uses LocalStack for DynamoDB, SQS, and S3-compatible services.

## Manual Flow

Package the app:

```bash
./mvnw -f framework/pom.xml -DskipTests install
./mvnw -f examples/restaurant-approval/pom.xml -pl monolith-svc -am -DskipTests package
```

Start the coordinator:

```bash
./examples/restaurant-approval/self-host/start-coordinator.sh
```

Create, register, and activate the local release descriptor:

```bash
./examples/restaurant-approval/self-host/register-and-activate-release.sh
```

Run the accepted and declined control-plane flow:

```bash
python3 examples/restaurant-approval/self-host/demo-client.py run-flows \
  --base-url http://localhost:8081 \
  --tenant-id restaurant-demo \
  --pipeline-id org.pipelineframework.restaurantapproval \
  --await-step-id ProcessAwaitRestaurantDecisionService \
  --control-plane-token restaurant-control-plane-admin-token
```

Inspect a known execution:

```bash
python3 examples/restaurant-approval/self-host/demo-client.py status \
  --base-url http://localhost:8081 \
  --tenant-id restaurant-demo \
  --execution-id <execution-id> \
  --control-plane-token restaurant-control-plane-admin-token
```

List pending await interactions:

```bash
python3 examples/restaurant-approval/self-host/demo-client.py pending \
  --base-url http://localhost:8081 \
  --tenant-id restaurant-demo \
  --await-step-id ProcessAwaitRestaurantDecisionService \
  --control-plane-token restaurant-control-plane-admin-token
```

Read a terminal result:

```bash
python3 examples/restaurant-approval/self-host/demo-client.py result \
  --base-url http://localhost:8081 \
  --tenant-id restaurant-demo \
  --execution-id <execution-id> \
  --control-plane-token restaurant-control-plane-admin-token
```

Run the incident flow against an already-started coordinator:

```bash
python3 examples/restaurant-approval/self-host/demo-client.py run-incident \
  --base-url http://localhost:8081 \
  --tenant-id restaurant-demo \
  --pipeline-id org.pipelineframework.restaurantapproval \
  --await-step-id ProcessAwaitRestaurantDecisionService \
  --control-plane-token restaurant-control-plane-admin-token \
  --log-file examples/restaurant-approval/monolith-svc/target/tpf-self-host/logs/coordinator.log
```

Logs are written under `examples/restaurant-approval/monolith-svc/target/tpf-self-host/logs`.

## Operator Walkthrough

For a normal await flow:

1. Submit through `/tpf/control-plane/tenants/{tenantId}/executions`.
2. Poll `/tpf/control-plane/tenants/{tenantId}/executions/{executionId}` until `WAITING_EXTERNAL`.
3. Query `/tpf/control-plane/tenants/{tenantId}/interactions/pending` with the await step id.
4. Complete the interaction through `/tpf/control-plane/tenants/{tenantId}/interactions/complete`.
5. Poll status until `SUCCEEDED`, then read `/result`.

For the incident flow:

1. Submit and wait for `WAITING_EXTERNAL`.
2. Complete the await interaction with the deliberately invalid decision payload.
3. Poll status until `FAILED`.
4. Inspect `errorCode`, `errorMessage`, `attempt`, and `stepIndex` from the status response.
5. Inspect `coordinator.log` for `Execution moved to DLQ` and the matching execution id.
6. Re-drive the failed execution through `/tpf/admin/tenants/{tenantId}/executions/{executionId}/redrive` after validating downstream idempotency.

Current recovery boundary: single-execution re-drive reads the durable execution record and re-enqueues the same execution id. DLQ messages are triage evidence, not the replay source, and there is no built-in bulk DLQ-message consumer. Correct the business input or downstream dependency before re-driving, and keep downstream side effects idempotent.

## What This Proves

1. A coordinator process can submit and inspect executions without using generated `/pipeline/*` app routes.
2. Release registration, activation, artifact storage, execution pinning, and worker availability checks are exercised.
3. Local transition execution works without configuring a remote worker target.
4. Await suspension and completion happen through the hosted control-plane API.
5. Terminal failure and DLQ publication are visible in the self-host operator flow.

To prove the REST worker protocol separately, start `start-worker.sh`, configure `pipeline.orchestrator.worker.rest.base-url` and `pipeline.orchestrator.worker.rest.shared-secret` on a coordinator process, and run the same control-plane client. The automated split-process IT covers that protocol path.

For a production-ish local stack using containers and AWS-compatible substrates, use the container reference in this directory.

This is not a managed service, dynamic JAR loading, worker fleet orchestration, or production tenancy. It is the reference self-host setup for the current release.
