# Restaurant Approval Self-Hosted Coordinator

This directory is the local self-hosted coordinator reference path for `restaurant-approval`.

It runs the same packaged monolith twice:

1. one process as a REST transition worker,
2. one process as the coordinator with generic control-plane and bundle-admin APIs enabled.

The coordinator owns execution state, await state, bundle activation, worker availability checks, and result APIs. The worker executes restaurant-order business transitions through the signed REST worker protocol.

## Quick Start

From the repository root:

```bash
./examples/restaurant-approval/self-host/run-self-hosted-demo.sh --ci
```

The script packages the monolith, starts both processes, registers and activates the generated bundle JAR, submits accepted and declined orders through `/tpf/control-plane/...`, completes the await interaction, and verifies terminal results.

By default it first installs the current `framework` SNAPSHOT so the example build uses the runtime code from this checkout. For faster reruns after the local SNAPSHOT is current:

```bash
TPF_SKIP_FRAMEWORK_INSTALL=true ./examples/restaurant-approval/self-host/run-self-hosted-demo.sh --ci
```

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
| `TPF_WORKER_SECRET` | `restaurant-transition-worker-secret` |
| `TPF_BUNDLE_STORE_ROOT` | `examples/restaurant-approval/monolith-svc/target/tpf-self-host/bundles` |

These defaults are local/dev only. Real self-host deployments should use secret references, durable stores, and explicit operational runbooks.

## Manual Flow

Package the app:

```bash
./mvnw -f framework/pom.xml -DskipTests install
./mvnw -f examples/restaurant-approval/pom.xml -pl monolith-svc -am -DskipTests package
```

Start the worker:

```bash
./examples/restaurant-approval/self-host/start-worker.sh
```

Start the coordinator:

```bash
./examples/restaurant-approval/self-host/start-coordinator.sh
```

Register and activate the generated bundle:

```bash
./examples/restaurant-approval/self-host/register-and-activate-bundle.sh
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

Logs are written under `examples/restaurant-approval/monolith-svc/target/tpf-self-host/logs`.

## What This Proves

1. A coordinator process can submit and inspect executions without using generated `/pipeline/*` app routes.
2. Bundle registration, activation, artifact storage, execution pinning, and worker availability checks are exercised.
3. A separate worker process executes transitions through the REST worker protocol.
4. Await suspension and completion happen through the hosted control-plane API.

This is not a managed service, dynamic JAR loading, worker fleet lifecycle, or production tenancy. It is the public self-host adoption path for the current runtime seams.
