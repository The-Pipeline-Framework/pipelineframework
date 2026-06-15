# Base Containerized Self-Hosted HA Reference

This directory runs the restaurant approval self-host path as the base compute-first HA local stack:

1. LocalStack provides DynamoDB, SQS, and S3-compatible endpoints.
2. One coordinator container owns control-plane APIs, release admin APIs, execution/await state, SQS work dispatch, DLQ publication, release metadata, artifact storage, and worker lifecycle.
3. One REST worker container hosts the same restaurant pipeline release and exposes the signed transition worker endpoint.

It uses the same `monolith-svc` image in two modes. The coordinator is configured with `pipeline.orchestrator.control-plane.require-remote-worker=true`, so it fails instead of silently falling back to local in-process execution.

## Run The Happy Path

From the repository root:

```bash
./examples/restaurant-approval/self-host/container/run-container-ha-demo.sh --ci
```

The script:

1. builds the current framework runtime/deployment modules,
2. builds the restaurant `monolith-svc` Jib image,
3. starts LocalStack,
4. creates DynamoDB tables, SQS queues, and the S3 release artifact bucket,
5. starts the REST worker and coordinator containers,
6. registers and activates `pipeline-release.json`,
7. registers a healthy REST worker for the active release,
8. runs accepted and declined approval flows through `/tpf/control-plane/...`.

Set `TPF_SKIP_FRAMEWORK_INSTALL=true` or `TPF_SKIP_CONTAINER_BUILD=true` for faster reruns after local artifacts/images are current.

## Run The Incident Path

```bash
./examples/restaurant-approval/self-host/container/run-container-ha-incident.sh --ci
```

This starts the same container stack, completes the await interaction with an intentionally invalid but API-valid restaurant decision, observes terminal failure through the durable coordinator, and calls the admin re-drive endpoint. The re-drive intentionally fails again because the bad payload is unchanged; this proves operator control without pretending unrecoverable input fixes itself.

## Local Defaults

| Variable | Default |
| --- | --- |
| `TPF_RESTAURANT_IMAGE` | `localhost/restaurant-approval/monolith-svc:latest` |
| `TPF_TENANT_ID` | `restaurant-demo` |
| `TPF_PIPELINE_ID` | `org.pipelineframework.restaurantapproval` |
| `TPF_COORDINATOR_PORT` | `8081` |
| `TPF_WORKER_PORT` | `8181` |
| `TPF_LOCALSTACK_PORT` | `4566` |
| `TPF_CONTROL_PLANE_TOKEN` | `restaurant-control-plane-admin-token` |
| `TPF_ADMIN_TOKEN` | `restaurant-control-plane-admin-token` |
| `TPF_WORKER_SECRET` | `restaurant-transition-worker-secret` |

These defaults are for local verification only. Real deployments should use secret references, platform-managed credentials, and externally managed DynamoDB/SQS/S3 resources.

## What This Proves

1. The coordinator can run as its own service and dispatch transition work to a separate worker service.
2. Execution, await, release, and worker lifecycle metadata use DynamoDB-compatible stores.
3. Work dispatch and execution DLQ use SQS-compatible queues.
4. Release artifact storage can use an S3-compatible blob store for coordinator-managed artifacts.
5. Worker lifecycle must report a healthy worker for the active release before hosted submissions are accepted.

This is still a local reference stack, not a production deployment package. For milestone status and deferred hardening, see [Self-Hosted HA Roadmap](/guide/evolve/durable-coordinator/self-hosted-ha-roadmap).
