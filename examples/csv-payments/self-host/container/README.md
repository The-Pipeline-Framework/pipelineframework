# CSV Payments Containerized Self-Hosted HA Reference

This directory runs the CSV Payments example as the advanced compute-first self-host HA reference:

1. LocalStack provides DynamoDB, SQS, and S3-compatible coordinator stores.
2. SQS carries the await request/completion flow for the mock payment provider.
3. Postgres backs the CSV example persistence plugin path.
4. One coordinator container owns control-plane APIs, release admin APIs, execution/await state, SQS dispatch, DLQ publication, release metadata, artifact storage, and worker lifecycle.
5. One REST transition worker container hosts the canonical `orchestrator-svc` pipeline order and delegates generated step calls to the grouped runtime and persistence services.
6. One `pipeline-runtime-svc` container hosts the grouped gRPC step services and SQS mock provider.

Restaurant approval remains the smaller human-await reference. This stack proves the same coordinator model against stream input plus SQS-backed await completions. The default CSV example still uses Kafka as the event-stream await proof; this container path is the AWS-shaped self-host HA reference.

## Run The Demo

From the repository root:

```bash
./examples/csv-payments/self-host/container/run-container-ha-demo.sh --ci
```

The script:

1. builds the current framework,
2. builds CSV `orchestrator-svc`, `pipeline-runtime-svc`, and `persistence-svc` Jib images with gRPC step transport and the SQS await pipeline config,
3. starts LocalStack, Postgres, persistence, runtime, worker, and coordinator containers,
4. creates DynamoDB tables, SQS work/DLQ/await queues, and an S3 release bucket,
5. registers and activates a release from the `orchestrator-svc` artifact,
6. registers a healthy REST worker for the active release,
7. submits `payments_12.csv` through `/tpf/control-plane/...`,
8. waits for SQS await completions and verifies generated `.out` files.

Set `TPF_SKIP_FRAMEWORK_INSTALL=true` or `TPF_SKIP_CONTAINER_BUILD=true` for faster reruns after local artifacts/images are current.

## Local Defaults

| Variable | Default |
| --- | --- |
| `TPF_CSV_COORDINATOR_IMAGE` | `localhost/csv-payments/orchestrator-svc:latest` |
| `TPF_CSV_WORKER_IMAGE` | `localhost/csv-payments/orchestrator-svc:latest` |
| `TPF_CSV_RUNTIME_IMAGE` | `localhost/csv-payments/pipeline-runtime-svc:latest` |
| `TPF_CSV_PERSISTENCE_IMAGE` | `localhost/csv-payments/persistence-svc:latest` |
| `TPF_TENANT_ID` | `csv-demo` |
| `TPF_PIPELINE_ID` | `org.pipelineframework.csv` |
| `TPF_COORDINATOR_PORT` | `8082` |
| `TPF_WORKER_PORT` | `8182` |
| `TPF_PERSISTENCE_PORT` | `8282` |
| `TPF_LOCALSTACK_PORT` | `4567` |
| `TPF_CONTROL_PLANE_TOKEN` | `csv-control-plane-admin-token` |
| `TPF_ADMIN_TOKEN` | `csv-control-plane-admin-token` |
| `TPF_WORKER_SECRET` | `csv-transition-worker-secret` |

These defaults are for local verification only. Real deployments should use secret references, platform-managed credentials, externally managed substrates, and immutable artifact references.

## What This Proves

1. The durable coordinator can submit and observe a stream-shaped CSV pipeline through the generic control-plane API.
2. A separated REST worker can execute the grouped pipeline runtime while the coordinator owns durable execution, await, release, worker lifecycle, work queue, and DLQ state.
3. SQS await dispatch/completion works in the self-host shape.
4. The example persistence path can run beside the durable coordinator substrates.

This is still a local reference stack, not a production deployment package.

## Runtime Boundary Note

SQS await completions resume item continuations through the same bounded transition-worker seam used for normal queue-async work. The REST transition worker executes the continuation segment up to the aggregate boundary, while generated gRPC step calls target the runtime and persistence containers.
