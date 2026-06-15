# Self-Hosted HA Milestone

The compute-first self-hosted durable coordinator milestone is credible for adoption and demos.

Users can run the durable coordinator, activate a release, register a worker, submit executions, complete awaits, inspect results, triage terminal failures, and re-drive individual executions. The remaining work is hardening, not a milestone blocker.

## Adoption Entry Points

Use the restaurant approval reference first. It is the smallest human-await path:

```bash
./examples/restaurant-approval/self-host/container/run-container-ha-demo.sh --ci
```

Use CSV Payments after that when you need the stream-await/provider proof:

```bash
./examples/csv-payments/self-host/container/run-container-ha-demo.sh --ci
TPF_CSV_AWAIT_TRANSPORT=kafka ./examples/csv-payments/self-host/container/run-container-ha-demo.sh --ci
```

The self-host HA path is `COMPUTE + QUEUE_ASYNC`: a coordinator service owns durable execution state and dispatches work to local, REST, gRPC, or SQS workers. Current `FUNCTION` support is serverless invocation/adapter support, not a TPF-owned durable HA coordinator.

## Current Proof

| Capability | Status |
| --- | --- |
| Batteries-included local coordinator demo | present in `restaurant-approval` |
| Coordinator and worker split-process proof | present in `RestaurantApprovalHostedCoordinatorRestWorkerIT` |
| Containerized HA reference | present in `restaurant-approval/self-host/container` |
| Release registration and activation | present |
| Execution pinning to active release version | present |
| Worker availability check before submit | present |
| Await pending query and completion | present |
| Accepted/declined terminal results | present |
| Failure/DLQ incident walkthrough | present in `restaurant-approval/self-host` |
| Single-execution operator re-drive | present for terminal `DLQ` and explicit `FAILED` executions |
| Operator walkthrough | present in `restaurant-approval/self-host` |
| Production-ish deployment recipe | present in [Self-Hosted Deployment](/guide/evolve/durable-coordinator/self-hosted-deployment) |
| Durable release metadata | Dynamo registry with immutable release records and append-only activation events |
| Shared/replicated artifact storage | local filesystem or S3-compatible blob store for coordinator-managed artifacts; OCI/Maven-style repositories remain preferred for native artifact forms |
| Minimal worker lifecycle | registration, heartbeat, drain, stale detection, and submit-time healthy-worker gate |
| Stream-await provider proof | CSV Payments container reference supports SQS and Kafka lanes |

## Post-Milestone Hardening

| Hardening item | Tracking |
| --- | --- |
| Bulk DLQ replay campaigns | [#406](https://github.com/The-Pipeline-Framework/pipelineframework/issues/406) |
| Append-only execution/await stores | [#396](https://github.com/The-Pipeline-Framework/pipelineframework/issues/396) |

These items are valid follow-up work, but they should not block the adoption milestone.

All-serverless durable orchestration would be a separate design. It would need a coordinator loop backed by durable services such as DynamoDB, SQS, and EventBridge-style scheduling rather than relying on a Lambda/Azure/GCP function process to hold orchestration state.
