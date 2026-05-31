---
search: false
---

# Orchestrator Runtime

The orchestrator runtime is the generated part of a TPF application that starts the pipeline, calls each step in order, tracks execution state when configured, and exposes generated REST, gRPC, LOCAL, or FUNCTION-platform entry points.

## Guide Pages

1. [Overview](/versions/v26.5.2/guide/development/orchestrator-runtime/) covers runtime modes, generated entry points, and generated structure.
2. [Queue-Async Runtime](/versions/v26.5.2/guide/development/orchestrator-runtime/queue-async) covers durable execution, crash behaviour, idempotency, providers, and HA baseline configuration.
3. [Checkpoint Handoff](/versions/v26.5.2/guide/development/orchestrator-runtime/checkpoint-handoff) covers reliable cross-pipeline publication and admission.
4. [Await Boundaries](/versions/v26.5.2/guide/development/orchestrator-runtime/await) covers durable external suspend/resume steps.

## Runtime Modes

TPF supports two orchestrator runtime modes:

1. `SYNC` (default): in-process request/response execution.
2. `QUEUE_ASYNC`: background async execution with stored execution state, dispatch, retry, recovery, and dead-letter handling.

Set mode with:

```properties
pipeline.orchestrator.mode=SYNC
# or
pipeline.orchestrator.mode=QUEUE_ASYNC
```

Use [Queue-Async Runtime](/versions/v26.5.2/guide/development/orchestrator-runtime/queue-async) before relying on background execution in production-style environments.

## Generated Transport Entry Points

Generated orchestrator entry points use the selected transport. Transport modes are `REST`, `gRPC`, and `LOCAL`; platform modes are `COMPUTE` and `FUNCTION`.

1. REST:
   - `POST /pipeline/run`
   - `POST /pipeline/run-async`
   - `GET /pipeline/executions/{executionId}`
   - `GET /pipeline/executions/{executionId}/result`
   - `POST /pipeline/ingest`
   - `GET /pipeline/subscribe`
2. gRPC:
   - `Run`
   - `RunAsync`
   - `GetExecutionStatus`
   - `GetExecutionResult`
   - `Ingest`
   - `Subscribe`
3. LOCAL:
   - in-process client calls for same-JVM deployments

In `FUNCTION` platform mode, TPF also generates provider handler artifacts. These are not a transport mode:

   - `PipelineRunFunctionHandler`
   - `PipelineRunAsyncFunctionHandler`
   - `PipelineExecutionStatusFunctionHandler`
   - `PipelineExecutionResultFunctionHandler`

## Generated Structure

```text
orchestrator-svc/
├── src/main/java/<base>/orchestrator/service/
│   ├── PipelineRunResource.java
│   ├── OrchestratorGrpcService.java
│   ├── PipelineRunFunctionHandler.java
│   ├── PipelineRunAsyncFunctionHandler.java
│   ├── PipelineExecutionStatusFunctionHandler.java
│   └── PipelineExecutionResultFunctionHandler.java
└── src/main/resources/application.properties
```
