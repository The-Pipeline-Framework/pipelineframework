---
title: All Settings Reference
---

# All Settings Reference

This page lists every supported configuration option, grouped by build-time and runtime usage.

::: tip Canonical Settings Reference
This is the long-form configuration reference. Use this page when you need the supported knobs and defaults in one place. The split pages under this guide call out task-specific slices, but they do not replace this reference.
:::

## Build-Time Configuration

These settings are read during build/compile and affect generated code or CLI generation.

### Pipeline YAML

The pipeline YAML controls global settings used by the annotation processor.

| Property    | Type | Default | Description                                                 |
|-------------|------|---------|-------------------------------------------------------------|
| `transport` | enum | `GRPC`  | Global transport for generated adapters (`GRPC`, `REST`, or `LOCAL`). |
| `platform`  | enum | `COMPUTE` | Target deployment platform (`COMPUTE` or `FUNCTION`; legacy aliases: `STANDARD`, `LAMBDA`). |

If `pipeline-config.yaml` (the template configuration produced by Canvas or the template generator) is present,
the build can also use it to generate protobuf definitions and orchestrator endpoints at compile time.

### Orchestrator CLI (Annotation)

CLI metadata is configured on the orchestrator annotation.

| Annotation Attribute | Type    | Default                       | Description                                            |
|----------------------|---------|-------------------------------|--------------------------------------------------------|
| `generateCli`        | boolean | `true`                        | Enables generation of the orchestrator CLI entrypoint. |
| `name`               | string  | `"orchestrator"`              | CLI command name.                                      |
| `description`        | string  | `"Pipeline Orchestrator CLI"` | CLI command description.                               |
| `version`            | string  | `"1.0.0"`                     | CLI command version.                                   |

Example:

```java
@PipelineOrchestrator(
    generateCli = true,
    name = "payments-orchestrator",
    description = "CSV Payments Orchestrator CLI",
    version = "1.2.0"
)
public class OrchestratorMarker {
}
```

CLI input expectations:
- `--input` / `PIPELINE_INPUT` must be a JSON object matching the input DTO.
- `--input-list` / `PIPELINE_INPUT_LIST` must be a JSON array of input DTO objects.

Examples:

```bash
./app -i '{"path":"/data/in"}'
```

```bash
./app --input-list '[{"path":"/data/a"},{"path":"/data/b"}]'
```

### Annotation Processor Options

Pass via `maven-compiler-plugin` with `-A` arguments.

| Option                                  | Type    | Default | Description                                                                                                        |
|-----------------------------------------|---------|---------|--------------------------------------------------------------------------------------------------------------------|
| `-Apipeline.generatedSourcesDir`        | path    | none    | Base directory for role-specific generated sources.                                                                |
| `-Apipeline.generatedSourcesRoot`       | path    | none    | Legacy alias of `pipeline.generatedSourcesDir`.                                                                    |
| `-Apipeline.orchestrator.generate`      | boolean | `false` | Generate orchestrator endpoint even without `@PipelineOrchestrator`. CLI generation still requires the annotation. |
| `-Apipeline.transport`                  | enum    | from YAML (`GRPC`) | Build-time transport override.                                                                                |
| `-Apipeline.platform`                   | enum    | from YAML (`COMPUTE`) | Build-time platform override (`COMPUTE` or `FUNCTION`; legacy aliases: `STANDARD`, `LAMBDA`).                                                      |
| `-Apipeline.rest.naming.strategy`       | enum    | `RESOURCEFUL` | REST endpoint naming strategy (`RESOURCEFUL` or `LEGACY`).                                                     |

Equivalent process-level overrides are also supported through:
- system properties: `pipeline.transport`, `pipeline.platform`, `pipeline.rest.naming.strategy`
- environment variables: `PIPELINE_TRANSPORT`, `PIPELINE_PLATFORM`, `PIPELINE_REST_NAMING_STRATEGY`

### REST Path Overrides (Build-Time)

The annotation processor reads `src/main/resources/application.properties` during compilation to override REST paths:

| Property                                            | Type   | Default | Description                                |
|-----------------------------------------------------|--------|---------|--------------------------------------------|
| `pipeline.rest.path.<ServiceName>`                  | string | none    | Overrides REST path by service name.       |
| `pipeline.rest.path.<fully.qualified.ServiceClass>` | string | none    | Overrides REST path by service class name. |

When `pipeline.rest.naming.strategy=RESOURCEFUL` (default), generated REST paths are:
- 1-1 (`UNARY_UNARY`) and N-1 (`STREAMING_UNARY`): `/api/v1/<output-type>`
- 1-N (`UNARY_STREAMING`) and M-N (`STREAMING_STREAMING`): `/api/v1/<input-type>`
- side effects append plugin token when available (for example `/api/v1/ack-payment-sent/persistence`)

When `pipeline.rest.naming.strategy=LEGACY`, generated REST paths stay in the older form:
- `/api/v1/process-<service>/process`

## Runtime Configuration

Prefix: `pipeline`

### Orchestrator Client Wiring (Generated)

The annotation processor generates default orchestrator client configuration at build time.
Generated values have lower priority than explicit `application.properties` settings or environment variables.

To override routing or ports, set the following in `application.properties`:

| Property                                 | Type   | Default | Description                                       |
|------------------------------------------|--------|---------|---------------------------------------------------|
| `pipeline.module.<module>.host`          | string | none    | Host for a module; Quarkus config expressions are preserved. |
| `pipeline.module.<module>.port`          | int or expression | none    | Port for a module; literal values are validated and Quarkus config expressions are preserved. |
| `pipeline.module.<module>.steps`         | list   | none    | Comma/space-separated client names to assign.     |
| `pipeline.module.<module>.aspects`       | list   | none    | Aspect names to assign (e.g. `persistence`).      |
| `pipeline.client.base-port`              | int    | `8443`  | Base port used when assigning per-module offsets. |
| `pipeline.client.tls-configuration-name` | string | none    | TLS registry name for generated clients.          |

Client names follow the same conventions as generated adapters:
- regular steps: `process-<service>` (for example `ProcessPaymentService` → `process-payment`)
- synthetic steps: `observe-<aspect>-<type>-side-effect` (for example `persistence` + `PaymentRecord`)

About modules:
- A module is a deployment/runtime group for one or more services (for example, `payments-processing-svc` hosting multiple steps).
- Use module overrides when multiple services share the same runtime or when legacy layouts bundle steps together (like `csv-payments`).
- Avoid module overrides if you want the default 1‑step‑per‑module layout; in that case, only override individual clients as needed.

Avoiding drift:
- Keep server ports and orchestrator client ports aligned by sourcing them from the same `pipeline.module.<module>.port` or shared environment variables.
- For deployment-specific endpoints, use Quarkus/MicroProfile config expressions such as `pipeline.module.pipeline-runtime-svc.host=${PIPELINE_RUNTIME_HOST:127.0.0.1}` and `pipeline.module.pipeline-runtime-svc.port=${PIPELINE_RUNTIME_GRPC_PORT:9000}`.
- If you override a client endpoint directly, verify the corresponding server listens on the same host/port (especially when TLS is enabled).

If you need to override a single client endpoint, set the Quarkus property directly:

```properties
quarkus.grpc.clients.<client>.host=host
quarkus.grpc.clients.<client>.port=8444
quarkus.grpc.clients.<client>.tls-configuration-name=pipeline-client
quarkus.rest-client.<client>.url=https://host:8444
quarkus.rest-client.<client>.tls-configuration-name=pipeline-client
```

### REST Client Endpoints

REST client steps use Quarkus REST client configuration:

| Property                                | Type   | Default | Description                      |
|-----------------------------------------|--------|---------|----------------------------------|
| `quarkus.rest-client.<client-name>.url` | string | none    | Base URL for a REST client step. |

`client-name` is derived from the service class name in kebab-case with a trailing `Service` removed (for example `ProcessPaymentService` → `process-payment`).

### Cache Configuration

Prefix: `pipeline.cache`

| Property                                      | Type     | Default           | Description                                                                                                              |
|-----------------------------------------------|----------|-------------------|--------------------------------------------------------------------------------------------------------------------------|
| `pipeline.cache.provider`                     | string   | none              | Cache provider name (for example `redis`, `caffeine`, `memory`).                                                         |
| `pipeline.cache.provider.class`               | string   | none              | Fully-qualified cache provider class name to lock selection at runtime.                                                  |
| `pipeline.cache.policy`                       | string   | `cache-only`      | Default cache policy (`prefer-cache`/`return-cached`, `cache-only`, `skip-if-present`, `require-cache`, `bypass-cache`). |
| `pipeline.cache.ttl`                          | duration | none              | Default cache TTL.                                                                                                       |
| `pipeline.cache.caffeine.name`                | string   | `pipeline-cache`  | Cache name for the Caffeine provider.                                                                                    |
| `pipeline.cache.caffeine.maximum-size`        | long     | `10000`           | Maximum cache size for the Caffeine provider.                                                                            |
| `pipeline.cache.caffeine.expire-after-write`  | duration | none              | Expire entries after write for the Caffeine provider.                                                                    |
| `pipeline.cache.caffeine.expire-after-access` | duration | none              | Expire entries after access for the Caffeine provider.                                                                   |
| `pipeline.cache.redis.prefix`                 | string   | `pipeline-cache:` | Key prefix for Redis cache entries.                                                                                      |

### Repository Materialization Configuration

Prefix: `pipeline.repository`

Repository materialization stores selected large fields out of line and keeps an inline `payload_ref` sibling field in the message. It is configured by materialization policy in the pipeline YAML; runtime properties select and tune the storage backend without changing business steps or runtime mappings.

| Property                                      | Type    | Default                 | Description                                                                          |
|-----------------------------------------------|---------|-------------------------|--------------------------------------------------------------------------------------|
| `pipeline.repository.provider`                | string  | none                    | Repository provider name (`filesystem` or `s3`).                                     |
| `pipeline.repository.provider.class`          | string  | none                    | Fully-qualified repository provider class name to lock selection at runtime.         |
| `pipeline.repository.verify-checksum`         | boolean | `true`                  | Verify stored payload checksums on load.                                             |
| `pipeline.repository.filesystem.root`         | string  | `target/tpf-repository` | Filesystem repository root for local development and deterministic tests.            |
| `pipeline.repository.s3.bucket`               | string  | none                    | S3 bucket used by the S3 repository provider.                                        |
| `pipeline.repository.s3.prefix`               | string  | empty                   | Optional key prefix for S3 objects.                                                  |
| `pipeline.repository.s3.region`               | string  | none                    | AWS region for explicitly constructed S3 clients.                                    |
| `pipeline.repository.s3.endpoint-override`    | string  | none                    | S3-compatible endpoint override, useful for LocalStack/MinIO.                        |
| `pipeline.repository.s3.path-style`           | boolean | `false`                 | Enable path-style addressing for S3-compatible stores that require it.               |

See [Field Materialization](/design/materialization) for the YAML surface.

### Persistence Configuration

Prefix: `pipeline.persistence`

| Property                              | Type   | Default | Description                                                                   |
|---------------------------------------|--------|---------|-------------------------------------------------------------------------------|
| `pipeline.persistence.duplicate-key`  | string | `fail`  | Duplicate key policy for persistence (`fail`, `ignore`, `upsert`).            |
| `persistence.provider.class` | string | none    | Fully-qualified persistence provider class name to lock selection at runtime. This key intentionally stays outside the `pipeline.persistence` prefix for provider SPI compatibility. |

### Pipeline Execution

Prefix: `pipeline`

| Property                   | Type    | Default | Description                                                 |
|----------------------------|---------|---------|-------------------------------------------------------------|
| `pipeline.parallelism`     | string  | `AUTO`  | Parallelism policy: `SEQUENTIAL`, `AUTO`, or `PARALLEL`.    |
| `pipeline.max-concurrency` | integer | `128`   | Per-step maximum in-flight items when parallel execution is enabled. |

### Orchestrator Background Execution

Use these settings when the orchestrator should accept work, store execution state, dispatch work in the background, retry failures, and publish terminal failures. The config value for that execution path is `QUEUE_ASYNC`.

Prefix: `pipeline.orchestrator`

| Property | Type | Default | Description |
|---|---|---|---|
| `pipeline.orchestrator.mode` | enum | `SYNC` | `SYNC` waits for the result in the request; `QUEUE_ASYNC` stores execution state and continues work in the background. |
| `pipeline.orchestrator.default-tenant` | string | `default` | Fallback tenant when caller omits tenant id. |
| `pipeline.orchestrator.execution-ttl-days` | int | `7` | Execution state retention in days. |
| `pipeline.orchestrator.lease-ms` | long | `30000` | Lease duration for claimed executions. |
| `pipeline.orchestrator.max-retries` | int | `3` | Max execution-level retries before terminal failure. |
| `pipeline.orchestrator.retry-delay` | duration | `PT10S` | Base retry delay. |
| `pipeline.orchestrator.retry-multiplier` | double | `2.0` | Retry backoff multiplier. |
| `pipeline.orchestrator.sweep-interval` | duration | `PT30S` | Interval for due-execution sweep/re-dispatch. |
| `pipeline.orchestrator.sweep-limit` | int | `100` | Max due executions swept per pass. |
| `pipeline.orchestrator.idempotency-policy` | enum | `OPTIONAL_CLIENT_KEY` | `OPTIONAL_CLIENT_KEY`, `CLIENT_KEY_REQUIRED`, `SERVER_KEY_ONLY`. |
| `pipeline.orchestrator.state-provider` | string | `memory` | `ExecutionStateStore` provider selector. |
| `pipeline.orchestrator.dispatcher-provider` | string | `event` | `WorkDispatcher` provider selector. |
| `pipeline.orchestrator.dlq-provider` | string | `log` | Dead-letter publisher provider for terminal execution failures. |
| `pipeline.orchestrator.dlq-url` | string | none | Dead-letter queue URL when `dlq-provider=sqs`. |
| `pipeline.orchestrator.queue-url` | string | none | Queue URL for external dispatcher providers. |
| `pipeline.orchestrator.resume-token-secret` | string | none | HMAC secret used to sign and verify await resume tokens. Required for signed webhook, Kafka, and SQS await dispatch/completion. Use a high-entropy value, preferably at least 32 random bytes encoded as base64 or hex. If this property is missing, signed await dispatch fails with a clear error rather than allowing unsigned resumptions. See [Await Boundaries](/deploy/orchestrator-runtime/await). |
| `pipeline.orchestrator.control-plane.enabled` | boolean | `false` | Enables the internal hosted-control-plane REST surface under `/tpf/control-plane/tenants/{tenantId}` for local/dev coordinator proofs. |
| `pipeline.orchestrator.control-plane.admin-token` | string | none | Bearer token accepted by the hosted-control-plane REST surface. Configure exactly one of `admin-token` or `admin-token-ref` when the surface is enabled. |
| `pipeline.orchestrator.control-plane.admin-token-ref` | string | none | Local secret reference for the hosted-control-plane bearer token. Supports `env:NAME`, `sys:property.name`, and `config:some.config.key`. |
| `pipeline.orchestrator.dynamo.execution-table` | string | `tpf_execution` | DynamoDB table used for execution state rows. |
| `pipeline.orchestrator.dynamo.execution-key-table` | string | `tpf_execution_key` | DynamoDB table used for submit dedupe keys. |
| `pipeline.orchestrator.dynamo.execution-payload-table` | string | `tpf_execution_payload` | DynamoDB table used for immutable manifests and byte chunks when an execution payload is not stored inline. |
| `pipeline.orchestrator.dynamo.await-interaction-table` | string | `tpf_await_interaction` | DynamoDB table used for durable await interaction rows. |
| `pipeline.orchestrator.dynamo.await-interaction-key-table` | string | `tpf_await_interaction_key` | DynamoDB table used for await idempotency and correlation lookup keys. |
| `pipeline.orchestrator.dynamo.region` | string | none | Optional DynamoDB region override. |
| `pipeline.orchestrator.dynamo.endpoint-override` | string | none | Optional DynamoDB endpoint override (local/dev). |
| `pipeline.orchestrator.sqs.region` | string | none | Optional SQS region override. |
| `pipeline.orchestrator.sqs.endpoint-override` | string | none | Optional SQS endpoint override (local/dev). |
| `pipeline.orchestrator.sqs.local-loopback` | boolean | `true` | Also fire in-process work event after SQS enqueue (dev convenience). |
| `pipeline.orchestrator.worker.max-in-flight` | int | `64` | Maximum admitted transition work items per runtime instance. |
| `pipeline.orchestrator.worker.saturated-delay` | duration | `PT1S` | Delay before re-enqueueing work when transition admission is saturated. |
| `pipeline.orchestrator.worker.execution-mode` | enum | `same-thread` | Transition execution mode: `same-thread` or `virtual-thread`. |
| `pipeline.orchestrator.worker.rest.base-url` | string | none | Optional remote REST transition worker base URL. When set, transition work is routed to the REST worker client instead of the local in-process worker. |
| `pipeline.orchestrator.worker.rest.path` | string | `/pipeline/worker/transitions/execute` | REST transition worker execution path. |
| `pipeline.orchestrator.worker.rest.connect-timeout` | duration | `PT2S` | REST transition worker connection timeout. |
| `pipeline.orchestrator.worker.rest.request-timeout` | duration | `PT30S` | REST transition worker request timeout. |
| `pipeline.orchestrator.worker.rest.server-enabled` | boolean | `false` | Enables the local REST transition worker endpoint. Disabled by default because this endpoint is an internal runtime protocol surface. |
| `pipeline.orchestrator.worker.rest.shared-secret` | string | none | HMAC secret used to sign and verify REST transition worker requests. Required when `rest.base-url` or `rest.server-enabled=true` is used. |
| `pipeline.orchestrator.worker.rest.shared-secret-ref` | string | none | Local secret reference for the REST transition worker signing secret. Supports `env:NAME`, `sys:property.name`, and `config:some.config.key`. Configure exactly one of `shared-secret` or `shared-secret-ref`. |
| `pipeline.orchestrator.worker.rest.signature-tolerance` | duration | `PT2M` | Maximum timestamp skew accepted for signed REST transition worker requests. |
| `pipeline.orchestrator.worker.allowed-payload-prefixes` | list | `org.pipelineframework.` | Java package prefixes accepted by the JSON transition payload codec for application payload classes across local, REST, gRPC, and SQS workers. |
| `pipeline.orchestrator.worker.grpc.endpoint` | string | none | Optional remote gRPC transition worker endpoint in `host:port` form. When set, transition work is routed to the gRPC worker client instead of the local in-process worker. |
| `pipeline.orchestrator.worker.grpc.plaintext` | boolean | `false` | Uses plaintext gRPC channels for local/test worker processes. |
| `pipeline.orchestrator.worker.grpc.request-timeout` | duration | `PT30S` | gRPC transition worker request timeout. |
| `pipeline.orchestrator.worker.grpc.server-enabled` | boolean | `false` | Enables the local gRPC transition worker service. Disabled by default because this service is an internal runtime protocol surface. |
| `pipeline.orchestrator.worker.grpc.shared-secret` | string | none | HMAC secret used to sign and verify gRPC transition worker requests. Required when `grpc.endpoint` or `grpc.server-enabled=true` is used. |
| `pipeline.orchestrator.worker.grpc.shared-secret-ref` | string | none | Local secret reference for the gRPC transition worker signing secret. Supports `env:NAME`, `sys:property.name`, and `config:some.config.key`. Configure exactly one of `shared-secret` or `shared-secret-ref`. |
| `pipeline.orchestrator.worker.grpc.signature-tolerance` | duration | `PT2M` | Maximum timestamp skew accepted for signed gRPC transition worker requests. |
| `pipeline.orchestrator.worker.sqs.request-queue-url` | string | none | Optional SQS request queue URL. When set, transition work is routed to the SQS request/reply worker client instead of the local in-process worker. |
| `pipeline.orchestrator.worker.sqs.response-queue-url` | string | none | SQS response queue URL used by the coordinator to receive worker result envelopes. In v1 this must be dedicated per coordinator instance or shard, for example `tpf-worker-responses-shard-a`; shared response queues can deliver responses to the wrong process because v1 has no demultiplexing. |
| `pipeline.orchestrator.worker.sqs.server-enabled` | boolean | `false` | Enables the local SQS transition worker poller. Disabled by default because this poller consumes an internal runtime protocol queue. |
| `pipeline.orchestrator.worker.sqs.request-timeout` | duration | `PT30S` | Timeout while waiting for the matching SQS worker response. |
| `pipeline.orchestrator.worker.sqs.poll-start-delay` | duration | `PT0S` | Optional delay before the SQS transition worker poller starts consuming requests. |
| `pipeline.orchestrator.worker.sqs.visibility-timeout` | duration | `PT30S` | Visibility timeout for claimed SQS transition worker request/response messages. |
| `pipeline.orchestrator.worker.sqs.shared-secret` | string | none | HMAC secret used to sign and verify SQS transition worker request and response messages. Required when `sqs.request-queue-url` or `sqs.server-enabled=true` is used. |
| `pipeline.orchestrator.worker.sqs.shared-secret-ref` | string | none | Local secret reference for the SQS transition worker signing secret. Supports `env:NAME`, `sys:property.name`, and `config:some.config.key`. Configure exactly one of `shared-secret` or `shared-secret-ref`. |
| `pipeline.orchestrator.worker.sqs.signature-tolerance` | duration | `PT2M` | Maximum timestamp skew accepted for signed SQS transition worker messages. |
| `pipeline.orchestrator.strict-startup` | boolean | `true` | Fail startup if queue mode prerequisites are invalid. |

Background execution notes:

1. `QUEUE_ASYNC` rejects async streaming output for this milestone.
2. Keep `strict-startup=true` in production, so invalid provider configuration fails fast.
3. In queue mode, strict startup also requires `pipeline.orchestrator.idempotency-policy` to be explicitly set to a non-default value.
4. In-memory providers are for local/dev only; use providers backed by external storage/queues for crash recovery.
5. For dead-letter handling that survives restarts, set both `pipeline.orchestrator.dlq-provider=sqs` and `pipeline.orchestrator.dlq-url`.
6. For webhook, Kafka, or SQS await steps using signed resume tokens, configure a stable `pipeline.orchestrator.resume-token-secret`; rotating it invalidates outstanding resume tokens.
7. Remote transition worker selection is inferred from configured targets: REST uses `pipeline.orchestrator.worker.rest.base-url`, gRPC uses `pipeline.orchestrator.worker.grpc.endpoint`, SQS uses `pipeline.orchestrator.worker.sqs.request-queue-url`, and no remote target uses the local in-process worker.
8. Configure at most one remote worker target. Multiple remote targets fail startup as ambiguous; there is no `worker.provider` selector.
9. `pipeline.platform` remains orthogonal and does not select worker invocation.
10. REST, gRPC, and SQS transition workers require their matching signing secret on both coordinator and worker processes. Configure exactly one of `shared-secret` or `shared-secret-ref` for each enabled protocol.
11. SQS transition worker `response-queue-url` must be dedicated per coordinator shard/process in v1. Shared response queues can cause dropped or misrouted responses and extra latency because v1 has no response demultiplexing.
12. The local control-plane REST surface is internal groundwork. It requires an explicit tenant path and bearer token, but it is not a managed service API.
13. When `pipeline.orchestrator.dynamo.await-interaction-table` is used, provision ALL-projected GSIs named `await-interaction-by-unit`, `await-interaction-pending-by-tenant`, `await-interaction-pending-by-assignee`, `await-interaction-pending-by-group`, `await-interaction-pending-by-step`, and `await-interaction-pending-by-deadline`. These indexes back await unit lookup, pending interaction listing, and timeout selection without full-table scans.

Example crash-recovery provider configuration:

```properties
pipeline.orchestrator.mode=QUEUE_ASYNC
pipeline.orchestrator.state-provider=dynamo
pipeline.orchestrator.dispatcher-provider=sqs
pipeline.orchestrator.dlq-provider=sqs
pipeline.orchestrator.queue-url=https://sqs.eu-west-1.amazonaws.com/123456789012/tpf-work
pipeline.orchestrator.dlq-url=https://sqs.eu-west-1.amazonaws.com/123456789012/tpf-dlq
pipeline.orchestrator.idempotency-policy=CLIENT_KEY_REQUIRED
```

### Await Transports

Await transport selection is authored per `kind: await` step in pipeline YAML. These runtime properties enable the concrete adapter plumbing for broker-backed await providers.

#### Kafka Await

Prefix: `tpf.await.kafka`

| Property | Type | Default | Description |
|---|---|---|---|
| `tpf.await.kafka.reactive-messaging.enabled` | boolean | `false` | Enables the Quarkus Reactive Messaging Kafka await publisher and completion consumer. |

Kafka await also requires SmallRye channel configuration for the framework-owned `tpf-await-kafka-requests` outgoing channel and `tpf-await-kafka-responses` incoming channel. See [Await Boundaries](/deploy/orchestrator-runtime/await).

#### SQS Await

Prefix: `tpf.await.sqs`

| Property | Type | Default | Description |
|---|---|---|---|
| `tpf.await.sqs.poller.enabled` | boolean | `false` | Enables the coordinator-side SQS await completion poller. |
| `tpf.await.sqs.response-queue-url` | string | none | SQS response queue URL consumed by the completion poller. Required when the poller is enabled. |
| `tpf.await.sqs.poll-start-delay` | duration | `PT0S` | Optional delay before the completion poller starts. |
| `tpf.await.sqs.visibility-timeout` | duration | `PT30S` | Visibility timeout for claimed completion messages. |
| `tpf.await.sqs.completion-timeout` | duration | `PT5M` | Timeout while admitting one completion through the await coordinator. |
| `tpf.await.sqs.wait-time-seconds` | int | `1` | Long-poll wait time for SQS receive calls. Values are clamped to the SQS range `1..20`. |
| `tpf.await.sqs.max-messages` | int | `1` | Maximum messages received per poll. Values are clamped to the SQS range `1..10`. |

SQS await dispatch uses the `request.queueUrl` and `response.queueUrl` authored on the await step. The completion poller uses the runtime `response-queue-url` so the hosting coordinator can decide which queue it consumes. Region and endpoint override reuse `pipeline.orchestrator.sqs.region` and `pipeline.orchestrator.sqs.endpoint-override`.

### Item Reject Sink

Prefix: `pipeline.item-reject`

| Property | Type | Default | Description |
|---|---|---|---|
| `pipeline.item-reject.provider` | string | `log` | Step-level reject sink provider selector (`log`, `memory`, `sqs`). |
| `pipeline.item-reject.strict-startup` | boolean | `true` | Fail startup when selected sink provider configuration is invalid. |
| `pipeline.item-reject.include-payload` | boolean | `false` | Include rejected payload in sink envelope. |
| `pipeline.item-reject.memory-capacity` | int | `512` | Bounded ring size for the `memory` provider. |
| `pipeline.item-reject.publish-failure-policy` | enum | `CONTINUE` | Sink publish failure behaviour (`CONTINUE`, `FAIL_PIPELINE`). |
| `pipeline.item-reject.sqs.queue-url` | string | none | SQS queue URL when `provider=sqs`. |
| `pipeline.item-reject.sqs.region` | string | none | Optional SQS region override. |
| `pipeline.item-reject.sqs.endpoint-override` | string | none | Optional SQS endpoint override (local/dev). |

Operational notes:

1. Item reject sink is step-level recover-and-continue behaviour (`recoverOnFailure=true`).
2. In production launch mode, startup fails when recovery is enabled and a non-durable sink (`log`/`memory`) is selected.
3. In non-production modes, non-durable sinks are allowed with warning logs.
4. Keep `include-payload=false` unless payload capture is explicitly required for triage.

Example durable step-level reject sink:

```properties
pipeline.item-reject.provider=sqs
pipeline.item-reject.sqs.queue-url=https://sqs.eu-west-1.amazonaws.com/123456789012/tpf-item-reject
pipeline.item-reject.include-payload=false
pipeline.item-reject.publish-failure-policy=CONTINUE
```

### Telemetry

Prefix: `pipeline.telemetry`

| Property                      | Type   | Default | Description                                                        |
|-------------------------------|--------|---------|--------------------------------------------------------------------|
| `pipeline.telemetry.item-input-type` | string | none    | Fully-qualified input type used to define the item boundary (build-time only — requires rebuild to take effect). |
| `pipeline.telemetry.item-output-type` | string | none    | Fully-qualified output type used to define the item boundary (build-time only — requires rebuild to take effect). |
| `pipeline.telemetry.slo.rpc-latency-ms` | number | `1000` | RPC latency threshold (ms) used to emit SLO counters. |
| `pipeline.telemetry.slo.item-throughput-per-min` | number | `1000` | Item throughput threshold (items/min) used to emit SLO counters. |

Item boundary types are compiled into telemetry metadata; runtime changes do not apply unless you rebuild the project.

### In-flight Probe (Kill Switch)

Prefix: `pipeline.kill-switch`

| Property                                                              | Type     | Default     | Description                                   |
|-----------------------------------------------------------------------|----------|-------------|-----------------------------------------------|
| `pipeline.kill-switch.retry-amplification.enabled`                    | boolean  | `false`     | Enable retry amplification guard.             |
| `pipeline.kill-switch.retry-amplification.window`                     | duration | `PT30S`     | Evaluation window for sustained inflight growth. |
| `pipeline.kill-switch.retry-amplification.inflight-slope-threshold`   | double   | `10`        | Inflight slope threshold (items/sec).         |
| `pipeline.kill-switch.retry-amplification.mode`                       | string   | `fail-fast` | Guard behaviour (`fail-fast` or `log-only`).   |
| `pipeline.kill-switch.retry-amplification.sustain-samples`            | integer  | `3`         | Consecutive samples above the threshold required to trigger. |

### Global Defaults

Prefix: `pipeline.defaults`

| Property                                         | Type    | Default  | Description                                 |
|--------------------------------------------------|---------|----------|---------------------------------------------|
| `pipeline.defaults.retry-limit`                  | integer | `3`      | Max retry attempts for steps.               |
| `pipeline.defaults.retry-wait-ms`                | long    | `2000`   | Base delay between retries (ms).            |
| `pipeline.defaults.recover-on-failure`           | boolean | `false`  | Enables recovery behaviour on failure.       |
| `pipeline.defaults.max-backoff`                  | long    | `30000`  | Maximum backoff delay (ms).                 |
| `pipeline.defaults.jitter`                       | boolean | `false`  | Adds jitter to retry delays.                |
| `pipeline.defaults.backpressure-buffer-capacity` | integer | `128`   | Per-step backpressure buffer capacity (in items). |
| `pipeline.defaults.backpressure-strategy`        | string  | `BUFFER` | Backpressure strategy (`BUFFER` or `DROP`). |

These defaults apply to every step unless a step-level override is present.

### Function Transport Context Attributes (Function Handlers/Adapters)

These values are carried in `FunctionTransportContext` attributes by function handlers/adapters in Lambda deployments.
They are not `application.properties` keys.

| Attribute Key              | Type   | Default  | Description                                                                 |
|---------------------------|--------|----------|-----------------------------------------------------------------------------|
| `tpf.idempotency.policy`  | enum   | `CONTEXT_STABLE` | Function transport idempotency policy (`CONTEXT_STABLE` or `EXPLICIT`; legacy `RANDOM` alias accepted).            |
| `tpf.idempotency.key`     | string | none     | Explicit caller-provided idempotency key used when policy is `EXPLICIT`.   |

Important:
- The Pipeline Framework (TPF) transport idempotency is a boundary-level aid.
- Authoritative duplicate prevention must still be enforced in business/data layers (domain constraints, DB primary/unique keys, warehouse controls).

### Build-Time Validation (Annotation Processor)

These are build-time options passed to the annotation processor (not runtime config).

| Option                           | Type   | Default | Description                                                                                              |
|----------------------------------|--------|---------|----------------------------------------------------------------------------------------------------------|
| `pipeline.provider.class.<name>` | string | none    | Provider class name to validate ordering/thread-safety hints (e.g. `pipeline.provider.class.cache=...`). |

### Per-Step Overrides

Prefix: `pipeline.step."fully.qualified.StepClass"`

All properties listed under `pipeline.defaults.*` can be overridden per step:

```properties
pipeline.step."com.example.MyStep".retry-limit=7
pipeline.step."com.example.MyStep".recover-on-failure=true
pipeline.step."com.example.MyStep".backpressure-buffer-capacity=4096
pipeline.step."com.example.MyStep".backpressure-strategy=BUFFER
```

### Startup Health Checks

| Property                          | Type     | Default | Description                                            |
|-----------------------------------|----------|---------|--------------------------------------------------------|
| `pipeline.health.startup-timeout` | duration | `PT5M`  | Max time to wait for startup dependency health checks. |
