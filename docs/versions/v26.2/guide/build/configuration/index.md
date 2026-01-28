---
title: Configuration Reference
search: false
---

# Configuration Reference

This page lists every supported configuration option, grouped by build-time and runtime usage.

## Build-Time Configuration

These settings are read during build/compile and affect generated code or CLI wiring.

### Pipeline YAML

The pipeline YAML controls global settings used by the annotation processor.

| Property    | Type | Default | Description                                                 |
|-------------|------|---------|-------------------------------------------------------------|
| `transport` | enum | `GRPC`  | Global transport for generated adapters (`GRPC` or `REST`). |

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

| Option                             | Type    | Default | Description                                                                                                        |
|------------------------------------|---------|---------|--------------------------------------------------------------------------------------------------------------------|
| `-Apipeline.generatedSourcesDir`   | path    | none    | Base directory for role-specific generated sources.                                                                |
| `-Apipeline.generatedSourcesRoot`  | path    | none    | Legacy alias of `pipeline.generatedSourcesDir`.                                                                    |
| `-Apipeline.orchestrator.generate` | boolean | `false` | Generate orchestrator endpoint even without `@PipelineOrchestrator`. CLI generation still requires the annotation. |

### REST Path Overrides (Build-Time)

The annotation processor reads `src/main/resources/application.properties` during compilation to override REST paths:

| Property                                            | Type   | Default | Description                                |
|-----------------------------------------------------|--------|---------|--------------------------------------------|
| `pipeline.rest.path.<ServiceName>`                  | string | none    | Overrides REST path by service name.       |
| `pipeline.rest.path.<fully.qualified.ServiceClass>` | string | none    | Overrides REST path by service class name. |

## Runtime Configuration

Prefix: `pipeline`

### Orchestrator Client Wiring (Generated)

The annotation processor generates default client wiring for orchestrators at build time.
Generated values have lower priority than explicit `application.properties` settings or environment variables.

To override routing or ports, set the following in `application.properties`:

| Property                                 | Type   | Default | Description                                       |
|------------------------------------------|--------|---------|---------------------------------------------------|
| `pipeline.module.<module>.host`          | string | none    | Host for a module (applies to all its services).  |
| `pipeline.module.<module>.port`          | int    | none    | Port for a module (applies to all its services).  |
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

### Persistence Configuration

Prefix: `pipeline.persistence`

| Property                              | Type   | Default | Description                                                                   |
|---------------------------------------|--------|---------|-------------------------------------------------------------------------------|
| `pipeline.persistence.duplicate-key`  | string | `fail`  | Duplicate key policy for persistence (`fail`, `ignore`, `upsert`).            |
| `pipeline.persistence.provider.class` | string | none    | Fully-qualified persistence provider class name to lock selection at runtime. |

### Pipeline Execution

Prefix: `pipeline`

| Property                   | Type    | Default | Description                                                 |
|----------------------------|---------|---------|-------------------------------------------------------------|
| `pipeline.parallelism`     | string  | `AUTO`  | Parallelism policy: `SEQUENTIAL`, `AUTO`, or `PARALLEL`.    |
| `pipeline.max-concurrency` | integer | `128`   | Per-step maximum in-flight items when parallel execution is enabled. |

### Telemetry

Prefix: `pipeline.telemetry`

| Property                      | Type   | Default | Description                                                        |
|-------------------------------|--------|---------|--------------------------------------------------------------------|
| `pipeline.telemetry.item-input-type` | string | none    | Fully-qualified input type used to define the item boundary (build-time only — requires rebuild to take effect). |
| `pipeline.telemetry.item-output-type` | string | none    | Fully-qualified output type used to define the item boundary (build-time only — requires rebuild to take effect). |
| `pipeline.telemetry.slo.rpc-latency-ms` | number | `1000` | RPC latency threshold (ms) used to emit SLO counters. |
| `pipeline.telemetry.slo.item-throughput-per-min` | number | `1000` | Item throughput threshold (items/min) used to emit SLO counters. |

Item boundary types are compiled into telemetry metadata; runtime changes do not apply unless you rebuild the project.

### Global Defaults

Prefix: `pipeline.defaults`

| Property                                         | Type    | Default  | Description                                 |
|--------------------------------------------------|---------|----------|---------------------------------------------|
| `pipeline.defaults.retry-limit`                  | integer | `3`      | Max retry attempts for steps.               |
| `pipeline.defaults.retry-wait-ms`                | long    | `2000`   | Base delay between retries (ms).            |
| `pipeline.defaults.recover-on-failure`           | boolean | `false`  | Enables recovery behavior on failure.       |
| `pipeline.defaults.max-backoff`                  | long    | `30000`  | Maximum backoff delay (ms).                 |
| `pipeline.defaults.jitter`                       | boolean | `false`  | Adds jitter to retry delays.                |
| `pipeline.defaults.backpressure-buffer-capacity` | integer | `128`   | Per-step backpressure buffer capacity (in items). |
| `pipeline.defaults.backpressure-strategy`        | string  | `BUFFER` | Backpressure strategy (`BUFFER` or `DROP`). |

These defaults apply to every step unless a step-level override is present.

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
