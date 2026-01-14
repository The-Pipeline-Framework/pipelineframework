---
title: Configuration Reference
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

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `-Apipeline.generatedSourcesDir` | path | none | Base directory for role-specific generated sources. |
| `-Apipeline.generatedSourcesRoot` | path | none | Legacy alias of `pipeline.generatedSourcesDir`. |
| `-Apipeline.orchestrator.generate` | boolean | `false` | Generate orchestrator endpoint even without `@PipelineOrchestrator`. CLI generation still requires the annotation. |

### REST Path Overrides (Build-Time)

The annotation processor reads `src/main/resources/application.properties` during compilation to override REST paths:

| Property                                            | Type   | Default | Description                                |
|-----------------------------------------------------|--------|---------|--------------------------------------------|
| `pipeline.rest.path.<ServiceName>`                  | string | none    | Overrides REST path by service name.       |
| `pipeline.rest.path.<fully.qualified.ServiceClass>` | string | none    | Overrides REST path by service class name. |

## Runtime Configuration

Prefix: `pipeline`

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
| `pipeline.cache.policy`                       | string   | `cache-only`      | Default cache policy (`prefer-cache`/`return-cached`, `cache-only`, `skip-if-present`, `require-cache`, `bypass-cache`). |
| `pipeline.cache.ttl`                          | duration | none              | Default cache TTL.                                                                                                       |
| `pipeline.cache.caffeine.name`                | string   | `pipeline-cache`  | Cache name for the Caffeine provider.                                                                                    |
| `pipeline.cache.caffeine.maximum-size`        | long     | `10000`           | Maximum cache size for the Caffeine provider.                                                                            |
| `pipeline.cache.caffeine.expire-after-write`  | duration | none              | Expire entries after write for the Caffeine provider.                                                                    |
| `pipeline.cache.caffeine.expire-after-access` | duration | none              | Expire entries after access for the Caffeine provider.                                                                   |
| `pipeline.cache.redis.prefix`                 | string   | `pipeline-cache:` | Key prefix for Redis cache entries.                                                                                      |

### Persistence Configuration

Prefix: `pipeline.persistence`

| Property                             | Type   | Default | Description                                                        |
|--------------------------------------|--------|---------|--------------------------------------------------------------------|
| `pipeline.persistence.duplicate-key` | string | `fail`  | Duplicate key policy for persistence (`fail`, `ignore`, `upsert`). |

### Global Defaults

Prefix: `pipeline.defaults`

| Property                                         | Type    | Default  | Description                                 |
|--------------------------------------------------|---------|----------|---------------------------------------------|
| `pipeline.defaults.retry-limit`                  | integer | `3`      | Max retry attempts for steps.               |
| `pipeline.defaults.retry-wait-ms`                | long    | `2000`   | Base delay between retries (ms).            |
| `pipeline.defaults.parallel`                     | boolean | `false`  | Enables parallel processing for steps.      |
| `pipeline.defaults.recover-on-failure`           | boolean | `false`  | Enables recovery behavior on failure.       |
| `pipeline.defaults.max-backoff`                  | long    | `30000`  | Maximum backoff delay (ms).                 |
| `pipeline.defaults.jitter`                       | boolean | `false`  | Adds jitter to retry delays.                |
| `pipeline.defaults.backpressure-buffer-capacity` | integer | `1024`   | Backpressure buffer capacity.               |
| `pipeline.defaults.backpressure-strategy`        | string  | `BUFFER` | Backpressure strategy (`BUFFER` or `DROP`). |

### Per-Step Overrides

Prefix: `pipeline.step."fully.qualified.StepClass"`

All properties listed under `pipeline.defaults.*` can be overridden per step:

```properties
pipeline.step."com.example.MyStep".retry-limit=7
pipeline.step."com.example.MyStep".parallel=true
```

### Startup Health Checks

| Property                          | Type     | Default | Description                                            |
|-----------------------------------|----------|---------|--------------------------------------------------------|
| `pipeline.health.startup-timeout` | duration | `PT5M`  | Max time to wait for startup dependency health checks. |
