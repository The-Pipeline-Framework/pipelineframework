---
title: Configuration Reference
---

# Configuration Reference

This page lists every supported configuration option, grouped by build-time and runtime usage.

## Build-Time Configuration

These settings are read during build/compile and affect generated code or CLI wiring.

### Pipeline YAML

The pipeline YAML controls global settings used by the annotation processor.

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `transport` | enum | `GRPC` | Global transport for generated adapters (`GRPC` or `REST`). |

### CLI Build Options (Quarkus Build-Time)

Prefix: `pipeline-cli`

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `pipeline-cli.generate-cli` | boolean | `false` | Enables generation of the CLI entrypoint. |
| `pipeline-cli.version` | string | `0.9.2` | Framework version embedded in the CLI metadata. |
| `pipeline-cli.cli-name` | string | none | CLI command name. |
| `pipeline-cli.cli-description` | string | none | CLI description. |
| `pipeline-cli.cli-version` | string | none | CLI version override; when unset, use `pipeline-cli.version`. |

### Annotation Processor Options

Pass via `maven-compiler-plugin` with `-A` arguments.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `-Apipeline.generatedSourcesDir` | path | none | Base directory for role-specific generated sources. |
| `-Apipeline.generatedSourcesRoot` | path | none | Legacy alias of `pipeline.generatedSourcesDir`. |
| `-Apipeline.cache.keyGenerator` | class name | none | Fully-qualified `CacheKeyGenerator` used for `@CacheResult`. |

### REST Path Overrides (Build-Time)

The annotation processor reads `src/main/resources/application.properties` during compilation to override REST paths:

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `pipeline.rest.path.<ServiceName>` | string | none | Overrides REST path by service name. |
| `pipeline.rest.path.<fully.qualified.ServiceClass>` | string | none | Overrides REST path by service class name. |

## Runtime Configuration

Prefix: `pipeline`

### REST Client Endpoints

REST client steps use Quarkus REST client configuration:

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `quarkus.rest-client.<client-name>.url` | string | none | Base URL for a REST client step. |

`client-name` is derived from the service class name in kebab-case with a trailing `Service` removed (for example `ProcessPaymentService` → `process-payment`).

### Pipeline Order

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `pipeline.order` | list | empty | Comma-separated list of fully-qualified step class names. |

### Global Defaults

Prefix: `pipeline.defaults`

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `pipeline.defaults.retry-limit` | integer | `3` | Max retry attempts for steps. |
| `pipeline.defaults.retry-wait-ms` | long | `2000` | Base delay between retries (ms). |
| `pipeline.defaults.parallel` | boolean | `false` | Enables parallel processing for steps. |
| `pipeline.defaults.recover-on-failure` | boolean | `false` | Enables recovery behavior on failure. |
| `pipeline.defaults.max-backoff` | long | `30000` | Maximum backoff delay (ms). |
| `pipeline.defaults.jitter` | boolean | `false` | Adds jitter to retry delays. |
| `pipeline.defaults.backpressure-buffer-capacity` | integer | `1024` | Backpressure buffer capacity. |
| `pipeline.defaults.backpressure-strategy` | string | `BUFFER` | Backpressure strategy (`BUFFER` or `DROP`). |

### Per-Step Overrides

Prefix: `pipeline.step."fully.qualified.StepClass"`

All properties listed under `pipeline.defaults.*` can be overridden per step:

```properties
pipeline.step."com.example.MyStep".retry-limit=7
pipeline.step."com.example.MyStep".parallel=true
```

### Startup Health Checks

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `pipeline.health.startup-timeout` | duration | `PT5M` | Max time to wait for startup dependency health checks. |
