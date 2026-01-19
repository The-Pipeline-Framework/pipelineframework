# Persistence Plugin

The persistence plugin is a foundational side-effect plugin that stores pipeline data without changing the stream. It is designed to be minimal and composable: you bring the provider dependency you want, and the plugin handles the rest.

## What it does

- Observes stream elements and persists them
- Returns the original element unchanged
- Selects the appropriate persistence provider at runtime

## Module layout

The plugin is split into two parts:

1. **Plugin library**: `plugins/foundational/persistence`
2. **Service host module**: e.g. `examples/.../persistence-svc`

The host module exists so the annotation processor can generate typed transport adapters and client steps in a concrete module that knows your domain types.

## Required dependencies

The service host module should depend on:

- `common` (domain types and mappers)
- `plugins/foundational/persistence`
- One or more persistence providers (reactive or blocking)

## Provider selection

Providers implement `PersistenceProvider<T>` and declare whether they support the current execution context. The persistence plugin will:

1. Find a provider that supports the item type
2. Ensure it matches the current runtime context (reactive vs blocking)
3. Persist the entity and return the original item

This lets you plug in multiple backends without changing the plugin code.

To lock a specific provider (recommended for production), set
`pipeline.persistence.provider.class` to the provider's fully-qualified class name. The persistence
manager will fail fast if the configured provider cannot be found or does not support the current
execution context. For build-time validation, pass `-Apipeline.provider.class.persistence=<fqcn>`
to the annotation processor.

## Parallelism guidance

Persistence providers can declare ordering hints. When a provider does not declare hints, the framework
assumes `RELAXED` ordering and `SAFE` thread safety and emits warnings. With `pipeline.parallelism=AUTO`,
the framework will run providers that advise strict ordering sequentially (with a warning); with
`pipeline.parallelism=PARALLEL` the framework will allow parallel execution and warn that ordering
advice is overridden.

If your workload depends on ordering (for example, sequence numbers or cross-step dependencies), keep
the pipeline in `SEQUENTIAL` or `AUTO`.

## Runtime note

Reactive persistence requires a Mutiny session or transaction. The plugin ensures this by running persistence calls inside a transaction boundary (via the persistence manager). If you add custom persistence providers, keep the reactive session/transaction requirement in mind.

## Configuring the aspect

Enable the persistence aspect in your pipeline config and point it at the plugin implementation class:

```yaml
aspects:
  persistence:
    enabled: true
    scope: "GLOBAL"
    position: "AFTER_STEP"
    config:
      pluginImplementationClass: "org.pipelineframework.plugin.persistence.PersistenceService"
```

The framework expands this into side-effect steps that observe the stream after each step.
