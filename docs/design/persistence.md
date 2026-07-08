# Persistence Plugin

Persistence stores pipeline data without changing the flow itself. In practical terms, this gives developers saved application data they can query later from downstream APIs, reports, or user interfaces.

This is one of the most important state capabilities in The Pipeline Framework (TPF). You keep the business function focused on processing, while the persistence plugin stores the resulting records in a way that fits reactive execution cleanly.

::: tip Runtime Scope
The current persistence implementation is Quarkus/Mutiny-oriented. Spring persistence providers are not production-supported yet; see [Spring Support Status](/develop/spring-support). For the broader state map, see [State Model](/design/state-model).
:::

## What it does

- Observes stream elements and persists them
- Returns the original element unchanged
- Selects the appropriate persistence provider at runtime

If you are looking for the broader value story behind persistence plus caching, start with [State, Replay, and Queryable Data](/value/state-replay-and-queryable-data) and [Cache vs Persistence](/design/caching/cache-vs-persistence).

Persistence pairs well with the [JPA Query Connector](/design/jpa-query-connector/). The query connector captures read-side facts before a decision; the persistence plugin stores the resulting business outputs as durable application records. Together they keep both sides of state visible without turning business steps into session or repository plumbing.

## Module layout

The plugin is split into two parts:

1. **Plugin library**: `plugins/foundational/persistence`
2. **Service host module**: e.g. `examples/.../persistence-svc`

The host module provides a concrete module that knows your domain types and enables runtime discovery.

In the current Quarkus runtime, `PersistenceManager` and `PersistenceService` are CDI beans that discover providers via `Instance<PersistenceProvider<?>>`. The `PersistencePluginHost` is a marker class annotated with `@PipelinePlugin` to make the module discoverable by the framework. This is a runtime-discovery model, not a build-time code-generation hook.

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
`persistence.provider.class` to the provider's fully qualified class name. The persistence
manager will fail fast if the configured provider cannot be found or does not support the current
execution context. For build-time validation, pass `-Apersistence.provider.class=<fqcn>`
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

AFTER_STEP observes the output of a step (the next step's BEFORE_STEP). This means a single
position captures every boundary except one cap. AFTER_STEP misses the first input boundary;
BEFORE_STEP misses the final output boundary. Use two aspects if you need both caps.
