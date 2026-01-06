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
