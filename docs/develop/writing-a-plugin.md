# Writing a Plugin

Plugins extend pipeline behavior without changing step inputs or outputs. They are the primary mechanism for cross-cutting concerns such as persistence, auditing, and metrics.

## Plugin Landscape

The framework distinguishes between:

1. **Foundational plugins**: Built-in plugins maintained in the core repository
2. **Community plugins**: External plugins authored and versioned independently

Foundational plugins are stable and opinionated. Community plugins are encouraged for organization-specific needs.

Plugins are reusable, external components that perform side effects in your pipeline. They might persist data, log events, send notifications, or collect metrics. As a plugin author, you focus on implementing your specific business logic without worrying about how it integrates with pipelines.

## What is a plugin?

A plugin is a component that:
- Performs side effects (like persistence or logging) without changing the data flowing through the pipeline
- Is transport-agnostic and doesn't know about gRPC, DTOs, or pipeline internals
- Uses simple interfaces that work with CDI and Mutiny
- Can be applied to many different pipelines

## What a plugin is NOT

A plugin is not:
- A pipeline step that transforms data
- Concerned with transport protocols like gRPC
- Responsible for pipeline orchestration
- Required to know about DTOs, mappers, or protos

## Plugin interfaces

The framework provides interfaces for different patterns:

- `ReactiveSideEffectService<T>`: Observe a single item and return it unchanged

The `T` type represents your domain type - the actual business object your plugin will work with. The framework handles converting between your domain types and transport types.

## Example: Persistence plugin

Here's a simple persistence plugin that stores domain objects:

```java
@ApplicationScoped
public class PersistenceService<T> implements ReactiveSideEffectService<T> {
    private final PersistenceManager persistenceManager;

    public PersistenceService(PersistenceManager persistenceManager) {
        this.persistenceManager = persistenceManager;
    }

    @Override
    public Uni<T> process(T item) {
        return persistenceManager.persist(item)
            .replaceWith(item);
    }
}
```

## Persistence plugin requirements

The foundational persistence plugin is intentionally small and expects the host application to provide the rest:

- **Depends on `common`**: the host service module must depend on the `common` module so the generated transport adapters can reference your domain types.
- **Database providers are pluggable**: add the provider dependency you want (reactive or blocking). The plugin auto-selects a provider that supports the current execution context.
- **Service host module required**: the typed transport services and client steps are generated in a dedicated service module, not inside the plugin library itself.

In practice, you wire this as:

1. `plugins/foundational/persistence` (library, no generated transport code)
2. `examples/.../persistence-svc` (service host module with the marker annotation)

For a complete walkthrough, see the dedicated persistence plugin page.

## Plugin host modules

To generate plugin-server artifacts in a dedicated module, add a marker class annotated with `@PipelinePlugin("name")`
inside that module. This tells the annotation processor to emit the transport adapters and CDI producers there.

The host module should depend on:
- Your plugin library (e.g., `plugins/foundational/persistence`)
- The `common` module that owns your domain types
- Any persistence provider dependencies (reactive or blocking)

## Plugin lifecycle and constraints

Your plugin implementation follows standard CDI lifecycle management. The framework provides these guarantees:
- Your plugin receives domain objects directly (no DTOs or gRPC messages)
- The framework handles all transport concerns
- Type safety is preserved end-to-end

Plugins must not:
- Block threads
- Change the types of data passing through
- Alter the functional behavior of the pipeline

## Parallelism hints

Plugins can declare ordering and thread-safety requirements so the framework can validate
parallel execution decisions:

- `@ParallelismHint(ordering=STRICT_REQUIRED)` forces sequential execution
- `@ParallelismHint(ordering=STRICT_ADVISED)` warns under `AUTO` and is overridden by `PARALLEL`
- `@ParallelismHint(ordering=RELAXED)` allows parallel execution

If your plugin delegates to a provider (for example cache or persistence backends), the provider
may declare `@ParallelismHint`, and the plugin can delegate hints at runtime based on the selected
provider. When no provider hints are declared, the framework assumes `RELAXED` ordering and `SAFE`
thread safety and emits warnings to make the assumption explicit. For build-time validation, pass
`-Apipeline.provider.class.<name>=<fqcn>` to the annotation processor.

## What plugin authors never need to care about

As a plugin author, you don't need to know about:
- gRPC protocols or message formats
- Code generation processes
- Adapter implementations
- Pipeline configuration details
- DTO mapping logic

Your focus is entirely on implementing your specific business logic in a reactive way.
