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

The framework provides several interfaces for different patterns:

- `PluginReactiveUnary<T>`: Process a single item and perform a side effect
- `PluginReactiveUnaryReply<T, R>`: Process input and return a result
- `PluginReactiveStreamIn<T>`: Process a stream of inputs

The `T` type represents your domain type - the actual business object your plugin will work with. The framework handles converting between your domain types and transport types.

## Example: Persistence plugin

Here's a simple persistence plugin that stores domain objects:

```java
@ApplicationScoped
public class PersistencePlugin<T> implements PluginReactiveUnary<T> {
    private final PersistenceManager persistenceManager;

    public PersistencePlugin(PersistenceManager persistenceManager) {
        this.persistenceManager = persistenceManager;
    }

    @Override
    public Uni<Void> process(T item) {
        return persistenceManager.persist(item)
            .replaceWithVoid();
    }
}
```

## Plugin lifecycle and constraints

Your plugin implementation follows standard CDI lifecycle management. The framework provides these guarantees:
- Your plugin receives domain objects directly (no DTOs or gRPC messages)
- The framework handles all transport concerns
- Type safety is preserved end-to-end

Plugins must not:
- Block threads
- Change the types of data passing through
- Alter the functional behavior of the pipeline

## What plugin authors never need to care about

As a plugin author, you don't need to know about:
- gRPC protocols or message formats
- Code generation processes
- Adapter implementations
- Pipeline configuration details
- DTO mapping logic

Your focus is entirely on implementing your specific business logic in a reactive way.
