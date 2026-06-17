---
search: false
---

# Persistence Portability

Panache is isolated to the persistence plugin surface and does not leak broadly into `framework/runtime/src/main`.

Current SPI state:

- `PersistenceProvider<T>` returns `Uni<T>`.
- `ExecutionStateStore` and `Await*Store` contracts are Mutiny-oriented and live beside existing provider implementations.
- Store providers are present in plugin modules, not only runtime core.

Proposed migration:

- Move core interfaces to neutral async types (`CompletionStage` / `Publisher` boundary as needed).
- Keep provider implementations in framework-specific modules.

Proposed modules:

- `tpf-store-core`
- `tpf-store-inmemory`
- `tpf-store-dynamo`
- `tpf-store-quarkus-hibernate-reactive`
- `tpf-store-spring-r2dbc`
