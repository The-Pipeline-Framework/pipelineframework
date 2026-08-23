# Composition and placement

Read this reference only for types/routing, nested pipelines, retry/redrive entrypoints, runtime mapping, transport/platform, or deployment work. Verify exact syntax and support in current docs/source/tests.

## Typed routing and composition

Use canonical product types for immutable values and discriminated unions for closed outcomes. Use `accepts` to narrow a branch. Let the compiler route known types; avoid `instanceof`, class-name switches, predicate strings, and application dispatch registries.

Repeated fields are ordered finite collections inside one value. They do not declare reactive cardinality. Choose step cardinality separately.

Use a pipeline as a typed step for a meaningful subflow. Let a nested/local pipeline own its declared input, output, ordered steps, routing, and currently supported recursion. Prefer separate typed retry/redrive entrypoints where supported instead of contaminating every happy-path type.

## Placement

Keep portable pipeline semantics in canonical types and `pipeline.yaml`. Put adapter/runtime details in connector bindings, runtime configuration, runtime mapping, and deployment/build topology.

Do not conflate transport, platform, runtime layout, build topology, or wire/worker protocol. Placement changes must preserve typed meaning, effect identity, correlation, retry classification, and deadlines. Runtime mapping does not automatically reshape Maven topology.

Search current template DSL and composition docs plus loader/routing tests for type work. For placement, search runtime-layout/mapping docs, generated deployment phases, topology examples/tests, and the current transport/platform configuration.
