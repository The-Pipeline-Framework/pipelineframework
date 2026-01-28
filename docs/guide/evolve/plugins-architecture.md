# Architectural overview

The plugin and aspect system implements an AOP-like model where cross-cutting concerns are applied declaratively to pipeline steps. This preserves the simplicity of step-focused pipeline definitions while enabling sophisticated infrastructure capabilities.

## Compile-time weaving

Aspects are applied at compile-time during the annotation processing phase. This differs from runtime AOP frameworks and provides:
- Deterministic behavior
- No runtime performance overhead for aspect application
- Clear visibility of all pipeline behavior after compilation

## Synthetic steps concept

When aspects are applied, the framework conceptually expands them into synthetic steps. For example, a pipeline with a persistence aspect might expand from:

`Input -> ProcessOrder -> Output`

To:

`Input -> ProcessOrder -> Persistence -> Output`

This expansion happens during compilation and is not visible in your source configuration.

## Side-effect transport model

Side-effect plugins observe stream elements and are exposed as unary services for the configured transport (gRPC or REST).
The service name is deterministic and aspect-qualified:
`Observe<AspectName><T>SideEffectService`, where `AspectName` is the PascalCase aspect name and `T` is the element type name.
This avoids collisions when multiple aspects observe the same type.

Placement depends on aspect position:
- `AFTER_STEP`: services observe the step output type.
- `BEFORE_STEP`: services observe the step input type.

## Plugin host modules

Plugin-server artifacts are generated only in modules that declare a `@PipelinePlugin("name")` marker. This keeps
plugin implementations out of regular service modules while still producing the required plugin-client and server
adapters for orchestrator and plugin deployments.

## Build-time requirements

- A pipeline config YAML must be available so the processor can discover step output types for type-indexed side-effect adapters.
  The loader searches the parent module root and a `config/` subfolder for `pipeline.yaml`, `pipeline-config.yaml`,
  or `*-canvas-config.yaml`.
- For gRPC transport, protobuf definitions must include the type-indexed, aspect-qualified
  `Observe<AspectName><T>SideEffectService` services for any observed type, and the descriptor set must include those
  definitions.
- For build-time parallelism validation, pass `-Apipeline.parallelism=SEQUENTIAL|AUTO|PARALLEL` to the annotation processor.
  You can also pass `-Apipeline.provider.class.<name>=<fqcn>` to validate provider ordering/thread-safety hints.

## Compilation flow (plugins)

```mermaid
flowchart TD
  A[pipeline-config.yaml] --> B[AspectExpansionProcessor]
  C[Descriptor set (gRPC only)] --> D[Transport binding resolver]
  B --> E[Synthetic side-effect steps]
  E --> D
  D --> F[Role-specific renderers]
  F --> G[plugin-server sources<br/>@PipelinePlugin module]
  F --> H[plugin-client sources<br/>@PipelinePlugin module]
  F --> I[pipeline-server / orchestrator-client sources]
  G --> J[plugin-server classifier JAR]
  H --> K[plugin-client classifier JAR]
```

## Expansion example

Consider a pipeline with two steps and a global persistence aspect applied AFTER each step:

Before aspect application:
```text
Order -> ValidateOrder -> ValidatedOrder -> ProcessPayment -> PaymentResult
```

After aspect application:
```text
Order -> ValidateOrder -> PersistValidation -> ProcessPayment -> PersistPayment -> PaymentResult
```

## Why this preserves determinism and deployability

By applying aspects at compile-time:
- The final pipeline structure is known before deployment
- There's no runtime configuration to manage
- Deployment packages are self-contained
- Behavior is predictable and testable

## Known limitations

- Aspect ordering within the same position and order value is implementation-dependent
- Complex aspect interactions may be difficult to reason about
- Build-time validation relies on plugin classes being present on the annotation processor classpath

## Intentional constraints

- Aspects cannot alter the functional behavior of the pipeline
- Plugin interfaces are limited to side-effect patterns
- Aspect configuration is declarative rather than programmatic

## Deployment Roles & Packaging Boundaries

- Role selection is a deployment concern; it must not change generated code.
- Aspect expansion decides deployment roles for synthetic steps; normal steps keep their configured role.
- Renderers are pure code generators; they only write to role-specific directories and apply `@GeneratedRole`.
- Packaging policy (classifier JARs, fat artifacts) is a build-time decision, not a generator decision.

## Non-goals

- Runtime aspect reconfiguration
- Aspect-to-aspect communication
- Aspects that change pipeline topology

## Future work

- Richer step selection for STEPS-scoped aspects
- Enhanced aspect configuration options
- Visualization tooling to show expanded pipeline structure
- More sophisticated ordering and conflict resolution
