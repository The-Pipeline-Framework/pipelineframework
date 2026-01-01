# Architectural overview (AOP-style, high level)

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

## Expansion example

Consider a pipeline with two steps and a global persistence aspect applied AFTER each step:

Before aspect application:
```
Order -> ValidateOrder -> ValidatedOrder -> ProcessPayment -> PaymentResult
```

After aspect application:
```
Order -> ValidateOrder -> PersistValidation -> ProcessPayment -> PersistPayment -> PaymentResult
```

## Why this preserves determinism and deployability

By applying aspects at compile-time:
- The final pipeline structure is known before deployment
- There's no runtime configuration to manage
- Deployment packages are self-contained
- Behavior is predictable and testable

## Known limitations

- STEPS-scoped aspects are not yet fully implemented
- Aspect ordering within the same position and order value is implementation-dependent
- Complex aspect interactions may be difficult to reason about

## Intentional constraints

- Aspects cannot alter the functional behavior of the pipeline
- Plugin interfaces are limited to side-effect patterns
- Aspect configuration is declarative rather than programmatic

## Non-goals

- Runtime aspect reconfiguration
- Aspect-to-aspect communication
- Aspects that change pipeline topology

## Future work

- Richer step selection for STEPS-scoped aspects
- Enhanced aspect configuration options
- Visualization tooling to show expanded pipeline structure
- More sophisticated ordering and conflict resolution