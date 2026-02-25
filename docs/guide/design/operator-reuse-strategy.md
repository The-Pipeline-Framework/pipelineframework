# Operator Reuse Strategy

This guide helps architects and product leaders decide when to reuse external libraries as operators in TPF pipelines.

## Why This Matters

Operator reuse changes delivery economics:

- You keep proven compute logic instead of rewriting it as new step services.
- You reduce migration scope and delivery risk.
- You can modernize orchestration and transport incrementally while preserving domain logic ownership.

## What Can Be Reused

In practice, operators are a good fit for:

| Library Type | Typical Fit | Notes |
| --- | --- | --- |
| Domain calculators/rules engines | High | Deterministic logic, stable contracts, low integration risk. |
| Validation/enrichment libraries | High | Works well as unary request/response operators. |
| Data transformation/mapping libraries | High | Good for normalization and shaping steps. |
| ML/AI client wrappers | Medium | Useful when wrapped as unary calls with explicit DTO contracts. |
| Legacy service facades | Medium | Valuable for migration, but dependency/runtime constraints must be reviewed. |
| Heavy streaming engines | Low (current) | Current invocation path is unary-focused. |

## Readiness Checklist for a Reusable Operator Library

- Packaged as a dependency (module/JAR) available to the pipeline build/runtime.
- Public operator method with unambiguous name/signature.
- Compatible input/output contract for pipeline flow.
- Indexed for build-time resolution (Jandex visibility).
- If instance method: CDI-manageable class and dependencies.

## Architecture Decisions

When to prefer operator reuse:

- Existing logic is trusted, tested, and already in production.
- Time-to-value is prioritized over rebuilding internals.
- Teams want central orchestration with distributed logic ownership.

When to avoid operator reuse:

- Library API is unstable or tightly coupled to a specific runtime environment.
- The logic requires deep streaming semantics not yet supported in the target flow.
- Ownership/governance of the external library is unclear.

## Product Planning Signal

For portfolio planning, operators are useful when you need visible progress without full rewrites:

1. Reuse core business compute now.
2. Standardize transport and orchestration in TPF.
3. Replace/refactor internals later only where ROI is clear.

## Related

- [Operators](/guide/build/operators)
- [Runtime Topology Strategy](/guide/design/runtime-topology-strategy)
- [Business Value](/value/business-value)
