# Operators for Developers

This page explains how application developers should write and use operators in day-to-day TPF work.

## Quick Start

```yaml
steps:
  - name: "Enrich Payment"
    operator: "com.acme.payment.PaymentOperators::enrich"
```

```java
package com.acme.payment;

public class PaymentOperators {
    public PaymentOut enrich(PaymentIn in) {
        return new PaymentOut(in.id(), "ENRICHED");
    }
}
```

## Developer Rules

- Operator format: `fully.qualified.Class::method`.
- Method must be uniquely resolvable (no ambiguous overloads).
- Method must have at most one parameter.
- Use explicit generics for reactive returns (`Uni<T>`, `Multi<T>`).

## Return Shape Behavior

- Non-reactive returns are normalized to reactive metadata at build time.
- Streaming shapes may be represented in metadata, but current Phase 1 invocation focuses on unary flow.

## Phase 1 Operator Library Requirements

- The operator class must be packaged in a module/JAR available on the build/runtime classpath.
- The class must be indexed so build-time resolution can find it via Jandex.
- Operator methods must be public, non-abstract, and unambiguous.
- Reactive returns must use explicit generic parameters (`Uni<T>`/`Multi<T>`).
- For instance methods, the class must be CDI-manageable so TPF can inject and invoke it.
- If your pipeline domain types differ from operator I/O types, ensure mapper coverage for those types in your project.

## Practical Guidance

- Keep operator methods small and domain-focused.
- Prefer stable DTO/domain contracts between steps.
- Treat operator methods as pipeline API surfaces: version carefully.

## Troubleshooting Example

- Error: class or method not found.
- Check: operator FQCN/method name, module dependency, and Jandex indexing visibility.

## Related

- [Operators (YAML Build-Time)](/guide/build/operators)
- [Mappers and DTOs](/guide/development/mappers-and-dtos)
- [Testing with Testcontainers](/guide/development/testing)
