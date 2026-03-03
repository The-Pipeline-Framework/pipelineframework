# Developing with Operators

## Minimal Example

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

## Method Contract Checklist

- Format: `fully.qualified.Class::method`.
- Method must be public and non-abstract.
- At most one input parameter.
- No ambiguous overload resolution.
- Reactive returns should be explicit (`Uni<T>` / `Multi<T>` with generic type).

## Library Packaging Requirements

- Operator class is shipped in a module/JAR available at build/runtime.
- The library is visible in Jandex indexing.
- Instance operators are CDI-manageable.

## Mapper and Transport Notes

When application domain types differ from operator I/O types, mapper coverage is required for delegated/operator adapter paths.

- REST flow can work with direct JSON/domain mapping paths.
- gRPC flow requires descriptor + mapper-compatible bindings.
- Mapper fallback policies are configuration-driven; implicit conversion is not enabled by default.

## Example: AI Pipeline Chain

```yaml
steps:
  - name: "Chunk Document"
    operator: "com.example.ai.sdk.service.DocumentChunkingUnaryService::process"
  - name: "Embed Chunk"
    operator: "com.example.ai.sdk.service.ChunkEmbeddingService::process"
  - name: "Store Vector"
    operator: "com.example.ai.sdk.service.VectorStoreService::process"
  - name: "Search Similar"
    operator: "com.example.ai.sdk.service.SimilaritySearchUnaryService::process"
  - name: "Build Prompt"
    operator: "com.example.ai.sdk.service.ScoredChunkPromptService::process"
  - name: "LLM Complete"
    operator: "com.example.ai.sdk.service.LLMCompletionService::process"
```

## Troubleshooting

- `class not found`: verify module dependency and package name.
- `method not found/ambiguous`: verify signature and overloads.
- `unsupported return shape`: verify unary constraints for current invoker scope.
- `gRPC mapper/proto error`: verify mapper binding and descriptor generation.
- Build/CI failures: for failure signatures and triage flow, use [Operator Build Troubleshooting](/guide/development/operators-build-troubleshooting).

## Related

- [Operators](/guide/build/operators)
- [External Library Delegation](/guide/development/external-library-delegation)
- [Mappers and DTOs](/guide/development/mappers-and-dtos)
- [Extending TPF with Operator Libraries](/guide/development/extension/operator-libraries)
