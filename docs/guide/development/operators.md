# Developing with Operators

TPF supports two operator execution models:

- local Java operators resolved at build time with `operator: Class::method`
- remote IDL v2 operators declared with `execution.mode: REMOTE`

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

## Remote Operator Example

Use the remote form when the executable code is outside the current Java build, but the step contract is still owned by the pipeline YAML.

```yaml
version: 2

messages:
  ChargeRequest:
    fields:
      - number: 1
        name: "orderId"
        type: "uuid"
  ChargeResult:
    fields:
      - number: 1
        name: "paymentId"
        type: "uuid"

steps:
  - name: "Charge Card"
    cardinality: "ONE_TO_ONE"
    inputTypeName: "ChargeRequest"
    outputTypeName: "ChargeResult"
    execution:
      mode: "REMOTE"
      operatorId: "charge-card"
      protocol: "PROTOBUF_HTTP_V1"
      timeoutMs: 3000
      target:
        urlConfigKey: "tpf.remote-operators.charge-card.url"
```

At runtime, the generated adapter:
- serializes the step input message as protobuf,
- issues an HTTP `POST` with `Content-Type: application/x-protobuf`,
- propagates canonical `x-tpf-*` metadata headers,
- decodes either the output protobuf message or a `google.rpc.Status` failure envelope.

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

Remote v2 operators use the step contract directly. The generated adapter still uses the normal mapper model to bridge domain types to generated protobuf message types. It does not resolve a Java operator implementation, and it does not use the FUNCTION remote adapter’s `BytesValue` + JSON contract.

## Remote Operator Constraints

- Remote operators are v2-only.
- Only `ONE_TO_ONE` is supported in this slice.
- `execution.target.urlConfigKey` is resolved at runtime startup. Missing or blank values fail startup.
- `execution.timeoutMs`, when set, caps the outbound HTTP call. The runtime also applies the propagated deadline if one is present, using the smaller of the two budgets.
- Retries and duplicate dispatch are possible. Remote operators are expected to be idempotent and to honor `x-tpf-idempotency-key`.

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
- `remote target missing at startup`: verify the property named by `execution.target.urlConfigKey` resolves to a non-blank URL.
- `remote operator deadline exceeded before dispatch`: verify step timeout, upstream deadline budget, and clock skew between caller and remote service.
- `remote operator returned non-retryable status`: inspect the `google.rpc.Status` envelope and validate request contract correctness before retrying.
- Build/CI failures: for failure signatures and triage flow, use [Operator Build Troubleshooting](/guide/development/operators-build-troubleshooting).

## Related

- [Operators](/guide/build/operators)
- [External Library Delegation](/guide/development/external-library-delegation)
- [Mappers and DTOs](/guide/development/mappers-and-dtos)
- [Extending TPF with Operator Libraries](/guide/development/extension/operator-libraries)
