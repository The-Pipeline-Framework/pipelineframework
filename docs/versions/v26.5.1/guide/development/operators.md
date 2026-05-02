---
search: false
---

# Developing with Operators

TPF supports two operator execution models:

- local Java operators resolved at build time with `operator: fully.qualified.Class::method`
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

- Remote v2 operators use the step contract directly.
- The generated adapter still uses the normal mapper model to bridge domain types to generated protobuf message types.
- It does not resolve a Java operator implementation.
- It does not use the FUNCTION remote adapter’s `BytesValue` + JSON contract.

## Remote Operator Constraints

- Remote operators are v2-only.
- Only `ONE_TO_ONE` is supported currently.
- The generated adapter sends its HTTP `POST` to the fully configured target URL. If you use `execution.target.url`, include the full path to call. If you use `execution.target.urlConfigKey`, the configured value must also be the full target URL, including any path segment.
- The value of `execution.target.urlConfigKey` is resolved at application startup; if it is missing or blank, the application will fail to start.
- `execution.timeoutMs`, when set, caps the outbound HTTP call. The runtime also applies the propagated deadline if one is present, using the smaller of the two budgets.
- Retries and duplicate dispatch are possible. This can happen due to automatic retries, transport-layer or network duplicates, and manual replays. For example, an automatic retry after a timeout can race with the original in-flight request. Remote operators are expected to be idempotent and to honour `x-tpf-idempotency-key` across those cases.

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

- `class not found`: verify module dependency, package name, and that the operator uses `operator: fully.qualified.Class::method` rather than a shortened class name.
- `method not found/ambiguous`: verify signature, overloads, and the `fully.qualified.Class::method` double-colon syntax.
- `unsupported return shape`: verify unary constraints for current invoker scope.
- `gRPC mapper/proto error`: verify mapper binding and descriptor generation.
- `remote target missing at startup`: verify the property named by `execution.target.urlConfigKey` resolves to a non-blank URL.
- `remote operator deadline exceeded before dispatch`: verify step timeout, upstream deadline budget, and clock skew between caller and remote service.
- `remote operator returned non-retryable status`: inspect the `google.rpc.Status` envelope and validate request contract correctness before retrying.
- Build/CI failures: for failure signatures and triage flow, use [Operator Build Troubleshooting](/versions/v26.5.1/guide/development/operators-build-troubleshooting).

## Related

- [Operators](/versions/v26.5.1/guide/build/operators)
- [External Library Delegation](/versions/v26.5.1/guide/development/external-library-delegation)
- [Mappers and DTOs](/versions/v26.5.1/guide/development/mappers-and-dtos)
- [Extending TPF with Operator Libraries](/versions/v26.5.1/guide/development/extension/operator-libraries)
