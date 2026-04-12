---
search: false
---

# Client Step Extensions

Client steps are generated classes used by the orchestrator to call backend services. You can extend them when you need custom behaviour.

## Typical Extensions

1. Add metrics around remote calls
2. Wrap calls with retries or fallbacks
3. Enrich headers or metadata before invoking the client step (gRPC, REST, or Local)

## Client step types

- `GrpcClientStep`: use when the runtime boundary is remote and you want the stricter protobuf contract and generated stubs.
- `RestClientStep`: use when the runtime boundary is remote and HTTP/JSON interoperability matters more than protobuf transport efficiency.
- `LocalClientStep`: use when the step runs in-process and you want the same orchestration shape without a network hop.

## Extend a generated client step

Subclass the generated step when you need custom headers, metrics, or policy around the generated invocation.

```java
@ApplicationScoped
public class InstrumentedParseDocumentClientStep extends ParseDocumentGrpcClientStep {
    @Inject
    MeterRegistry registry;

    @Override
    public Uni<ParsedDocument> invoke(ParsedDocumentRequest request) {
        Timer.Sample sample = Timer.start(registry);
        return super.invoke(request)
            .invoke(() -> sample.stop(registry.timer("tpf.client.parse-document")))
            .onFailure().invoke(error -> registry.counter("tpf.client.parse-document.failures").increment());
    }
}
```

## Common customisations

- Add metrics around remote calls.
- Wrap calls with retries or fallbacks.
- Enrich headers or metadata before invoking the client step (`GrpcClientStep`, `RestClientStep`, or `LocalClientStep`).

## Related docs

- [Pipeline Compilation](/versions/v26.2.5/guide/build/pipeline-compilation)
- [Orchestrator Runtime](/versions/v26.2.5/guide/development/orchestrator-runtime)
- [Operators](/versions/v26.2.5/guide/development/operators)
