# Runtime Split

Current runtime is Quarkus-heavy, so `framework/runtime` cannot become core unchanged.

| Target module | Should contain |
| --- | --- |
| `tpf-runtime-core` | Pipeline records, YAML/template models, cardinality, mapper contracts, lineage/telemetry event models, execution and await records, neutral store SPIs, neutral dispatch policies |
| `tpf-runtime-mutiny` | Mutiny execution adapters, current `Uni`/`Multi` contracts, Mutiny telemetry hooks, backpressure helpers |
| `tpf-runtime-quarkus` | CDI beans, Arc lookup, Quarkus config, RESTEasy resources/filters, Quarkus gRPC customizers, reactive messaging Kafka bridge, Vert.x context carrier, Quarkus AWS adapters |
| `tpf-runtime-reactor` | Reactor adapters, Reactor context propagation, scheduler/offload policy |
| `tpf-runtime-spring` | Spring bean lookup, Boot auto-configuration, WebFlux endpoints, Spring lifecycle hooks, Spring Kafka integration |

Candidate core seams:

- `BeanLookup`
- `RuntimeProfile`
- `ConfigProvider`
- `ExecutionContextCarrier`
- `ReactiveRuntime`
- `SchedulerBoundary`
- `EventBus`
- `TransactionBoundary`
