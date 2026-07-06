# Agent Glossary

Load this file when terminology, docs wording, transport/platform naming, architecture explanations, or public-facing copy matters.

## Canonical Terms

- **Functional core**: typed Java business logic that transforms explicit input contracts into explicit output contracts. It should not own persistence, transport, retries, correlation, polling, or deployment wiring.
- **Imperative shell**: framework-owned or infrastructure-owned code around the functional core: generated adapters, connectors, await handling, persistence, caching, materialization, replay, telemetry, retries, and deployment/runtime integration.
- **Pipeline**: a strongly typed application flow made of ordered steps. It is not a CI/CD pipeline, generic workflow diagram, or arbitrary orchestration graph.
- **Step**: one typed unit in the pipeline. A step may be an authored Java service, an operator reference, an await boundary, a connector-backed boundary, or another semantic step kind supported by YAML/compiler validation.
- **Business function**: ordinary application code that makes a domain decision or transformation. Prefer keeping it transport-neutral and framework-light.
- **Operator**: an existing Java method, class method reference, or remote endpoint reused as a pipeline step, for example `operator: fully.qualified.Class::method`. Operator references are resolved and validated at build time where possible.
- **Mapper**: typed boundary translation code between domain types and external/operator/transport types. Mapper selection must stay pair-accurate (`Domain` + `External`) and deterministic in ambiguity diagnostics.
- **Connector**: framework-owned I/O shell that admits or publishes external reality while preserving a typed pipeline boundary. Examples include object ingest/publish and captured query connectors. Connectors are not generic plugins.
- **Plugin**: cross-cutting framework extension such as persistence, caching, telemetry, or logging. Plugins run through declared aspect/side-effect rules and should not redefine the application contract.
- **Aspect**: a declared rule for where plugin side effects run relative to pipeline steps, such as before/after a step. Aspects are semantic side effects expanded during compilation.
- **Generated adapter**: TPF-generated REST, gRPC, local, function-style, worker-facing, or envelope/wire-boundary code that calls the business function without moving transport logic into the function.
- **Transport mode**: the top-level generated component call mode selected by `pipeline.transport`. Canonical transport modes are `GRPC`, `REST`, and `LOCAL`. Do not describe `FUNCTION`, `HTTP_LAMBDA`, `PROTOBUF_HTTP_V1`, or `ENVELOPE_HTTP_V1` as top-level transport modes.
- **Deployment pattern**: a composed runtime/deployment shape, not a transport mode. `HTTP_LAMBDA` is the HTTP/Lambda-style path implemented as `pipeline.transport=REST` with `pipeline.platform=FUNCTION`.
- **Wire/envelope protocol**: payload or invocation envelope shape used at a boundary, layered on top of a transport or dispatch substrate. `PROTOBUF_HTTP_V1` and `ENVELOPE_HTTP_V1` describe HTTP boundary encoding/contract shape for remote step hosts/operators; they do not replace `REST`, `GRPC`, or `LOCAL` as `pipeline.transport` values.
- **Worker invocation protocol**: how a queue-async coordinator invokes remote transition workers. Current protocols include REST, gRPC, and SQS-style worker targets. This is related to, but narrower than, public transport mode.
- **Platform mode**: generated runtime platform shape. `COMPUTE` is the service/resource execution path; `FUNCTION` is the function-handler path. Platform and transport are orthogonal dimensions.
- **Function platform**: `pipeline.platform=FUNCTION`, used for function-style entry points such as AWS Lambda-oriented flows. It normally composes with `pipeline.transport=REST` today; it is not a transport mode.
- **Runtime layout**: logical runtime shape declared in runtime mapping, such as `modular`, `pipeline-runtime`, or `monolith`. It describes where orchestrator, steps, and side effects run.
- **Build topology**: Maven module/POM/JAR/container structure that physically builds deployables for a runtime layout. Runtime layout and build topology are related but not interchangeable.
- **Runtime mapping**: YAML/configuration that declares runtime placement, grouping, targets, synthetics, plugins/aspects, and layout choices. Runtime mapping does not automatically reshape Maven modules.
- **Orchestrator runtime**: generated runtime surface that admits pipeline work, invokes steps, exposes status/result APIs where applicable, and owns runtime behavior such as queue-async execution.
- **`QUEUE_ASYNC`**: orchestrator mode for durable background execution. TPF stores execution state, dispatches work, retries failed transitions, recovers leased work after crashes, and can route terminal failures to a DLQ.
- **Await boundary**: a typed step boundary where execution pauses for external reality, such as human approval, webhook callback, provider response, brokered reply, or long-running job completion.
- **Await unit**: durable runtime record for an await boundary. It tracks pending interaction state, completion admission, correlation, timeout, duplicate completion handling, and resume semantics.
- **Checkpoint handoff**: stable cross-pipeline admission boundary. One pipeline publishes a checkpoint; another pipeline admits it and owns the downstream lifecycle, retries, and DLQ after admission.
- **Persistence**: durable business output storage for later APIs, reports, UIs, audit, or follow-on processing. It is not the same as queue-async execution state.
- **Caching**: reuse of deterministic derived outputs to avoid expensive recomputation during normal execution or replay. It is not the durable business record.
- **Field materialization**: claim-check style representation policy for large fields. TPF can move payloads out of line via `payload_ref` while preserving the semantic message contract.
- **Replay**: rerunning or reconstructing execution from captured lineage, state surfaces, cache, persistence, await completions, and connector snapshots where supported. Replay is bounded by what was captured.
- **Lineage**: deterministic metadata that tracks where an item came from and which step produced it, including split/merge relationships. Treat lineage behavior as deterministic, not best-effort.
- **DLQ**: dead-letter channel for terminal execution failures that need investigation or replay.
- **TPFGo**: reference/evolution work around checkpoint-style business flows and DDD alignment. Do not present TPFGo roadmap material as general product behavior unless examples and tests prove it.
