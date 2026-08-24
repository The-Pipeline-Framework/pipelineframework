---
name: tpf-authoring
description: Design, author, or migrate applications built with The Pipeline Framework (TPF). Use for pipeline.yaml, canonical types, authored services/operators, mappings, deployment shape, and application architecture; not for maintaining TPF compiler/runtime internals.
---

# TPF authoring

Use this Skill as the architectural prior for TPF application work. It prevents ordinary Java, Spring, repository, and workflow-engine patterns from displacing a TPF mechanism that already owns the job. This is not API documentation, a support matrix, or guidance for maintaining TPF itself.

> This Skill defines architectural priors. Current repository source, compiler behavior, tests and current docs are authoritative for exact syntax and available capabilities. Search them before inventing an application workaround.

## The TPF shape

TPF is a functional core with a framework-owned imperative shell:

- Authored services are small typed transformations by default.
- `pipeline.yaml` and canonical types own flow shape, branching, composition, cardinality, and supported I/O shells.
- The compiler validates the model and generates adapters, routing, metadata, and runtime boundaries.
- Connector/provider setup, suspension, durability, replay, retries, telemetry, and placement stay around the authored function.

```text
known execution-local data        -> carry immutably through typed pipeline state
large immutable content           -> PayloadReference / representation
fresh external observation        -> Query
external side effect              -> Command
durable human/external suspension -> Await
orthogonal persistence/history    -> persistence aspect
pipeline-result replay            -> generic cache
external-observation replay       -> Query capture
external-effect authority         -> CommandEffectStore
branching                          -> typed unions / accepts
composition                        -> pipeline step / nested pipeline
typed iteration / agentic looping -> bounded recursive nested pipeline
```

These are different jobs with different authorities. Do not collapse them into an application registry.

## Author the functional core

The step input should contain what the decision needs. The service transforms it; it does not rediscover known data, open storage, call a model, perform the external effect, update a workflow row, or select a runtime.

```java
record ApprovedPayment(PaymentState payment, ApprovalId approvalId) {}

final class BuildReceipt implements ReactiveService<ApprovedPayment, Receipt> {
    public Uni<Receipt> process(ApprovedPayment input) {
        return Uni.createFrom().item(receiptFrom(input));
    }
}
```

Do not inject repositories, connector clients, object stores, materializers, workflow registries, retry ledgers, or LLM clients into authored steps unless current TPF capabilities have been searched and a genuine gap is proven. If the pipeline already knows a fact, carry it. Query is only for a new/current/historical observation.

## Choose the authored shape before Java

- One input -> one result is `ONE_TO_ONE`; one input -> stream is `ONE_TO_MANY`; stream -> one result is `MANY_TO_ONE`; stream -> stream is `MANY_TO_MANY`.
- Put cardinality in pipeline topology. Do not hide a stream boundary inside an arbitrary `List` merely because Java collections are convenient.
- Prefer reactive shapes for async work and end-to-end backpressure. Use an explicit blocking shape for synchronous libraries; list forms materialize, while the supported iterator shape is incremental.
- Use an ordinary service for an application transformation. Use an operator when a reusable/delegated execution unit owns its own model or genuinely needs an independently selectable boundary. A helper method or one application's policy is not an operator.
- Query, Command, and Await are semantic I/O boundaries, not operator variants.
- Repeated fields are value shape, not stream/cardinality semantics. Use unions and `accepts` for compiler-known branch applicability, not Java `instanceof` or switch dispatch.

Read [authoring-model.md](references/authoring-model.md) for types, operators, cardinality, backpressure, unions, nested pipelines, recursion, failure channels, or tests.

## Keep value identities separate

```text
canonical pipeline value
    != necessarily authored Java representation
    != necessarily persistence representation
    != necessarily wire representation
```

A `Path` can be a local representation of canonical `payload_ref`; it should not leak into durable state. `java:` binds the Java execution type. A type's `mappings:` describes a named external representation for a consumer. A representation provider may generate the boundary. Connector mappers adapt admission/publication. None of these declarations automatically defines wire identity.

Inspect representation-provider support before adding glue, changing canonical types to suit a local API, or carrying JPA entities, provider DTOs, `Path`, bytes, or storage locations through the pipeline.

Read [representations-and-runtime-mappings.md](references/representations-and-runtime-mappings.md) for Java bindings, mappers, persistence/file representations, object ingest/publish, materialization, or wire/local/durable identity.

## Keep execution, configuration, and placement explicit

Connector operation configuration selects one capability; it must not become a second pipeline language. Prefer generated/framework boundaries over generic executors, service locators, provider factories, or application glue.

```text
portable semantic choice      -> pipeline.yaml / canonical types
generated contract or adapter -> build-time configuration, then rebuild
framework policy/provider     -> runtime or deployment configuration
dynamic business fact         -> typed pipeline input
```

Do not inject settings registries into authored steps. Telemetry boundary metadata and available instrumentation are build-produced; enablement, exporters, backends, and operational thresholds are runtime/deployment concerns.

Start with the simplest supported deployment shape. Add runtime/deployment separation only for a real isolation, scaling, security, failure, or ownership need. Runtime layout is logical placement; Maven/build topology decides artifacts; transport decides how calls cross boundaries. `pipeline.runtime.yaml` does not convert values or rewrite Maven modules.

- Read [execution-and-replay.md](references/execution-and-replay.md) for Query/Command/Await, connectors, aspects, persistence/cache/capture/effects, resilience, retry/DLQ, or checkpoint handoff.
- Read [deployment-and-packaging.md](references/deployment-and-packaging.md) for configuration lifetime, telemetry, runtime placement, transport/platform, generated artifacts, bootstrap, testing, or single-unit packaging.

Do not load every reference. Search `docs/design/` for meaning, `docs/develop/` for authoring, and `docs/deploy/` for runtime mechanics, then the relevant compiler/runtime code and focused tests. Read the relevant record under `docs/decisions/` only when application docs leave the owning TPF primitive or an important trade-off unclear. Examples prove compatibility; they may contain historical or application-specific residue. Repowise is an index and archaeological lead, never authority.

## Before you implement

1. Identify the data, effect, observation, and suspension involved.
2. Classify each dependency using the TPF shape above.
3. Inspect `pipeline.yaml` and the current typed state first.
4. Load only the relevant reference; search current docs/source/examples/tests for the primitive.
5. Draft or update `pipeline.yaml` and canonical types before inventing infrastructure.
6. Compile and inspect generated diagnostics and artifacts.
7. Only then write the smallest authored Java needed.
8. If a framework gap remains, isolate and report it instead of hiding it in application infrastructure.

## Common wrong-but-normal architecture

```text
repository.find(id)                          vs carry the already-known typed fact
service injects filesystem/S3/materializer   vs PayloadReference + generated representation boundary
service calls LLM                            vs Query with typed input/output and capture
service sends email/payment/archive          vs Command with stable logical effect identity
mutable workflow registry                    vs typed flow + distinct durability/replay/effect/Await stores
Java instanceof/switch branching             vs unions + accepts + compiler-known routing
generic executor/service locator              vs ordinary service, operator, or nested pipeline as intended
List used to conceal streaming                vs explicit cardinality + matching reactive/blocking contract
Path/JPA entity/provider DTO in durable state vs canonical value + local/persistence/wire representation
retry variants in every happy-path type       vs typed failure plus runtime retry or separate redrive entrypoint
browser/model echoes trusted context          vs framework combines trusted state with untrusted observation
connector config sequences business logic     vs pipeline.yaml owns flow; connector config selects capability
layout flag assumed to create JARs             vs align runtime layout with the intended build topology
```

## Migrate ownership, not just files

When TPF gains a native boundary, remove the application glue, registry, client, dispatch/retry code, duplicated configuration, and obsolete tests whose responsibility it replaces. A migration that only adds files and deletes nothing may be installing a second architecture.

Do not overfit. TPF is not mandatory for simple local code; existing controllers, JPA, Kafka, Spring/Quarkus code, and monoliths can remain. Do not turn every method into a step or add a framework abstraction for one application's policy. Keep portable semantics separate from provider/runtime configuration and verify current release support.

## Final self-check

- Did I introduce hidden I/O into an authored step?
- Did I query data the pipeline already knew?
- Did I confuse result replay, observation replay, persistence, and effect authority?
- Did I make large payloads durable as raw application state rather than references?
- Did I use Java routing where TPF already knows the type branch?
- Did I introduce provider/runtime detail into portable pipeline semantics?
- Did I trust model/browser input to echo authoritative context?
- Did I add application infrastructure instead of reporting a framework gap?
- Did this migration remove obsolete application responsibility?
- Did I verify current TPF reality before claiming a missing capability?
