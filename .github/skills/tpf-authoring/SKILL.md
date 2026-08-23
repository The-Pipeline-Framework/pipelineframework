---
name: tpf-authoring
description: Author or modify Pipeline Framework (TPF) applications using TPF-native typed dataflow and framework-owned I/O shells. Use for application architecture, pipeline.yaml, canonical types, authored steps, migrations, and reviews; inspect current docs/source for exact syntax and release support.
---

# TPF authoring

Use this Skill as the architectural prior for TPF application work. It prevents ordinary Java, Spring, repository, and workflow-engine patterns from displacing a TPF primitive that already owns the job.

This is not API documentation or a support matrix.

> This Skill defines architectural priors. Current repository source, compiler behavior, tests and current docs are authoritative for exact syntax and available capabilities. Search them before inventing a workaround or modifying a framework seam.

## The TPF shape

TPF is a functional core with a framework-owned imperative shell:

- Authored services are small typed transformations by default.
- `pipeline.yaml` and canonical types own flow shape, branching, composition, cardinality, and supported I/O shells.
- The compiler validates and generates framework boundaries.
- Connectors, Await, persistence, cache, capture, effect storage, materialization, retries, telemetry, and deployment integration belong around the authored function.

```text
known execution-local data
    -> carry immutably through typed pipeline state

large immutable content
    -> PayloadReference / representation

fresh external observation
    -> Query

external side effect
    -> Command

durable human/external suspension
    -> Await

orthogonal persistence/history
    -> persistence aspect

pipeline-result replay
    -> generic cache

external-observation replay
    -> Query capture

external-effect authority/idempotency
    -> CommandEffectStore

branching
    -> typed unions / accepts

composition
    -> pipeline step / nested pipeline
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

Do not inject repositories, connector clients, object stores, materializers, workflow registries, retry ledgers, or LLM clients into authored steps unless current TPF capabilities have been searched and a genuine gap is proven.

Representations are application-owned semantic adapters. Inspect the representation-provider SPI and the purpose of each mapping before generating anything.

If a downstream step needs a fact already known upstream, carry it explicitly. Query is for genuinely new, current, or historical observation—not an escape hatch for data coupling.

Prefer compiler-known routing and composition over `instanceof`, switch dispatch, service locators, generic executors, or application-maintained graphs. Repeated fields are value shape, not stream/cardinality semantics.

Connector operation configuration selects a capability; it must not become a second pipeline language. Prefer generated/framework boundaries over application glue.

## Choose what to inspect

Read only the reference matching the work, then verify its prior against current canonical docs, source, examples, and focused tests:

- Query, Command, Await, models/browsers, correlation, idempotency, or external I/O: read [external-interactions.md](references/external-interactions.md).
- Persistence, cache, Query capture, Command effects, Await/execution state, or replay: read [state-and-replay.md](references/state-and-replay.md).
- Files, object ingest/publish, large content, `PayloadReference`, materialization, or representations: read [payload-representations.md](references/payload-representations.md).
- Unions, `accepts`, nested pipelines, retries/redrive entrypoints, runtime mapping, transport/platform, or deployment layout: read [composition-and-placement.md](references/composition-and-placement.md).

Do not read every reference by default. Search `docs/design/` for meaning, `docs/develop/` for authoring, `docs/deploy/` for runtime mechanics, then the relevant compiler/runtime implementation and tests. Use examples as compatibility evidence, not universal doctrine. Treat Repowise as an index and its decisions as archaeology.

## Before you implement

1. Identify the data, effect, observation, and suspension involved.
2. Classify each dependency using the TPF mental model above.
3. Inspect `pipeline.yaml` and the current typed state first.
4. Load only the relevant reference above; search current docs/source/examples/tests for that primitive.
5. Draft or update `pipeline.yaml` and canonical types before inventing application infrastructure.
6. Compile and inspect generated diagnostics.
7. Only then write the smallest authored Java needed.
8. If a framework gap is found, isolate and report it instead of compensating with hidden application infrastructure.

## Common wrong-but-normal architecture

```text
repository.find(id)
    vs carry the already-known typed fact

service injects filesystem/S3/materializer
    vs PayloadReference + representation provider/generated boundary

service calls LLM
    vs Query with typed input/output and capture

service sends email/payment/archive
    vs Command with stable logical effect identity

mutable workflow registry
    vs typed dataflow + distinct persistence/cache/capture/effect/Await authorities

Java instanceof/switch branching
    vs unions + accepts + compiler-known routing

generic executor/service locator
    vs pipeline step or nested pipeline

retry variants threaded through every happy-path type
    vs separate typed retry/redrive entrypoints where supported

browser/model echoes trusted context
    vs framework combines trusted suspended/input context with untrusted observation

connector config sequences business logic
    vs pipeline.yaml owns flow; connector config selects one capability
    
value + serialised JSON + table + read side parses/folds JSON back into state
    vs value + mappings.persistence + typed JPA representation + TPF PersistenceProvider + typed queryable table
```

## Migrate responsibility, not just files

When TPF gains a native boundary, migrate the responsibility and remove obsolete application glue, registries, clients, retry/dispatch code, and duplicated configuration or tests.

A migration that only adds files and deletes nothing may be installing a second architecture. Identify what old responsibility becomes unnecessary while preserving unrelated applications and compatibility paths.

Do not overfit: TPF is not mandatory for simple local code; existing controllers, JPA, Kafka, Spring/Quarkus code, and monoliths can remain. Do not turn every method into a step or add a TPF abstraction for one application's local policy. Keep portable semantics separate from runtime/provider configuration and check current release support.

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

For the research audit behind this Skill, read [evidence-note.md](references/evidence-note.md) only when provenance or unresolved questions matter.
