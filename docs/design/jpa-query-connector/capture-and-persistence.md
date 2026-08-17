# Capture, Replay, and Persistence

The JPA query connector is read-side infrastructure. It turns a decision-affecting database read into an explicit captured input for the next business step.

The persistence plugin is write-side infrastructure. It observes pipeline values and stores business outputs for audit, APIs, reports, UIs, and follow-on processing.

Used together, they give a strong state story:

1. Query connector loads and captures the facts a decision used.
2. The decision step stays pure Java over those facts.
3. Persistence stores the resulting business output in the application's durable record.

## Capture and replay

During managed TPF execution, the generated query step computes a capture key from tenant id, execution id, step index, query id, query version, and the selected `capture.keyFields` from the input. The first read result is stored and later retries of the same execution reuse the captured output instead of rereading mutable database state.

If `capture.keyFields` is omitted, the full query input becomes the key input. Prefer explicit key fields when the input contains non-decision metadata.

The current store is the in-memory query capture store. That proves the framework semantics and local replay behavior; durable query capture stores are the production hardening path for restart-safe replay.

Provider-backed Query operations use the same capture boundary. `Found` captures the typed output.
`NotFound` captures an explicit absence and replays it as `QueryNotFoundException`; absence is never
represented by `null` or a fabricated output object. Temporary availability, authentication, and
terminal provider failures are not captured as observations.

Query capture is separate from generic step-result caching:

1. A permitted generic cache hit replays the versioned step output before Query runtime.
2. On a cache miss or bypass, Query runtime checks the execution-scoped capture before resolving a provider.
3. Only when neither replay source supplies an observation does the JPA connector or selected provider execute live.

Consequently, `REQUIRE_CACHE` misses before capture lookup, while `BYPASS_CACHE` bypasses only the
generic cache and can still replay an existing execution capture. The generic cache key, Query
capture key, and live provider identity remain separate contracts.

## Same database, separate roles

Both the JPA query connector and the persistence plugin can use the same datasource, ORM configuration, and JPA entities in a Quarkus application. They are still separate features:

| Feature | Role | Typical timing |
| --- | --- | --- |
| JPA query connector | Captures read-side facts before a decision | Before a business step |
| Persistence plugin | Stores business outputs for later use | After a step or boundary |

The query connector is not bundled into the persistence plugin and does not require application-supplied connector code.

## Runtime boundaries

The public provider `QueryOperation` and query connector/store contracts use JDK `CompletionStage`
for unary boundaries. The Quarkus JPA connector uses Hibernate Reactive internally to run the
read-only query, but provider and application step code do not depend on Mutiny.

## Current limits

- `connector: "jpa"` is the only first-party query connector.
- `cardinality: "ONE_TO_ONE"` and `result: "single"` are required.
- Java record projection is the supported output shape.
- Predicates are `AND` only; no `OR` groups.
- The first provider-operation runtime is unary only; no pagination or public streaming contract is exposed.
- JPA declarations still do not support JPQL, named queries, aggregates, optional results, or list results.
- Simple dotted paths are accepted syntactically; invalid JPA paths fail deterministically when the connector executes the query.
