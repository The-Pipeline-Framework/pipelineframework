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

## Same database, separate roles

Both the JPA query connector and the persistence plugin can use the same datasource, ORM configuration, and JPA entities in a Quarkus application. They are still separate features:

| Feature | Role | Typical timing |
| --- | --- | --- |
| JPA query connector | Captures read-side facts before a decision | Before a business step |
| Persistence plugin | Stores business outputs for later use | After a step or boundary |

The query connector is not bundled into the persistence plugin and does not require application-supplied connector code.

## Runtime boundaries

The public query connector/store contracts use JDK `CompletionStage` for unary boundaries. The Quarkus JPA connector uses Hibernate Reactive internally to run the read-only query, but application step code does not depend on Mutiny.

## Current limits

- `connector: "jpa"` is the only first-party query connector.
- `cardinality: "ONE_TO_ONE"` and `result: "single"` are required.
- Java record projection is the supported output shape.
- Predicates are `AND` only; no `OR` groups.
- No JPQL, named queries, aggregates, pagination, optional results, list results, or app-supplied query connector SPI.
- Simple dotted paths are accepted syntactically; invalid JPA paths fail deterministically when the connector executes the query.
