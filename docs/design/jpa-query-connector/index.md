# JPA Query Connector

The Hibernate query connectors let a pipeline declare a database read as an explicit `kind: query`
step. The blocking `jpa.query` provider uses Hibernate ORM/JPA; the non-blocking
`hibernate.reactive.query` provider uses Hibernate Reactive. Both share the same validated HQL,
predicate, cardinality, and projection model.

The intended shape is:

```text
LoadCustomerRiskFacts(CustomerRiskLookup) -> CustomerRiskFacts
AssessCustomerRisk(CustomerRiskFacts) -> RiskDecision
```

instead of hiding the read inside:

```text
AssessCustomerRisk(CustomerRiskLookup) -> RiskDecision
```

This keeps the decision facts visible in topology, tests, replay data, and audits. Application code
owns the entity, input record, output record, and decision step. TPF owns connector resolution,
session handling, predicate binding, projection, duplicate-row handling, and captured query replay.

## When to use it

Use a JPA query step when:

- a read affects a business branch, score, eligibility check, approval, routing decision, or enrichment
- retrying the same managed execution should reuse the already captured facts instead of rereading newer database state
- the decision step should be pure Java over an immutable input record

Keep ordinary repository code for local implementation details that do not define a pipeline boundary. When pipeline outputs should be stored for audit, APIs, reports, or UIs, use the [Persistence Plugin](/design/persistence). The query connector and persistence plugin are complementary: one captures decision inputs, the other stores business outputs.

## Guide pages

- [Setup and YAML](/design/jpa-query-connector/setup) shows the dependency, query definition, step declaration, and Java record shape.
- [Predicates and Selection](/design/jpa-query-connector/predicates) covers supported operators, dotted paths, projection, ordering, and duplicate-row behavior.
- [Capture, Replay, and Persistence](/design/jpa-query-connector/capture-and-persistence) explains captured replay, the relationship to the persistence plugin, runtime boundaries, and current limits.

## Current shape

- `jpa.query` is the blocking Hibernate ORM/JPA provider and is safely isolated on framework workers.
- `hibernate.reactive.query` is the non-blocking Hibernate Reactive provider and uses
  `Mutiny.SessionFactory` internally without making Mutiny part of the provider SPI.
- `kind: "query"` is the framework-owned step type.
- `cardinality: "ONE_TO_ONE"` and `result: "single"` are required.
- Java record projection is the supported output shape.
- App developers do not implement connector classes or call Hibernate sessions from the query step.

Both providers expose a unary `CompletionStage` Query. That contract does not provide element-level
backpressure; the reactive provider preserves non-blocking database execution and composes with TPF
admission, while the blocking provider isolates synchronous database work on workers. Applications
using Panache entities can use the reactive provider, but the connector itself does not depend on
Panache.

For the architectural rationale behind captured query steps, see [I/O Shell Absorption](/evolve/io-shell-absorption#captured-query-steps-for-dbapi-reads).
