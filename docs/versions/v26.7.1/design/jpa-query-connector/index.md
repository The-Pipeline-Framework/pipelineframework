---
search: false
---

# JPA Query Connector

The JPA query connector lets a pipeline declare a database read as an explicit `kind: query` step. Use it when mutable database state affects a downstream business decision.

The intended shape is:

```text
LoadCustomerRiskFacts(CustomerRiskLookup) -> CustomerRiskFacts
AssessCustomerRisk(CustomerRiskFacts) -> RiskDecision
```

instead of hiding the read inside:

```text
AssessCustomerRisk(CustomerRiskLookup) -> RiskDecision
```

This keeps the decision facts visible in topology, tests, replay data, and audits. Application code owns the JPA entity, input record, output record, and decision step. TPF owns connector resolution, read-only JPA session handling, predicate binding, projection, duplicate-row handling, and captured query replay.

## When to use it

Use a JPA query step when:

- a read affects a business branch, score, eligibility check, approval, routing decision, or enrichment
- retrying the same managed execution should reuse the already captured facts instead of rereading newer database state
- the decision step should be pure Java over an immutable input record

Keep ordinary repository code for local implementation details that do not define a pipeline boundary. When pipeline outputs should be stored for audit, APIs, reports, or UIs, use the [Persistence Plugin](/versions/v26.7.1/design/persistence). The query connector and persistence plugin are complementary: one captures decision inputs, the other stores business outputs.

## Guide pages

- [Setup and YAML](/versions/v26.7.1/design/jpa-query-connector/setup) shows the dependency, query definition, step declaration, and Java record shape.
- [Predicates and Selection](/versions/v26.7.1/design/jpa-query-connector/predicates) covers supported operators, dotted paths, projection, ordering, and duplicate-row behavior.
- [Capture, Replay, and Persistence](/versions/v26.7.1/design/jpa-query-connector/capture-and-persistence) explains captured replay, the relationship to the persistence plugin, runtime boundaries, and current limits.

## Current shape

- `connector: "jpa"` is the first-party query connector.
- `kind: "query"` is the framework-owned step type.
- `cardinality: "ONE_TO_ONE"` and `result: "single"` are required.
- Java record projection is the supported output shape.
- App developers do not implement connector classes and do not call `Panache.withSession()`, Hibernate sessions, or transaction helpers from the query step.

For the architectural rationale behind captured query steps, see [I/O Shell Absorption](/versions/v26.7.1/evolve/io-shell-absorption#captured-query-steps-for-dbapi-reads).
