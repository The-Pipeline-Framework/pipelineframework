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

Do not use it for simple repository code that is purely internal to one service transaction. Normal Spring, Quarkus, JPA, and Java APIs still own ordinary application persistence.

## Dependency

Add the connector where the generated query step runs:

```xml
<dependency>
    <groupId>org.pipelineframework</groupId>
    <artifactId>query-jpa-connector</artifactId>
    <version>${pipelineframework.version}</version>
</dependency>
```

The connector is first-party and declarative. Application developers do not implement connector classes and do not call `Panache.withSession()`, Hibernate sessions, or transaction helpers from the query step.

## YAML

Declare the query once under `queries`, then reference it from a `kind: query` step.

```yaml
queries:
  latest-active-risk:
    connector: "jpa"
    input: "com.example.CustomerRiskLookup"
    output: "com.example.CustomerRiskFacts"
    version: "v2"
    jpa:
      entity: "com.example.CustomerRiskEntity"
      where:
        customerId: "input.customerId"
        status:
          eq: "ACTIVE"
        score:
          gte: "input.minimumScore"
        deletedAt:
          isNull: true
      orderBy:
        updatedAt: "desc"
      limit: 1
      projection:
        customerId: "customerId"
        riskBand: "riskBand"
        score: "score"
        accountStatus: "account.status"
      result: "single"

steps:
  - name: "Load Customer Risk Facts"
    kind: "query"
    cardinality: "ONE_TO_ONE"
    query: "latest-active-risk"
    input: "com.example.CustomerRiskLookup"
    output: "com.example.CustomerRiskFacts"
    capture:
      keyFields: ["customerId"]

  - name: "Assess Customer Risk"
    service: "com.example.risk.AssessCustomerRiskService"
    cardinality: "ONE_TO_ONE"
    input: "com.example.CustomerRiskFacts"
    output: "com.example.RiskDecision"
```

`version` participates in captured query identity. Bump it when the read meaning changes, such as a predicate or projection change that should not reuse earlier captured facts.

## Java shape

Use small input and output records that make the read boundary obvious:

```java
package com.example;

public record CustomerRiskLookup(String customerId, int minimumScore) {
}
```

```java
package com.example;

public record CustomerRiskFacts(
    String customerId,
    String riskBand,
    int score,
    String accountStatus
) {
}
```

The output projection supports Java records. The projection keys are output record component names. The projection values are entity property paths.

## Predicate support

`where` supports the legacy equality shorthand:

```yaml
where:
  customerId: "input.customerId"
```

That is equivalent to:

```yaml
where:
  customerId:
    eq: "input.customerId"
```

Supported operators:

| Operator | Shape | Notes |
| --- | --- | --- |
| `eq` | scalar | Equality. |
| `in` | scalar or array | A scalar may reference an input collection or array. |
| `gt` / `gte` | scalar | Greater-than comparisons. |
| `lt` / `lte` | scalar | Less-than comparisons. |
| `between` | two-item array | Inclusive JPA `between`. |
| `like` | scalar | JPA `like`; provide the wildcard pattern explicitly. |
| `isNull` | boolean or `"true"` / `"false"` | Emits `is null` or `is not null`; no parameter is bound. |

Multiple predicates are combined with `AND`.

Scalar values can be literals or `input.<field>` references. Nested maps and nested arrays are rejected so predicate values stay predictable.

## Paths and ordering

`where`, `projection`, and `orderBy` accept simple dotted paths such as `account.status`. Each segment must be a Java identifier.

The connector does not accept aliases, functions, bracket syntax, collection joins, arbitrary HQL fragments, JPQL strings, or named queries in this slice.

Use `orderBy` plus `limit: 1` when the query should select the deterministic first row from multiple matches:

```yaml
orderBy:
  updatedAt: "desc"
limit: 1
```

Without `limit: 1`, the connector reads at most two rows and fails if more than one row matches. Zero rows also fail. That keeps `result: "single"` deterministic.

## Capture and replay

During managed TPF execution, the generated query step computes a capture key from tenant id, execution id, step index, query id, query version, and the selected `capture.keyFields` from the input. The first read result is stored and later retries of the same execution reuse the captured output instead of rereading mutable database state.

If `capture.keyFields` is omitted, the full query input becomes the key input. Prefer explicit key fields when the input contains non-decision metadata.

The current store is the in-memory query capture store. That proves the framework semantics and local replay behavior; durable query capture stores are the production hardening path for restart-safe replay.

## Relationship to persistence

The persistence plugin is write-side infrastructure: it observes pipeline values and stores business outputs for audit, APIs, reports, or UIs.

The JPA query connector is read-side infrastructure: it turns a decision-affecting database read into an explicit captured input for the next business step.

Both can use the same datasource, ORM configuration, and JPA entities in a Quarkus application, but they are separate features. The query connector is not bundled into the persistence plugin and does not require application-supplied connector code.

## Runtime boundaries

The public query connector/store contracts use JDK `CompletionStage` for unary boundaries. The Quarkus JPA connector uses Hibernate Reactive internally to run the read-only query, but application step code does not depend on Mutiny.

## Current limits

- `connector: "jpa"` is the only first-party query connector.
- `cardinality: "ONE_TO_ONE"` and `result: "single"` are required.
- Java record projection is the supported output shape.
- Predicates are `AND` only; no `OR` groups.
- No JPQL, named queries, aggregates, pagination, optional results, list results, or app-supplied query connector SPI.
- Simple dotted paths are accepted syntactically; invalid JPA paths fail deterministically when the connector executes the query.

For the architectural rationale behind captured query steps, see [I/O Shell Absorption](/evolve/io-shell-absorption#captured-query-steps-for-dbapi-reads).
