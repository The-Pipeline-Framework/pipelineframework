# JPA Query Connector Setup

Choose one provider where the generated query step runs.

For blocking Hibernate ORM/JPA:

```xml
<dependency>
    <groupId>org.pipelineframework</groupId>
    <artifactId>query-jpa-connector</artifactId>
    <version>${pipelineframework.version}</version>
</dependency>
```

For Hibernate Reactive:

```xml
<dependency>
    <groupId>org.pipelineframework</groupId>
    <artifactId>query-hibernate-reactive-connector</artifactId>
    <version>${pipelineframework.version}</version>
</dependency>
```

The blocking connector uses the application's JDBC datasource, Hibernate ORM configuration, and JPA
entities. The reactive connector uses the application's reactive datasource and Hibernate Reactive
`Mutiny.SessionFactory`; it does not require Hibernate ORM/JDBC or Panache. Both remain separate from
the [Persistence Plugin](/design/persistence), even when they use the same database.

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

This legacy top-level JPA form remains supported by `query-jpa-connector` and is blocking. It is
offloaded through framework blocking facilities.

For native provider execution, select the database behavior through a connector binding:

```yaml
connectors:
  database:
    provider: hibernate.reactive.query # use jpa.query for blocking ORM/JPA
    version: 1

steps:
  - name: Load Customer Risk Facts
    kind: query
    operation: find.one
    operationVersion: 1
    using: database
    config:
      entity: com.example.CustomerRiskEntity
      where:
        customerId:
          operator: eq
          values: [input.customerId]
        score:
          operator: gte
          values: [input.minimumScore]
      projection:
        customerId: customerId
        riskBand: riskBand
        score: score
      result: single
```

`jpa.query` and `hibernate.reactive.query` deliberately remain separate providers. There is no
runtime mode switch and the reactive provider does not implement the legacy automatic JPA path.

## Java records

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
