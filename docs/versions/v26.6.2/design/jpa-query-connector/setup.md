---
search: false
---

# JPA Query Connector Setup

Add the connector where the generated query step runs:

```xml
<dependency>
    <groupId>org.pipelineframework</groupId>
    <artifactId>query-jpa-connector</artifactId>
    <version>${pipelineframework.version}</version>
</dependency>
```

The connector uses the application's Quarkus datasource, ORM configuration, and JPA entities. It remains separate from the [Persistence Plugin](/versions/v26.6.2/design/persistence), even when both features use the same database.

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
