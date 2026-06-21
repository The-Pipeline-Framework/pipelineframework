# JPA Query Predicates and Selection

The JPA query connector intentionally supports a bounded predicate set rather than arbitrary JPQL. That keeps query reads declarative, schema-checkable, and safe to generate.

## Equality shorthand

`where` supports the equality shorthand:

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

## Operators

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

## Predicate values

The left side of each `where` entry is an entity property path:

```yaml
where:
  customerId: "input.customerId"
```

In that example, `customerId` means `CustomerRiskEntity.customerId`.

The right side is a predicate value. It can be either:

- a literal value, such as `"ACTIVE"`, `80`, or `true`
- an `input.<field>` reference, such as `"input.customerId"` or `"input.minimumScore"`

`input` is the query input record declared on the query:

```yaml
queries:
  latest-active-risk:
    input: "com.example.CustomerRiskLookup"
```

For:

```java
public record CustomerRiskLookup(String customerId, int minimumScore) {
}
```

`"input.customerId"` binds the `customerId` record component, and `"input.minimumScore"` binds the `minimumScore` record component.

This is a TPF query-connector convention, not JPA syntax. The connector uses it to keep YAML declarative and then renders safe JPA parameters internally. The generated HQL uses connector-owned parameter names such as `:p0`; user YAML does not provide JPQL parameter names, aliases, or expressions.

No other placeholder roots are supported in this slice. Values cannot reference `env`, `config`, `step`, `tenant`, previous outputs, or arbitrary expression functions. Add those values to the query input record when they are part of the decision read.

## Paths and projection

`where`, `projection`, and `orderBy` accept simple dotted paths such as `account.status`. Each segment must be a Java identifier.

The connector does not accept aliases, functions, bracket syntax, collection joins, arbitrary HQL fragments, JPQL strings, or named queries in this slice.

Projection maps output record component names to entity property paths:

```yaml
projection:
  customerId: "customerId"
  riskBand: "riskBand"
  score: "score"
  accountStatus: "account.status"
```

## Deterministic first row

Use `orderBy` plus `limit: 1` when the query should select the deterministic first row from multiple matches:

```yaml
orderBy:
  updatedAt: "desc"
limit: 1
```

Without `limit: 1`, the connector reads at most two rows and fails if more than one row matches. Zero rows also fail. That keeps `result: "single"` deterministic.
