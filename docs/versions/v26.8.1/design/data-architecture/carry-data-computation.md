---
search: false
---

# Data coupling is not inherently bad

There is an important implication here.

**Data coupling exists.**

If X needs something produced by B, X is coupled to that information whether the architecture admits it or not.

The question is where that coupling lives.

It can be hidden:

```text
X
 ↓
repository.find(...)
```

or explicit:

```text
B → ... → X
      ↑
 typed data dependency
```

TPF deliberately favors the latter.

The type flowing through the pipeline tells the truth about what the computation knows and what downstream computation requires.

That is not accidental coupling.

It is **declared coupling**.

## Carry what the computation already knows

Suppose step B discovers a fact that step X eventually needs.

A conventional imperative implementation often carries only an identifier and reconstructs the rest later:

```text
B produces CustomerAssessment
        ↓
carry customerId
        ↓
...
        ↓
X queries repository
        ↓
reconstruct CustomerAssessment
```

The code at X may look pleasantly small:

```java
repository.findByCustomerId(id);
```

but the simplicity is deceptive.

X actually depends on:

```text
database availability
current database state
repository semantics
transaction boundaries
network latency
schema compatibility
whatever may have modified the record since B
```

None of that dependency appears in X's declared input.

TPF prefers the more literal alternative:

```text
B produces CustomerAssessment
        ↓
carry CustomerAssessment
        ↓
reshape
        ↓
reshape
        ↓
X consumes CustomerAssessment
```

If the computation already knows something, **why throw that knowledge away only to retrieve it again later?**

Carry it.
