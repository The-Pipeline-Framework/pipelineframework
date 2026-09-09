# Glossary

Use these definitions when reading or writing about TPF. They keep domain contracts, runtime
mechanics, boundary semantics, and packaging concepts distinct. The same JSON source drives the
inline term tooltips throughout the current documentation.

```mermaid
flowchart LR
    C[Functional core] --> P[Pipeline contract]
    P --> S[Imperative shell]
    S --> E[External reality]
    B[Blocks] --> P
    X[Expansions] --> B
    X --> S
```

<GlossaryIndex />

For architectural context, continue with [Functional Core, Imperative Shell](/architecture/fcis).
