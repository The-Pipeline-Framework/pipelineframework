# State and replay authorities

Read this reference only for persistence, cache, capture, effects, Await/execution state, or replay work. Verify exact configuration and behavior in current docs/source/tests.

Keep each authority distinct:

| Mechanism | What it owns |
| --- | --- |
| typed pipeline state | facts known by this computation |
| persistence aspect | durable business values/history |
| generic cache | versioned pipeline/step result reuse |
| Query capture | replay of external observations |
| CommandEffectStore | logical effect identity and recorded outcome |
| Await storage | suspended interaction and completion/resume state |
| execution state | runtime progress, dispatch, retry, and lifecycle |

One database may host several authorities; shared storage does not merge them.

TPF persistence is orthogonal observation of typed pipeline boundaries.

Default rules:
- Business steps do not persist themselves.
- Use persistence aspects to observe meaningful boundary values.
- Persist the canonical root value actually seen at that boundary.
- Use mappings.persistence to adapt it to the configured persistence provider.
- JPA entities/mappers are representation plumbing and may be handwritten.
- Nested canonical types do not automatically imply independent tables.
- Persistence tables are queryable business history, not runtime effect authority.
- Do not introduce an application registry, generic JSON event journal, or custom
  persistence provider merely to avoid defining typed representations.
- If generating those representations is repetitive, report an ergonomics gap;
  do not solve the ergonomics problem by changing the persistence architecture.

When a canonical pipeline type needs persistence, file, CSV, API, or another external representation, use the existing TPF representation/mapping mechanism. Do not assume that an external representation is a trivial copy of the canonical record.
Before generating a mapper, inspect:
- representation-provider requirements;
- existing external classes and annotations;
- identity/lifecycle semantics;
- converters;
- omitted or derived fields;
- nested/repeated values;
- whether mapping must work in both directions;
- whether another representation intentionally shares the same external model.
Generate boring structural mapping where it is genuinely structural, but preserve application-owned representation semantics rather than inventing them.

A cache hit can replay a result without proving a live external observation or effect occurred. Persistence can record business history without authorizing an effect. Query capture cannot replace Command idempotency. Await storage cannot replace execution state or business persistence.

Do not create a mutable application registry that tries to remember what the pipeline knew, observed, executed, should retry, or awaits. Search the current persistence/cache/capture/effect/Await docs, managers/support classes, and replay/duplicate/failure tests before adding state.
