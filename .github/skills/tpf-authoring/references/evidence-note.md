# TPF authoring Skill evidence note

This note records the evidence used to synthesize `SKILL.md`. It is intentionally not a second guide.

## Repository areas inspected

- Architecture and data doctrine: `docs/design/application-structure.md`, `docs/design/state-model.md`, and the current `docs/design/data-architecture/` pages, especially pipeline knowledge, carrying known data, Query meaning, effects authority, and large-data references.
- Authoring and type model: `docs/develop/pipeline-template-dsl.md`, `docs/develop/code-a-step.md`, operator/connector authoring docs, and their v3 loader, schema, IDL, routing, repeated-field, union, and local-pipeline tests under `framework/runtime` and `framework/deployment`.
- Query, Command, and Await: runtime/deployment docs; `QueryStepSupport`, Query capture tests; `CommandStepSupport`, `CommandEffectStore`, command outcome tests; Await coordinator/admission/continuation code and duplicate, correlation, timeout, projection, and resume tests.
- Orthogonal state: persistence and cache design/docs plus foundational plugin managers/tests; JPA Query capture docs/tests; command effect and Await storage contracts.
- Large/object data: materialization and object-ingest docs; `PayloadReference`, `PayloadMaterializer`, `MaterializedPayload`, connector payload origin, Object Source/Publish operations, representation-provider SPI, generated file facade, and focused tests.
- Composition and placement: branch routing, v3 flow compatibility, nested/local pipeline composition, runtime-mapping docs, generated deployment phases, and transport/platform configuration.
- Examples and history: current pipeline YAML and authored services across CSV Payments, Search, Restaurant, object/stdio, Query/Command/Await fixtures, plus targeted git history for the same surfaces. CSV Payments was treated as a compatibility surface that includes both current native paths and historical/application-specific residue—not as universal doctrine.

The current working tree also contained uncommitted data-architecture documentation. Its arguments were cross-checked against current runtime/compiler source and tests before inclusion; this note does not imply those edits were authored by this task.

## Repowise decisions that were useful as leads

All 34 decisions returned by `repowise decision list --status all` were still `proposed`; none was accepted as authority by status alone.

- `817224fe` — “Keep LLM adapter tuning out of portable connector bindings.” Useful lead for separating portable Query semantics from adapter tuning. Retained only at the broader, source-backed level: portable semantics and provider/runtime tuning remain separate.
- `460dd001`, `7e4bb654`, `5a4d88c2`, and `4c176833` — v3 rejects legacy/wire metadata, compiler-owned IDL state/tags, and the canonical template DSL. Useful leads for compiler ownership. Exact migration and tag-allocation details were omitted as implementation-specific and high-staleness.
- `11ebf7f6` — CSV Payments object-I/O migration. Useful lead for checking whether `PayloadReference`, Object Ingest/Publish, and representation providers actually replace application file/storage responsibility. Current docs/source/tests verified the general rule.
- `7a8af571`, `0a47ca5c`, and `cbd4ce89` — live Await, explicit admission/continuation boundaries, and immutable queue-async control-plane records. Useful leads for durable admission, duplicate suppression, and separation of Await/execution state. Only transport-neutral Await semantics were retained.

## Decisions rejected from the Skill

- PR-slicing/refactor proposals such as extracting queue-async submission, read-model, operations, or renderer classes were rejected as internal implementation structure.
- CI caching, Testcontainers, smoke-template placement, and virtual-thread smoke details were rejected as operational/transient.
- `fa14d403` renderer splitting was rejected as a current deployment implementation choice, not an application-authoring prior.
- Low-confidence inferred decisions such as `f9bd2c20` (a CSV Payments sealed superclass) and `7c88f140` (specific replay prerequisites) were rejected as example-specific or inferred.
- Exact current support limits—provider catalogues, nested-pipeline recursion/cardinality restrictions, Query capture modes, failed-command redrive behavior, Spring parity, and representation consumers—were deliberately not promoted to doctrine. The Skill tells agents to inspect the current release.
- The immutable-control-plane proposals were not generalized to claim that every existing TPF store is append-only; current execution/Await stores include legacy mutable patterns.

## Genuinely unresolved questions

- Which retry/redrive entrypoint shapes will become uniformly available across Command providers and runtime layouts remains release-specific.
- The long-term boundary between generic representation mappings and consumer-specific generated adapters is still evolving; current consumers have different readiness rules.
- Some examples still carry historical Java types, mappers, logging, or file-path compatibility code while newer canonical v3/object-I/O paths exist. Whether each residue should be removed is a migration decision, not doctrine.
- Spring support and parity with canonical Quarkus compiler/runtime paths remain implementation-dependent and must be checked per capability.
