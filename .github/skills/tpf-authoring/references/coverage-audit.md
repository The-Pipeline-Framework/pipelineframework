# TPF Author Skill coverage audit

This matrix was written before revising the Skill. It compares the PR #690 Skill at
`f817e1a9` with current `origin/main` at `4c7e59b1`, current documentation/source/tests,
representative applications, Repowise archaeology, and the retired app-generator.
It is an audit record, not a second authoring guide.

| Topic | Current Skill coverage | Current source of truth | Skill action | Notes |
| --- | --- | --- | --- | --- |
| Functional core / framework-owned shell | strong | `docs/design/fcis.md`, `docs/design/application-structure.md` | core | Preserve the opening prior and small-service rule. |
| Carry already-known data | strong | `docs/design/state-model.md`, data-architecture docs, typed examples | core | Keep the concrete “carry it” rule. |
| Canonical records | partial | `docs/develop/pipeline-template-dsl.md`, v3 loader/generator tests | reference | The Skill named canonical types but did not explain the generated semantic model. |
| Nominal wrappers and aliases | absent | `docs/develop/pipeline-template-dsl.md`, canonical-domain readiness/tests | reference | Explain identity versus transparent aliasing only as a modeling decision. |
| Discriminated unions and `accepts` | strong | template DSL, branching planner/runtime tests | core + reference | Preserve compiler-known routing; add whole-union versus narrowed-variant applicability. |
| Repeated fields | strong | template DSL, repeated-field generator/compatibility tests | core | Preserve value-shape-not-cardinality rule. |
| Cardinality choice | partial | `docs/develop/code-a-step.md`, `docs/develop/pipeline-step.md`, renderer tests | core + reference | Add the four shapes and choose topology before Java implementation. |
| Reactive service shapes | absent | `code-a-step.md`, API interfaces, generated invocation tests | reference | Teach shape selection and backpressure, not an interface catalogue. |
| Blocking service shapes | absent | `code-a-step.md`, `pipeline-step.md`, blocking bridge tests | reference | Distinguish materialized lists, incremental iterators, and generated offload. |
| Ordinary services versus operators | absent | operator docs, delegation reference, compiler resolution tests | core + reference | Operators are reusable/delegated execution units, not a synonym for any step. |
| Operator mapping and generation | absent | operator delegation/service-contract docs, mapper inference tests | reference | Cover build-time selection, explicit conversion boundaries, and generated adapters. |
| Operator pass-through / union applicability | absent | branch applicability/aspect tests, operator renderer tests | reference | Routing stays compiler-known; do not hide branch dispatch in operator code. |
| Stream/backpressure semantics | absent | `docs/design/execution-safety.md`, concurrency guide, service contracts | reference | Cardinality is a flow contract; list-returning blocking forms materialize. |
| Nested pipelines as steps | partial | template DSL, `docs/develop/pipeline-step.md`, local invocation tests | core + reference | Preserve composition prior and explain one execution/typed call semantics. |
| Direct self-recursion and limits | absent | template DSL, recursion validation/runtime tests | reference | Current limits are release details; route agents to compiler/tests. |
| Query | strong | Query docs, `QueryStepSupport`, provider/capture tests | core + reference | Preserve new-observation decision. |
| Command | strong | command docs, `CommandStepSupport`, effect-store tests | core + reference | Preserve logical effect identity versus attempt identity. |
| Await | strong | Await docs, descriptors/coordinator/admission/resume tests | core + reference | Preserve durable suspension authority and trusted-context rule. |
| Connector bindings | partial | command connector docs, connector-provider manifests/tests | reference | Add provider config versus operation config versus typed invocation input. |
| Connector SPI for extension authors | partial | connector provider SPI/source, provider packaging tests | reference | Teach when a reusable boundary earns an SPI; avoid app-owned factories/descriptors. |
| Object Ingest | partial | `docs/design/object-ingest.md`, connector runtime/tests | reference | Keep connector-owned admission and canonical `PayloadReference`. |
| Grouped / multi-file ingest | absent | object-ingest grouped-selection docs and projection tests | reference | Canonical bundle/repeated field is one value; it does not imply streaming cardinality. |
| Object Publish | partial | object-ingest publish docs, object-publish runtime/tests | reference | Publication is a connector-owned terminal boundary, not a service client call. |
| `PayloadReference` | strong | materialization/object-ingest docs, runtime-core contract/codecs/tests | core + reference | Preserve as canonical durable large-content representation. |
| Materialization and local file/media forms | strong | materialization docs, file representation provider/runtime tests | reference | Keep `Path`/bytes/stream as bounded local forms. |
| Representation providers | partial | representation-provider API, preparation/generation phases, provider tests | core + reference | Expand the search prior: inspect provider support before adding glue or changing types. |
| Canonical versus authored Java representation | absent | template DSL Java-binding section, generated-domain binding and mapper tests | core + reference | Add the explicit non-identity model. `java:` binds execution types; it is not durable/wire identity. |
| Persistence representation/mapping | partial | template DSL mappings section, persistence processor/plugin tests | reference | Keep persistence mapping distinct from canonical and pipeline execution values. |
| Wire representation and compatibility identity | absent | template DSL wire-identity section, IDL state/adapter tests | reference | Mappings do not define protobuf/wire identity; transport adapters own conversion. |
| Deployment runtime mapping (`pipeline.runtime.yaml`) | partial | runtime-layout docs, mapping loader/resolver tests | core + reference | Disambiguate logical placement mapping from type representation `mappings:`. |
| Aspects | partial | persistence/plugin docs, aspect generation/applicability tests | reference | Aspects observe applicable typed boundaries; they are not hidden business steps. |
| Persistence/history | strong | persistence docs and foundational plugin tests | core + reference | Preserve orthogonal business-history authority. |
| Generic cache | strong | cache docs/managers and target-safe replay tests | core + reference | Preserve result replay distinction and exclude effect/suspension meaning. |
| Query capture | strong | JPA Query capture docs, capture codec/store tests | core + reference | Preserve observation replay distinction. |
| Command effects | strong | command effect store/runtime tests | core + reference | Preserve effect authority and provider idempotency caveat. |
| Await and execution storage | strong | Await/execution state contracts and recovery tests | reference | Preserve distinct authorities even when one database hosts them. |
| Expected failure, item rejection, retry and DLQ | partial | `code-a-step.md`, execution safety and error-handling docs | reference | Add the decision: typed outcome vs item reject vs systemic execution failure. |
| Circuit admission and resilience | absent | `docs/design/execution-safety.md`, circuit operations/docs/tests | reference | Framework can protect only visible managed boundaries; hidden outbound I/O evades it. |
| Checkpoint / cross-pipeline handoff | absent | checkpoint-handoff docs, publication/admission tests | reference | Handoff transfers ownership; downstream retry/DLQ begins after admission. |
| Transport choice | partial | runtime-layout/configuration docs and transport renderer tests | reference | Keep `GRPC`, `REST`, `LOCAL` distinct from platform and wire protocol. |
| Runtime layout and placement | partial | runtime-layout docs, mapping resolver/generation tests | core + reference | Start simple; split only for a real isolation/scale/ownership need. |
| Build topology and deployment units | absent | runtime-layout/POM lifecycle docs and topology examples | core + reference | Runtime mapping does not rewrite Maven modules or artifact count. |
| Current single-unit packaging | absent | monolith layout docs, CSV Payments/Restaurant monolith POMs and tests | core + reference | One runnable unit is supported through a matching monolith build topology; exact scaffold mechanics remain current implementation detail. |
| Local versus remote constraints | partial | operator, transport, function, and canonical-readiness docs/tests | reference | Do not freeze transient support matrices; compile the intended topology. |
| Compiler diagnostics and validation | partial | compilation docs, processor validation tests | core + reference | Treat diagnostics as design feedback before writing glue. |
| Generated sources and metadata | absent | compilation/lifecycle docs, generation tests, `META-INF/pipeline/` contracts | reference | Inspect generated adapters/contracts; do not hand-author their responsibilities. |
| Testing patterns | absent | `docs/develop/testing.md`, example unit/compile/IT tests | reference | Unit-test pure logic; compile the YAML boundary; add topology tests only for used shapes. |
| Dependencies, bootstrap, build and package | absent | POM lifecycle docs, current examples, processor/provider classpaths | reference | Teach what to inspect, not a frozen POM template. |
| Extension versus application-owned code | partial | connector/provider/plugin docs and framework module boundaries | core + reference | A local policy stays local; a proven missing reusable semantic boundary is a framework gap. |
| Migration deletion heuristic | strong | current object-I/O/canonical migrations and git history | core | Preserve “new capability removes old responsibility.” |
| Semantic-owner-first framework change | absent | current ownership invariants plus verified migration history | core | Add: owner -> coherent change -> impact -> compatibility/tests. Churn controls rigor, not ownership. |
| Example interpretation | partial | CSV Payments, Search, Checkout, Restaurant, stdio/object fixtures | core + audit note | Examples prove compatibility and may retain residue; never promote one scaffold to doctrine. |
| Spring/Quarkus/runtime support status | strong as a caution | runtime-specific modules and smoke coverage | omit exact status | Keep only the rule to verify the current release and chosen runtime. |
| Provider catalogues, property keys and API inventories | intentionally absent | current docs/source | omit | These are volatile implementation/support details, not architectural priors. |

## Supplemental audit: `all-settings.md`

This configuration audit was added after the initial coverage pass and before its
findings were incorporated into the Skill.

| Topic | Current Skill coverage | Current source of truth | Skill action | Notes |
| --- | --- | --- | --- | --- |
| Configuration ownership and lifetime | partial | `docs/develop/configuration/all-settings.md`, config loaders/processors | core + reference | Add the decision split: canonical semantics, build-time generation, runtime framework policy, deployment/provider wiring, and typed invocation input. |
| Build-time versus runtime effect | partial | settings reference, annotation processor and Quarkus augmentation paths | reference | A runtime property cannot create a generated adapter, metadata field, or dependency that was absent from the build. Rebuild when the contract/generation input changes. |
| Generated client/boundary wiring | partial | settings reference, client metadata renderers/tests | reference | Explicit overrides tune generated boundaries; they do not belong in business types or an application routing registry. |
| Execution controls | partial | settings reference, execution safety/backpressure/runtime tests | reference | Parallelism, concurrency, retry, backpressure, circuit, kill-switch, and health are distinct shell policies, not step business logic. |
| Runtime provider selection | partial | settings reference plus cache/repository/persistence/provider loaders | reference | YAML declares the semantic use; runtime config selects/tunes storage/provider implementations without changing business steps. |
| Background orchestration / Await plumbing | partial | settings reference plus orchestrator/Await runtime docs/tests | reference | Runtime settings host execution and adapters; they do not redefine Command/Await/checkpoint semantics authored in YAML. |
| Telemetry lifecycle | absent | settings telemetry section, generated telemetry metadata and runtime/exporter tests | core + reference | Typed boundary metadata and included instrumentation are build-produced; enablement, sampling/exporters/backends and SLO values are runtime/deployment concerns. |
| Defaults versus exact step/boundary overrides | absent | settings defaults/per-step/circuit sections and generated diagnostics | reference | Prefer global/framework defaults; override an exact generated boundary only for a real operational exception, using emitted identity rather than guessing. |
| Kafka/SQS/S3/Postgres/Redis/provider keys | intentionally absent | current settings and provider docs | omit exact catalogue | Useful search evidence, but provider/property inventories are release-specific implementation details. |
