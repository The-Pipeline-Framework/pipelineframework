# The Pipeline Framework

The Pipeline Framework (TPF) is a Java framework for strongly typed application flows.
Keep the core pure, connect to reality.

Core modules:
- `framework/pom.xml`: Parent POM of the multi-module Maven project
- `framework/deployment`: compiler and code generation phases (Quarkus/canonical)
- `framework/runtime-core`: framework-neutral TPF abstractions
- `framework/runtime`: runtime APIs, execution engine, telemetry, config loading (Quarkus/canonical)
- `framework/runtime-spring`: runtime APIs, execution engine, telemetry, config loading (Spring Boot)
- `framework/api`: framework-neutral API contracts for generated pipeline applications

Plugins:
- `framework/plugins`: cross-cutting side-effect capabilities (persistence, caching, materialisation)

Connectors:
- `framework/connectors`: Admit or publish files, object-store entries, and external payloads, or provide
  replay-safe Query and Command boundaries for external observations and effects.

Blocks:
- `blocks`: Reusable, compile-time pipeline definitions distributed as ordinary dependencies. Applications own
  connector bindings and Command authority for any capabilities a Block requires.

Expansions:
- Versioned distribution packages that can group related Blocks, Connectors, types, configuration, examples,
  operational assets, and documentation. An Expansion does not create a new runtime step kind.

Supporting repo surfaces:

- `examples`: reference applications, topology smoke paths, and end-to-end compatibility surfaces
- `ai-sdk`: standalone Java SDK used for delegation/operator stress testing and mapper/transport exercises
- `docs`: VitePress documentation site
- `web-ui`: SvelteKit Canvas/web UI (unmaintained)

For planning, PR slicing, architecture tradeoffs, roadmap shaping, or docs IA strategy, read `AGENTS.planning.md`. For ordinary implementation work, use this file plus the smallest relevant local context.

Before making or reviewing an architectural change, read `docs/decisions/`. It is the
GitNexus-aligned catalogue of durable TPF semantic ownership and distinctions. Keep
it current in the same change whenever a PR adds a semantic capability, moves
responsibility between abstractions, changes an authority or identity, or invalidates a
recorded decision. Use one accepted/draft/deprecated ADR per coherent choice; add a
successor rather than erasing superseded rationale. Exact syntax and transient support
limits remain in current source, tests, and feature documentation.

## Canonical Terms

Load `AGENTS.glossary.md` when terminology, docs wording, transport/platform naming, architecture explanations, or public-facing copy matters.

Always keep these distinctions active:

- **Functional core / imperative shell**: business logic stays typed and transport-neutral; TPF owns generated adapters, connectors, await handling, persistence, caching, replay, telemetry, retries, and deployment/runtime integration.
- **Pipeline**: a strongly typed application flow, not a CI/CD pipeline, generic workflow diagram, or arbitrary orchestration graph.
- **Transport mode**: only `GRPC`, `REST`, and `LOCAL` as `pipeline.transport` values. `FUNCTION`, `HTTP_LAMBDA`, `PROTOBUF_HTTP_V1`, and `ENVELOPE_HTTP_V1` are separate platform/deployment/wire-protocol concepts.
- **Runtime layout vs build topology**: runtime layout is the logical runtime shape; build topology is the Maven/JAR/container structure that physically builds deployables.
- **Connector vs plugin**: connectors model typed I/O boundaries; plugins provide cross-cutting framework extensions such as persistence, caching, telemetry, or logging.
- **Block vs Expansion**: a Block is reusable compile-time pipeline composition. An Expansion is a
  versioned package of related Blocks, Connectors, and supporting assets; it is not `ONE_TO_MANY` cardinality.

## Runtime and Build Commands

This repository uses an isolated Maven local repository per worktree. Always include:

    -Dmaven.repo.local="$PWD/.m2/repository"

on every Maven invocation.

Use `install -DskipTests -Dgpg.skip` only when warming the worktree cache or publishing patched framework artifacts into that local repository.

Load `AGENTS.validation.md` before choosing validation commands for non-trivial changes.

Most common gates:

- Framework verify: `./mvnw -f framework/pom.xml verify`
- Root verify: `./mvnw verify`
- Docs build: `npm --prefix docs run build`

## Architecture Notes

Generated metadata under `META-INF/pipeline/` at build time:
- Pipeline order: `order.json`.
- Telemetry metadata: `telemetry.json`.
- Branching metadata: `branching.json`.
- Platform, transport, module, and plugin-host metadata: `platform.json`
- Deterministic semantic contract and ordered step descriptors (used by release validation and queue-async transition-worker validation): `pipeline-contract.json`

Runtime is reactive-first; blocking work must be explicitly offloaded.

### Deployment Patterns And Wire Protocols

- `HTTP_LAMBDA`: deployment/platform pattern implemented as `pipeline.transport=REST` with `pipeline.platform=FUNCTION`
- `PROTOBUF_HTTP_V1`: protobuf-over-HTTP wire/envelope protocol for remote HTTP step-host/operator boundaries
- `ENVELOPE_HTTP_V1`: loose-envelope HTTP wire protocol for remote HTTP step-host/operator boundaries

Transport and platform are orthogonal dimensions; avoid coupling operator category directly to transport decisions.

## Current Engineering Invariants

Compilation and contracts:

- YAML-driven compilation is the primary contract source. Annotations may mark services or compatibility paths, but YAML/model phases should own flow shape, step order, cardinality, transport/platform choices, operators, connectors, and semantic step kinds.
- Contract failures should surface at build time whenever possible: step resolution, operator method shape, mapper compatibility, cardinality/link compatibility, connector declarations, transport requirements, and generated artifact availability.
- Mapper inference and selection must remain pair-accurate (`Domain` + `External`) and deterministic in ambiguity diagnostics.
- gRPC-bound flows require descriptor availability and compatible bindings during generation/binding phases.
- Generated artifacts are part of the contract. Pipeline order, telemetry metadata, runtime descriptions, generated adapters, function handlers, and template-generator schema exports should not drift from the compiler model.

Runtime semantics:

- Transport mode, platform mode, deployment pattern, wire/envelope protocol, and worker invocation protocol must stay distinct. Do not treat `FUNCTION`, `HTTP_LAMBDA`, `PROTOBUF_HTTP_V1`, or `ENVELOPE_HTTP_V1` as peers of `GRPC`, `REST`, and `LOCAL`.
- FUNCTION and COMPUTE paths should preserve equivalent cardinality, mapper, rejection, failure, and lineage semantics unless a difference is explicitly documented and validated.
- Runtime layout and build topology are related but not interchangeable. Runtime mapping changes generated placement/calls; it does not automatically reshape Maven modules, POMs, or deployable packaging.
- Split/merge lineage IDs and ordering must be deterministic and replay-safe across runtime adapters, platform modes, and generated transports.
- Reactive execution is the default. Blocking work must be explicitly offloaded, documented by the relevant execution hint, and validated in the runtime path that uses it.

Boundaries and I/O shells:

- Business functions should stay focused on typed domain transformations. Persistence, transport, retries, correlation, polling, replay capture, and deployment wiring belong in the imperative shell.
- Connectors model I/O admission/publication and captured external reality. Plugins model cross-cutting side effects. Do not blur connector semantics into generic plugin behavior or hide external I/O in business steps when a connector/runtime primitive exists.
- Await boundaries must preserve durable wait state, correlation, completion admission, timeout, duplicate completion handling, and resume semantics. Transport adapters may vary; await semantics should not.
- Checkpoint handoff is a cross-pipeline ownership boundary. After admission, the downstream pipeline owns retry/DLQ and lifecycle semantics.
- Persistence, caching, materialization, execution state, await state, and checkpoint handoff are separate state surfaces. Do not substitute one for another without explicit design rationale.

Durability and storage:

- New TPF control-plane storage should prefer immutable internal records. For new Dynamo-backed coordinator stores, avoid `UpdateItem`/upsert semantics; prefer conditional writes, immutable records, and append-only event records.
- Existing execution/await stores are legacy exceptions until explicitly redesigned. Do not use their mutable patterns as precedent for new durable control-plane code.
- Idempotency keys, dispatch identifiers, checkpoint identifiers, and correlation identifiers must remain stable and replay-safe across retries and adapter boundaries.

Portability:

- Quarkus is the canonical production runtime today, but framework-neutral semantics should live in `runtime-core` when they are not inherently Quarkus-specific.
- Spring support is emerging and limited. Do not claim Spring parity unless the matching compiler/runtime path and smoke coverage exist.
- Renderer-specific code should adapt the shared model; it should not redefine TPF semantics independently for Quarkus, Spring, function providers, or template generation.

Coding guardrails:

- New code should not use `return null`; use `Optional`, empty collections, explicit result records, or exceptions. Existing legacy/null-heavy code is not a precedent for new work.
- Prefer explicit result types and immutable records for new internal state. Avoid hidden mutable globals, broad static utility accretion, and "God classes".
- New semantic step kinds (`kind: command`, query steps, object I/O, or future DSL-owned I/O shells) and orthogonal lifecycle modifiers such as `await:` must update compiler/runtime support, validation tests, user docs, telemetry/replay metadata, replay-viewer rendering/legend, and affected examples or generator paths together.
- When troubleshooting, provide a focused regression coverage along with the fix.

## Persistence Plugin Notes

Persistence provider selection is configured via:

- runtime config key: `persistence.provider.class`
- build-time processor option: `-Apersistence.provider.class=<fqcn>`

Keep both forms aligned in docs and processor behavior.

## Testing Conventions

Unit tests use `*Test` with Surefire. Integration tests use `*IT` with Failsafe. E2E tests using containers should run in `verify` unless there is an explicit reason otherwise.

## Docs Source of Truth

Canonical docs live under top-level route directories:

- Architectural decisions and rationale: `docs/decisions/`
- Architecture and concepts: `docs/architecture/`
- Implementation and usage: `docs/develop/`
- Runtime topology and deployment mechanics: `docs/deploy/`
- Observability and operations: `docs/operate/`
- Implementation internals, design notes, and backlog material: `docs/evolve/`
- Product/value framing: `docs/value/`

`docs/guide/**` files are redirect/noindex compatibility stubs only. Do not add real content there. Move or merge useful guide-stub content into the canonical top-level route.

Use links between these areas when a feature spans both implementation and app usage.

## Agent Working Rules for This Repo

This is a large multi-surface repository. Do not start tasks with broad recursive search.

Use GitNexus first for orientation and discovery before broad repo search, but treat it as an index, not authority.
Do not refresh or rebuild GitNexus automatically unless explicitly requested.
GitNexus may point at a canonical indexed checkout, not the active worktree.
Verify conclusions against source before editing.
GitNexus may return sparse retrieval for some questions; treat outputs as a pointer, not an answer.
Use its retrieval tools and synthesize answers yourself.

### Change The Semantic Owner Boldly

Do not treat dependency count, churn, or centrality as reasons to avoid the abstraction
that semantically owns a concept. TPF is actively evolving; high-impact types and runtime
seams are often the correct place for a cross-cutting change.

Impact analysis identifies compatibility obligations and required tests. It must not
force a sibling helper, facade, codec, adapter, or parallel execution path merely to
minimise touched files. Before creating one, ask:

1. Which existing abstraction semantically owns the concept?
2. Would adding the capability there make the model more coherent?
3. Is the sidecar only avoiding migration or test work?
4. Will it leave two representations or execution paths for one concept?

Prefer changing the owning abstraction cleanly, including a breaking change when
appropriate, over preserving accidental structure through additive compatibility
layers. Additive change is not automatically safer. Prefer simplification, semantic
consolidation, migration, and deletion; preserve compatibility when an actual
compatibility promise exists.

For planning, PR slicing, roadmap, or architecture tradeoff work, load `AGENTS.planning.md`. For implementation work, read only the source files needed for the current decision and run the smallest validation command that proves the claim.

TPF-specific scoping rules:

- Core semantics live under `framework/api`, `framework/runtime`, `framework/runtime-*`, and `framework/deployment`.
- Runtime integrations should stay scoped:
  - Spring work: `core + spring`, not Quarkus unless parity is claimed.
  - Quarkus work: `core + quarkus`, not Spring unless parity is claimed.
- Examples are compatibility surfaces, not disposable demos.
- Docs should be updated with semantic changes, but do not scan all docs unless the affected concept is unclear.
- Replay/web-ui is relevant when execution semantics, telemetry, step lifecycle, or visual replay state changes.
- When changing Await/replay lifecycle semantics or refreshing the CSV Payments built-in replay, update the canonical dataset, its docs copy and analysis sidecar, the canonical replay docs, and homepage replay-video assets together. Read `tools/replay-viewer/README.md` before regenerating; validate the live-path event invariants and regenerate `tools/homepage-replay-video` outputs.
- `app-generator` is separate; only involve it when template generation, schema export, scaffold generation, or
  generated project behavior changes.
- Treat `examples/` and `ai-sdk/` as compatibility/reference surfaces, not disposable demos, when framework semantics change.
- Keep user-facing docs (`architecture`/`develop`/`deploy`/`operate`/`value`) free of internal planning terminology unless the topic is explicitly implementation-internal (`docs/evolve/`).
- Prefer enriching existing canonical docs pages over introducing standalone “feature islands” that duplicate navigation.
- Do not add “audience declaration” sections in user-facing docs. Make docs audience-fit by placing content in the right canonical docs area:
  - `design`: architecture, concepts, and user-facing design rationale
  - `develop`: implementation and usage
  - `deploy`: runtime topology and deployment mechanics
  - `operate`: observability and response
  - `evolve`: internals, design notes, and backlog-oriented material
- Keep risk registers, update reports, and future-work tracking out of user-facing docs unless they are actionable operator runbooks; place backlog/planning artifacts under `docs/evolve/` or external issue trackers.
- When changing operator or mapper semantics, update code + tests + docs together in the same change set.
- When adding or changing a semantic step kind (`kind: command`, query steps, object I/O, or future DSL-owned I/O shells), or changing an orthogonal lifecycle modifier such as `await:`, update compiler/runtime support, validation tests, user docs, telemetry/replay metadata, replay-viewer node rendering/legend, and any affected example replay datasets or generation paths in the same change set.
- Do not write procedural code that leads to "God classes" e.g. with 'static' methods.
- Use available Java FP patterns and language features whenever possible
- Do not `return null` or pass null values as parameters (use Optional<> instead)

## Git Safety

- Do not perform destructive git operations unless explicitly requested.
- Do not commit or push unless explicitly requested.
- If unexpected unrelated working-tree changes appear mid-task, stop and ask.

## Maven discipline

- Maven profiles must not be used in this repository (except when forced by dependencies and there is no
  alternative)
- Do not replace profiles with Maven properties that select source universes.
- Do not replace profiles with environment variables that select source universes.
- Do not hide dead Java files using compiler <excludes>.
- Do not make CI reconstruct a special build using -P....
- Do not make release commands reconstruct a special build.
- Do not make IntelliJ IDEA require profile selection.
- There must be one canonical Maven reactor/lifecycle.

## Token Discipline

Prefer GitNexus context over broad grep, but do not call every graph tool by default. Keep routine implementation context small; load the planning supplement only when the task is actually planning-shaped.

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **pipelineframework** (54434 symbols, 135286 relationships, 592 execution flows).

> Index stale? Run `node .gitnexus/run.cjs analyze --index-only` from the project root — it auto-selects an available runner. No `.gitnexus/run.cjs` yet? Bootstrap with `npx`, `bunx`, or `pnpm dlx` — e.g. `bunx gitnexus@latest analyze` (npm 11 npx crash; #1939).

## Always Do

- **MUST run impact before editing.** Use `impact({target: "symbolName", direction: "upstream"})` or `node .gitnexus/run.cjs impact "symbolName" --direction upstream --repo .`; report callers, processes, and risk. Never substitute grep for graph analysis.
- **MUST analyze graph changes before committing.** Use `detect_changes({scope: "all"})` (MCP) or `node .gitnexus/run.cjs detect-changes --scope all --repo .` (CLI fallback). `partial: true` or `truncated: true` is not a clean check — a zero means unseen, not unaffected; re-run it. For regression review: `detect_changes({scope: "compare", base_ref: "main"})` or `node .gitnexus/run.cjs detect-changes --scope compare --base-ref "main" --repo .`.
- MUST warn on HIGH/CRITICAL `risk` pre-edit; never use `riskSharedAxes` to waive a HIGH/CRITICAL `risk` warning. Compare File/symbol: MCP File omits axes; Graph-RAG expands File.
- **MUST treat `risk: UNKNOWN` as unresolved, not as low.** An empty caller set is not evidence the symbol is unused — it can also mean the callers are not resolvable by the index (plain-object property access, dynamic dispatch, cross-language calls). `impact` pairs `UNKNOWN` with a `riskNote` saying so. Confirm with a text search before treating the symbol as safe to change or delete; do not proceed on the strength of a zero.
- **MUST use `query({search_query: "concept"})` for concepts/flows, `context({name: "symbolName"})` for a named symbol, or `impact` for blast radius, on read-only callers, dependencies, imports, or execution flow.** Graph first; text search only for empty/`UNKNOWN`/literals.
- For security review, `explain({target: "fileOrSymbol"})` lists taint findings (source→sink flows; needs `analyze --pdg`).

## Never Do

- NEVER edit a function, class, or method before MCP/CLI impact analysis.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis, and never read `UNKNOWN` as an all-clear — it means the walk could not answer, which is the one verdict that requires confirming by other means.
- NEVER rename symbols with find-and-replace — use `rename` which understands the call graph.
- NEVER commit before MCP/CLI graph change analysis.

## Resources

| Resource | Use for |
| --- | --- |
| `gitnexus://repo/pipelineframework/context` | Codebase overview, check index freshness |
| `gitnexus://repo/pipelineframework/clusters` | All functional areas |
| `gitnexus://repo/pipelineframework/processes` | All execution flows |
| `gitnexus://repo/pipelineframework/process/{name}` | Step-by-step execution trace |

## CLI

| Task | Read this skill file |
| --- | --- |
| Understand architecture / "How does X work?" | `.agents/skills/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.agents/skills/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.agents/skills/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.agents/skills/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.agents/skills/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.agents/skills/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->
