# The Pipeline Framework

The Pipeline Framework (TPF) is a Java framework for strongly typed application flows.
Keep the core pure, connect to reality.

Core modules:
- `framework/pom.xml`: Parent POM of the mult-module Maven project
- `framework/deployment`: compiler and code generation phases (Quarkus/canonical)
- `framework/runtime-core`: framework-neutral TPF abstractions
- `framework/runtime`: runtime APIs, execution engine, telemetry, config loading (Quarkus/canonical)
- `framework/runtime-spring`: runtime APIs, execution engine, telemetry, config loading (Springboot))
- `framework/api`: framework-neutral API contracts for generated pipeline applications

Plugins:
- `framework/plugins`: cross-cutting side-effect capabilities (persistence, caching, materialisation)

Connectors:
- `framework/connectors`: Admit files, object-store entries, or external payloads (Object ingest/publish). Or record 
  and replay-safe external effects such as indexing, tickets, emails, or provisioning (Query JPA).

Supporting repo surfaces:

- `examples`: reference applications, topology smoke paths, and end-to-end compatibility surfaces
- `ai-sdk`: standalone Java SDK used for delegation/operator stress testing and mapper/transport exercises
- `docs`: VitePress documentation site
- `web-ui`: SvelteKit Canvas/web UI (unmaintained)

tpf-mcp-bridge lives in a separate repo now. It holds the MCP bridge and the template generator.

For planning, PR slicing, architecture tradeoffs, roadmap shaping, or docs IA strategy, read `AGENTS.planning.md`. For ordinary implementation work, use this file plus the smallest relevant local context.

Before making or reviewing an architectural change, read `docs/decisions/`. It is the
Repowise-compatible catalogue of durable TPF semantic ownership and distinctions. Keep
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
- New semantic step kinds (`kind: await`, `kind: command`, query steps, object I/O, or future DSL-owned I/O shells) must update compiler/runtime support, validation tests, user docs, telemetry/replay metadata, replay-viewer rendering/legend, and affected examples or generator paths together.
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
- Architecture and concepts: `docs/design/`
- Implementation and usage: `docs/develop/`
- Runtime topology and deployment mechanics: `docs/deploy/`
- Observability and operations: `docs/operate/`
- Implementation internals, design notes, and backlog material: `docs/evolve/`
- Product/value framing: `docs/value/`

`docs/guide/**` files are redirect/noindex compatibility stubs only. Do not add real content there. Move or merge useful guide-stub content into the canonical top-level route.

Use links between these areas when a feature spans both implementation and app usage.

## Agent Working Rules for This Repo

This is a large multi-surface repository. Do not start tasks with broad recursive search.

Use Repowise first for orientation and discovery before broad repo search, but treat it as an index, not authority.
Do not refresh or rebuild Repowise automatically unless explicitly requested.
Repowise may point at a canonical indexed checkout, not the active worktree.
Verify conclusions against source before editing.

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
- Keep user-facing docs (`design`/`develop`/`deploy`/`operate`/`value`) free of internal planning terminology unless the topic is explicitly implementation-internal (`docs/evolve/`).
- Prefer enriching existing canonical docs pages over introducing standalone “feature islands” that duplicate navigation.
- Do not add “audience declaration” sections in user-facing docs. Make docs audience-fit by placing content in the right canonical docs area:
  - `design`: architecture, concepts, and user-facing design rationale
  - `develop`: implementation and usage
  - `deploy`: runtime topology and deployment mechanics
  - `operate`: observability and response
  - `evolve`: internals, design notes, and backlog-oriented material
- Keep risk registers, update reports, and future-work tracking out of user-facing docs unless they are actionable operator runbooks; place backlog/planning artifacts under `docs/evolve/` or external issue trackers.
- When changing operator or mapper semantics, update code + tests + docs together in the same change set.
- When adding or changing a semantic step kind (`kind: await`, `kind: command`, query steps, object I/O, or future DSL-owned I/O shells), update compiler/runtime support, validation tests, user docs, telemetry/replay metadata, replay-viewer node rendering/legend, and any affected example replay datasets or generation paths in the same change set.
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

Prefer Repowise MCP context over broad grep, but do not call every Repowise tool by default. Keep routine implementation context small; load the planning supplement only when the task is actually planning-shaped.

<!-- REPOWISE_AGENTS:START — Do not edit below this line. Auto-generated by Repowise. -->
## Codebase Intelligence for pipelineframework (Repowise)

Indexed by [Repowise](https://repowise.dev). Last indexed: 2026-08-26 (commit 9c29853ba). Confidence: 100%.
### How to work in this repo

- **Trust the index for orientation.** `verified: true` means the indexed bytes were checked against the live tree, but the index is not edit authority: read the corresponding content from the active worktree immediately before editing it. For orientation and other read-only work, re-read indexed content on `bounds: "approximate"`, `_meta.stale_warning`, `search_method: "bm25"` or `confidence: "low"`; `index_behind: true` alone is informational.
- **Pre-edit, not instead-of-edit.** These tools decide *which* files to read and edit. Reading a file before you edit it is correct and expected.
- **Noisy commands** (tests, builds, `git log`/`diff`, searches, listings): prefer `repowise distill <cmd>`, the same command with its exit code preserved and errors-first output. A `[repowise#<ref>: N lines omitted]` marker is recoverable via `repowise expand <ref>` (add `-q <regex>` to filter); never re-run the command to see omitted output.
- **Recording a decision** you had to reason out: `repowise decision add --title T --decision D` records it without prompting and prints the id (`--format json` to parse it back). It lands `proposed`, for a person to confirm.

### Tools

| Tool | When and why |
|------|--------------|
| `get_answer(question)` | First call for any how/where/why question. Cite `confidence: "high"` or `grounding: "extracted"` directly; `degraded` means judge by `retrieval_quality`. `symbol_bodies` has live bodies. |
| `get_context(targets=[...])` | Triage card for files/modules/symbols: docs, signatures, hotspot, fix history. No source bytes — `include=["skeleton"]` for the whole file verified, `["callers"]` or `["decisions"]` for depth. Batch targets. |
| `get_symbol(id)` | **Follow-up, not an entry point** — one verified body for an id a prior response named (`path.py::Name`, `path.py:140-180`, `repowise#<hex>`). Never walk a file symbol by symbol; Read it. |
| `search_codebase(query)` | Hybrid search, auto-routed by query shape; force with `mode=symbol`, `path`, `concept`, or `hybrid`. A hit whose `sources` are `[fts]` only has no semantic agreement, so verify it. |
| `get_why(query, targets?)` | Why the code is shaped this way: decision records, git archaeology, rationale comments. Call before a refactor or a pattern divergence. |
| `get_risk(targets, changed_files?)` | What history says about touching these files. PR mode (`changed_files`) leads with a `directive`: read `will_break` / `missing_cochanges` / `missing_tests` / `tests_to_run` first. |
| `get_change_risk(revspec, extensions?, exclude_patterns?)` | Defect score for a whole commit or `base..head` range, from its diff on the live checkout. Lead with `risk_percentile`. Scores a range; `get_risk` scores paths. |
| `get_health(targets?, include?)` | Defect / maintainability / performance scores and findings. Self-check the files you touched before finishing. |
| `get_dead_code()` | Confidence-tiered unreachable files / unused exports / zombie packages. For cleanup sweeps, not targeted fixes. |
| `get_overview()` | Architecture map. Call once, first, in an unfamiliar repo; skip it after that. |

### Architecture
pipelineframework consumes typed Java operators, YAML pipeline definitions, and optional protobuf/connector configuration; build-time processors validate and compose the graph, generate contracts and adapters, and the runtime executes workflows with plugins, producing deployable service/function entry points, protocol artifacts, and operational UI/documentation surfaces.

### Key modules
- `framework/deployment/src/main/java/org/pipelineframework/processor` — PipelineStepProcessor presents the processor as a facade over an ordered PipelineCompilationPhase pipeline
- `framework/runtime/src/main/java/org/pipelineframework/orchestrator` — Async queue-mode orchestration owns the transition from submitted pipeline inputs and build-time contracts to durable execution records…
- `framework/runtime/src/main/java/org/pipelineframework/awaitable` — framework/runtime/src/main/java/org/pipelineframework/awaitable/AwaitCoordinator.java coordinates await-unit persistence, interaction…
- `framework/runtime/src/main/java/org/pipelineframework` — This division lets execution code produce pure continuation or commit plans while orchestration code applies the corresponding transition
- `framework/runtime/src/main/java/org/pipelineframework/telemetry` — Telemetry intent is represented by TelemetryPolicy, which deliberately contains framework policy rather than SDK or exporter state
- `framework/runtime-core/src/main/java/org/pipelineframework/connector` — ConnectorProviderId establishes stable, lowercase dotted provider identities, while ConnectorBindingName addresses a configured provider…
- `framework/deployment/src/main/java/org/pipelineframework/processor/ir` — Intermediate-representation mapping is the semantic boundary that turns YAML or legacy @PipelineStep definitions, annotation-derived type…
- `framework/runtime/src/main/java/org/pipelineframework/transport` — The function package establishes the vocabulary that lets adapters vary their transport shape without changing the surrounding pipeline…
- `framework/runtime/src/main/java/org/pipelineframework/transport/function` — The abstraction allows an invocation shape to resolve to direct execution, queue or topic handoff, or a streaming bridge
- `framework/runtime/src/main/java/org/pipelineframework/config/template` — framework/runtime/src/main/java/org/pipelineframework/config/template/PipelineTemplateConfig.java represents the complete template…

### Entry points
- `tools/replay-viewer/app.js`
- `web-ui/src/routes/+layout.svelte`
- `web-ui/src/routes/+page.svelte`

### Files that need care (bug-fix history first, then churn — check `get_risk` before editing)
- `framework/runtime/src/main/java/org/pipelineframework/PipelineExecutionService.java` — 9 bug fixes, last fix today (bug magnet); 19 commits/90d
- `framework/runtime/src/test/java/org/pipelineframework/PipelineExecutionServiceTest.java` — 8 bug fixes, last fix today (bug magnet); 14 commits/90d
- `framework/runtime/src/main/java/org/pipelineframework/orchestrator/DynamoExecutionStateStore.java` — 11 bug fixes, last fix yesterday (bug magnet); 18 commits/90d
- `examples/csv-payments/orchestrator-svc/src/test/java/org/pipelineframework/csv/orchestrator/service/AbstractCsvPaymentsEndToEnd.java` — 9 bug fixes, last fix 7 weeks ago (bug magnet); 21 commits/90d
- `framework/runtime/src/main/java/org/pipelineframework/awaitable/AwaitCoordinator.java` — 8 bug fixes, last fix 4 weeks ago (bug magnet); 17 commits/90d

### Code health
Three co-equal signals: defect risk 7.94/10 avg, hotspot health 5.48/10 (stable), worst `examples/csv-payments/orchestrator-svc/src/test/java/org/pipelineframework/csv/orchestrator/service/AbstractCsvPaymentsEndToEnd.java` at 1.0/10 · maintainability 8.62/10 · performance risk 329 open static I/O-in-loop / N+1 findings. Detail: `get_health()`.

Critical files:
- `framework/runtime/src/test/java/org/pipelineframework/PipelineRunnerCacheReadTest.java` — change entropy — impact −3.0
- `framework/runtime/src/test/java/org/pipelineframework/PipelineStepExecutorTest.java` — change entropy — impact −2.8
- `framework/connectors/llm-query-langchain4j/src/test/java/org/pipelineframework/connector/llm/langchain4j/LangChain4jOllamaQueryConnectorTest.java` — change entropy — impact −2.6
- `framework/runtime/src/main/java/org/pipelineframework/awaitable/AwaitAdmissionCoordinator.java` — complex conditional (persistedReservation) — impact −2.5
- `examples/restaurant-approval/self-host/container/run-container-ha-demo.sh` — change entropy — impact −2.4

### Standing decisions (ask `get_why` before diverging)
- Await owns durable suspension — Transport adapters may vary, but durable interaction correctness must not vary with them
- Change the semantic owner boldly — Avoiding the owning abstraction can reduce a diff while increasing permanent conceptual
  surface and compatibility burden.
- Command owns logical external effects — Exactly-once cannot be manufactured after an unknowable third-party result, but stable
  logical identity and recorded authority can prevent unsafe accidental redispatch.

<!-- REPOWISE_AGENTS:END -->
