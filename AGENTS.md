# The Pipeline Framework

The Pipeline Framework (TPF) is a Java framework for strongly typed application flows.
Keep the core pure. Connect to reality.

Core modules:
- `framework/pom.xml`: Parent POM of the mult-module Maven project
- `framework/deployment`: compiler and code generation phases (Quarkus/canonical)
- `framework/runtime-core`: framework-neutral TPF abstractions
- `framework/runtime`: runtime APIs, execution engine, telemetry, config loading (Quarkus/canonical)
- `framework/runtime-spring`: runtime APIs, execution engine, telemetry, config loading (Springboot))

Supporting repo surfaces:

- `examples`: reference applications, topology smoke paths, and end-to-end compatibility surfaces
- `ai-sdk`: standalone Java SDK used for delegation/operator stress testing and mapper/transport exercises
- `docs`: VitePress documentation site
- `web-ui`: SvelteKit Canvas/web UI (unmaintained)

tpf-mcp-bridge lives in a separate repo now. It holds the MCP bridge and the template generator.

For planning, PR slicing, architecture tradeoffs, roadmap shaping, or docs IA strategy, read `AGENTS.planning.md`. For ordinary implementation work, use this file plus the smallest relevant local context.

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

- Pipeline order is emitted to `META-INF/pipeline/order.json` at build time.
- Telemetry metadata is emitted to `META-INF/pipeline/telemetry.json` at build time.
- Branching metadata is emitted to `META-INF/pipeline/branching.json` at build time.
- Runtime is reactive-first; blocking work must be explicitly offloaded.

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

## Persistence Plugin Notes

Persistence provider selection is configured via:

- runtime config key: `persistence.provider.class`
- build-time processor option: `-Apersistence.provider.class=<fqcn>`

Keep both forms aligned in docs and processor behavior.

## Testing Conventions

Unit tests use `*Test` with Surefire. Integration tests use `*IT` with Failsafe. E2E tests using containers should run in `verify` unless there is an explicit reason otherwise.

## Docs Source of Truth

Canonical docs live under top-level route directories:

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

For planning, PR slicing, roadmap, or architecture tradeoff work, load `AGENTS.planning.md`. For implementation work, read only the source files needed for the current decision and run the smallest validation command that proves the claim.

TPF-specific scoping rules:

- Core semantics usually live under `framework/runtime-core` `framework/runtime` and `framework/deployment`.
- Runtime integrations should stay scoped:
  - Spring work: `core + spring`, not Quarkus unless parity is claimed.
  - Quarkus work: `core + quarkus`, not Spring unless parity is claimed.
- Examples are compatibility surfaces, not disposable demos.
- Docs should be updated with semantic changes, but do not scan all docs unless the affected concept is unclear.
- Replay/web-ui is relevant when execution semantics, telemetry, step lifecycle, or visual replay state changes.
- `tpf-mcp-bridge` is separate; only involve it when template generation, schema export, scaffold generation, or generated project behavior changes.
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

## Token Discipline

Prefer Repowise MCP context over broad grep, but do not call every Repowise tool by default. Keep routine implementation context small; load the planning supplement only when the task is actually planning-shaped.

<!-- REPOWISE_AGENTS:START — Do not edit below this line. Auto-generated by Repowise. -->
## Repowise Codebase Context For pipelineframework

This repository is indexed by Repowise. Use the Repowise MCP tools for codebase orientation, discovery, implementation context, modification risk, design rationale, and cleanup planning. MCP data reflects the last index run; verify against source files before editing.

Last indexed: 2026-07-09 (commit 8691bbf2e). Confidence: 100%.
### Architecture
Using tpf-technical-writer for repository-facing documentation language, and I’ll rely on the provided repo summary plus available repo context rather than editing files. I’m pulling the repository overview from the local Repowise index now so the page reflects the indexed architecture rather than only the prompt summary. The Pipeline Framework consumes YAML runtime mappings plus annotated Java pipeline steps, operators, mappers, and configuration, compiles them through build-time validation and code-generation phases, and produces reactive Quarkus runtime artifacts for local, REST, gRPC, and function-oriented pipeline execution. This repository is a Java-first monorepo for building transport-neutral reactive pipeline systems.
### Key Modules
| Module | Purpose | Owner |
|--------|---------|-------|
| `framework/deployment/src/main/java/org/pipelineframework/processor` | The pipelineframework/processor module is the compiler orchestration layer of… | - |
| `examples/csv-payments` | I’ll use the TPF documentation skill for repo-specific wording and keep the… | - |
### Entry Points
- `framework/runtime/src/main/java/org/pipelineframework/config/PipelineStepConfig.java`
- `framework/runtime/src/main/java/org/pipelineframework/orchestrator/PipelineOrchestratorConfig.java`
- `framework/runtime/src/main/java/org/pipelineframework/config/StepConfig.java`
- `framework/deployment/src/main/java/org/pipelineframework/processor/PipelineStepProcessor.java`
- `examples/checkout/nextjs-ui/lib/checkout-flow.js`
- `examples/csv-payments/common/src/main/java/org/pipelineframework/csv/common/domain/PaymentRecord.java`
- `examples/csv-payments/common/src/main/java/org/pipelineframework/csv/common/domain/PaymentStatus.java`
- `examples/search/common/src/main/java/org/pipelineframework/search/common/domain/CrawlRequest.java`
- `framework/runtime/src/main/java/org/pipelineframework/telemetry/PipelineTelemetry.java`
- `framework/runtime/src/main/java/org/pipelineframework/config/PipelineConfig.java`
### Risk Hotspots
| File | Churn | 90d Commits | Owner |
|------|-------|-------------|-------|
| `tools/replay-viewer/datasets/csv-payments-built-in.json` | 100.0th percentile | 12 | mariano.barcia |
| `tools/replay-viewer/app.js` | 99.4th percentile | 22 | mariano.barcia |
| `tools/homepage-replay-video/data/csv-payments-cinematic.json` | 98.9th percentile | 9 | mariano.barcia |
| `docs/public/replay-viewer-app/datasets/csv-payments-built-in.json` | 98.3th percentile | 7 | Mariano Barcia |
| `examples/csv-payments/orchestrator-svc/src/test/java/org/pipelineframework/csv/orchestrator/service/AbstractCsvPaymentsEndToEnd.java` | 97.7th percentile | 31 | mariano.barcia |

### Repowise MCP Workflow

- Overview: call `get_overview()` at the start of an unfamiliar task to orient on architecture, modules, entry points, and tech stack.
- Search: call `search_codebase(query="...")` when locating where a concept, symbol, feature, or behavior is implemented.
- Context: call `get_context(targets=["path/or/symbol", "..."])` for enriched docs, ownership, decisions, callers, and related files before relying on raw source alone.
- Risk: call `get_risk(targets=["path/to/file.py"])` before modifying shared utilities, public APIs, hotspots, high-coupling modules, or files with unknown dependents.
- Why: call `get_why(query="...")` before architectural changes or when the user asks why code is structured a certain way.
- Dead code: call `get_dead_code(safe_only=true)` before cleanup or removal work; treat lower-confidence findings as candidates to investigate.
- Connections: call `get_context(targets=["..."], include=["callers", "callees"])` when tracing how a symbol connects to the rest of the code.

<!-- REPOWISE_AGENTS:END -->
