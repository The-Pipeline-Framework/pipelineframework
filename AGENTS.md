# The Pipeline Framework

## Project Overview

The Pipeline Framework (TPF) is a Java framework for reactive pipeline systems on Quarkus 3.33.1+.
It generates transport adapters at build time from pipeline metadata:

- gRPC server/client artifacts
- REST resource/client artifacts
- local client artifacts for in-process transport

Core modules:

- `framework/runtime`: runtime APIs, execution engine, telemetry, config loading
- `framework/deployment`: annotation processor and code generation phases

Supporting repo surfaces:

- `examples`: reference applications, topology smoke paths, and end-to-end compatibility surfaces
- `ai-sdk`: standalone Java SDK used for delegation/operator stress testing and mapper/transport exercises
- `tpf-mcp-bridge` (separate repo): MCP bridge and template generator snapshot
- `web-ui`: SvelteKit Canvas/web UI
- `docs`: VitePress documentation site

### Current State (Post-PA Era)

- Compilation is YAML-driven first, with annotation processing and model phases enforcing contracts before runtime.
- Operator steps are first-class (`operator: fully.qualified.Class::method`) and are resolved/validated at build time.
- Mapper model uses `Mapper<Domain, External>` and pair-based matching for boundary validation.
- Function platform (`pipeline.platform=FUNCTION`) is an actively used runtime path and must stay semantically aligned with other runtime paths.
- Runtime lineage behavior (split/merge envelopes) is treated as deterministic behavior, not best-effort metadata.

## Canonical Terms

Load `AGENTS.glossary.md` when terminology, docs wording, transport/platform naming, architecture explanations, or public-facing copy matters.

Always keep these distinctions active:

- **Functional core / imperative shell**: business logic stays typed and transport-neutral; TPF owns generated adapters, connectors, await handling, persistence, caching, replay, telemetry, retries, and deployment/runtime integration.
- **Pipeline**: a strongly typed application flow, not a CI/CD pipeline, generic workflow diagram, or arbitrary orchestration graph.
- **Transport mode**: only `GRPC`, `REST`, and `LOCAL` as `pipeline.transport` values. `FUNCTION`, `HTTP_LAMBDA`, `PROTOBUF_HTTP_V1`, and `ENVELOPE_HTTP_V1` are separate platform/deployment/wire-protocol concepts.
- **Runtime layout vs build topology**: runtime layout is the logical runtime shape; build topology is the Maven/JAR/container structure that physically builds deployables.
- **Connector vs plugin**: connectors model typed I/O boundaries; plugins provide cross-cutting framework extensions such as persistence, caching, telemetry, or logging.

## Runtime and Build Commands

Load `AGENTS.validation.md` before choosing validation commands for non-trivial changes.

Most common gates:

- Framework verify: `./mvnw -f framework/pom.xml verify`
- Root verify: `./mvnw verify`
- Docs build: `npm --prefix docs run build`

## Maven Cache Policy

Multiple Codex worktrees may point at clones of this repo at the same time. Keep Maven cache behavior explicit:

- Use the shared Maven local repository, `~/.m2/repository`, by default. It stores downloaded dependencies and artifacts written by `mvn install`; override with `-Dmaven.repo.local=...` only when isolation is required.
- Use the worktree-local Maven build cache, `${session.executionRootDirectory}/.maven-build-cache`, for reusable build outputs. This is configured in `.mvn/maven-build-cache-config.xml` to avoid cross-worktree build-output leakage from the default `~/.m2/build-cache`.
- Use a worktree-local Maven repository such as `.mvn-local` only for tasks that require `mvn install`, generated scaffold smoke tests, or external repo/generated project consumption of `org.pipelineframework:*` artifacts.

Normal reactor builds should resolve reactor modules directly, not through `~/.m2/repository`. The collision risk is when TPF examples, templates, generated projects, or other external consumers depend on same-version `org.pipelineframework:*` artifacts outside the current reactor.

## Architecture Notes

- Pipeline order is emitted to `META-INF/pipeline/order.json` at build time.
- Telemetry metadata is emitted to `META-INF/pipeline/telemetry.json` at build time.
- Runtime is reactive-first; blocking work must be explicitly offloaded.

### Steps and Aspects

- `@PipelineStep` services are converted into IR and binding models, then rendered per generation target.
- Aspects are semantic side effects expanded during compilation.
- Side effects, plugins, and synthetics are first-class in mapping/target resolution.

### Transport Modes

- `GRPC`: remote client/server generation
- `REST`: REST resource/client generation
- `LOCAL`: in-process client generation (no remote hop)
- `HTTP_LAMBDA` (deployment pattern): HTTP/Lambda-style runtime path implemented as `pipeline.transport=REST` with `pipeline.platform=FUNCTION`

### Platform Modes

- `FUNCTION`: function handler path (including Lambda-oriented flows)
- `COMPUTE`: service/resource execution path

Transport and platform are orthogonal dimensions; avoid coupling operator category directly to transport decisions.

## Current Engineering Invariants

- Operator contract failures should be raised at build time whenever possible (resolution, shape, cardinality/link compatibility).
- gRPC-bound flows require descriptor availability and compatible bindings at generation/binding phases.
- Mapper inference/selection should remain pair-accurate (`Domain` + `External`) and deterministic in ambiguity diagnostics.
- Split/merge lineage IDs and ordering must be deterministic and replay-safe across runtime adapters.
- FUNCTION vs COMPUTE/REST paths should preserve equivalent behavior for cardinality and failure semantics unless explicitly documented.
- New TPF control-plane storage should prefer immutable internal records. For new Dynamo-backed coordinator stores, avoid `UpdateItem`/upsert semantics; prefer conditional writes, immutable records, and append-only event records. Existing execution/await stores are legacy exceptions until explicitly redesigned.
- New code should not use `return null`; use `Optional`, empty collections, explicit result records, or exceptions. Existing legacy/null-heavy code is not a precedent for new work.

## Persistence Plugin Notes

Persistence provider selection is configured via:

- runtime config key: `persistence.provider.class`
- build-time processor option: `-Apersistence.provider.class=<fqcn>`

Keep both forms aligned in docs and processor behavior.

## Testing Conventions

- Unit tests: `*Test` (Surefire)
- Integration tests: `*IT` (Failsafe)
- E2E tests using containers should run in `verify` unless there is an explicit reason otherwise.

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
Prefer the split annotation-processor guide under `docs/evolve/annotation-processor/` over older compatibility pages when both exist.

## Canonical Entry Docs

- `docs/deploy/runtime-layouts/index.md`
- `docs/develop/pipeline-compilation/index.md`
- `docs/develop/operators.md`
- `docs/develop/testing.md`
- `docs/design/persistence.md`
- `docs/develop/using-plugins.md`
- `docs/evolve/annotation-processor/index.md`
- `docs/evolve/compiler-pipeline-architecture.md`
- `docs/evolve/plugins-architecture.md`
- `docs/evolve/publishing.md`
- `docs/evolve/ci-guidelines.md`
- `docs/evolve/tpfgo/index.md`

## Agent Working Rules for This Repo

- Prefer `rg`/`rg --files` for searching.
- Do not perform destructive git operations unless explicitly requested.
- Do not commit/push unless explicitly requested.
- If unexpected unrelated working-tree changes appear mid-task, stop and ask.
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
- When changing runtime-layout, generator, or Canvas/web UI semantics, update `web-ui`, affected docs, tests, and the separate `The-Pipeline-Framework/tpf-mcp-bridge` repository when applicable.

## PR Slicing Criteria

When planning or proposing PRs, optimize for cohesive slices rather than many small PRs.

- One production risk per PR (for example: transport parity, durable state, DLQ durability, or crash-recovery semantics).
- One clear validation gate per PR: 1-3 deterministic CI commands that prove the slice.
- No mixed intent in the same PR (avoid combining unrelated runtime logic, docs refactors, and generator rewrites).
- Bounded blast radius (prefer a single module or a single execution path per PR unless explicitly approved).
- Mergeable independently: each PR must be shippable without requiring hidden follow-up fixes.
- Default target is fewer, larger-cohesive PRs per workstream (typically 1-2), not many micro-PRs.
