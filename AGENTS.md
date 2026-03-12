# The Pipeline Framework

## Project Overview

The Pipeline Framework (TPF) is a Java framework for reactive pipeline systems on Quarkus 3.31.3+.
It generates transport adapters at build time from pipeline metadata:

- gRPC server/client artifacts
- REST resource/client artifacts
- local client artifacts for in-process transport

Core modules:

- `framework/runtime`: runtime APIs, execution engine, telemetry, config loading
- `framework/deployment`: annotation processor and code generation phases

Supporting repo surfaces:

- `template-generator-node`: Node-based template generator and schema/templates
- `web-ui`: SvelteKit Canvas/web UI
- `docs`: VitePress documentation site

### Current State (Post-PA Era)

- Compilation is YAML-driven first, with annotation processing and model phases enforcing contracts before runtime.
- Operator steps are first-class (`operator: fully.qualified.Class::method`) and are resolved/validated at build time.
- Mapper model uses `Mapper<Domain, External>` and pair-based matching for boundary validation.
- Function platform (`pipeline.platform=FUNCTION`) is an actively used runtime path and must stay semantically aligned with other runtime paths.
- Runtime lineage behavior (split/merge envelopes) is treated as deterministic behavior, not best-effort metadata.

## Canonical Terms

- **Runtime layout**: logical runtime shape declared in runtime mapping (`modular`, `pipeline-runtime`, `monolith`).
- **Build topology**: Maven module/POM structure that physically builds deployables for a layout.

These are related but not identical. Runtime mapping does not automatically reshape Maven modules.

## CSV Payments Reference Topologies

Current example entrypoints:

- Modular: `examples/csv-payments/pom.xml`
- Pipeline-runtime: `examples/csv-payments/pom.pipeline-runtime.xml`
- Monolith: `examples/csv-payments/pom.monolith.xml`

Helper scripts:

- `examples/csv-payments/build-pipeline-runtime.sh`
- `examples/csv-payments/build-monolith.sh`

When changing layout support in csv-payments, update all of:

1. Topology POM/script
2. Runtime mapping YAML
3. Topology test(s)
4. E2E IT path (through `AbstractCsvPaymentsEndToEnd`)
5. Relevant CI workflows under `.github/workflows/` (at minimum `full-tests.yml` and any `e2e-csv-*.yml` jobs that cover the changed layout)
6. Build docs under `docs/guide/build/runtime-layouts/`

## Runtime and Build Commands

Framework:

- Build: `./mvnw -f framework/pom.xml clean install`
- Verify: `./mvnw -f framework/pom.xml verify`

Root project:

- Build: `./mvnw clean install`
- Verify: `./mvnw verify`

CSV payments targeted examples:

- Pipeline-runtime orchestrator verification:
  `./examples/csv-payments/build-pipeline-runtime.sh -pl orchestrator-svc -Dcsv.runtime.layout=pipeline-runtime -Dtest=PipelineRuntimeTopologyTest -Dit.test=CsvPaymentsPipelineRuntimeEndToEndIT verify`

- Monolith verification:
  `./examples/csv-payments/build-monolith.sh -DskipTests`

Search targeted example:

- Function platform smoke verification (build-switch based; no Lambda Maven profile):
  `./mvnw -f examples/search/pom.xml -pl orchestrator-svc -am -Dpipeline.platform=FUNCTION -Dpipeline.transport=REST -Dpipeline.rest.naming.strategy=RESOURCEFUL -DskipTests compile`
  `./mvnw -f examples/search/pom.xml -pl orchestrator-svc -Dpipeline.platform=FUNCTION -Dpipeline.transport=REST -Dpipeline.rest.naming.strategy=RESOURCEFUL -Dtest=LambdaMockEventServerSmokeTest test`

Targeted unit-test coverage helper:

- Generate deterministic JaCoCo coverage for a single framework module + test slice:
  `./scripts/coverage-targeted.sh runtime FunctionTransportBridgeTest,UnaryFunctionTransportBridgeTest`
  `./scripts/coverage-targeted.sh deployment RestFunctionHandlerRendererTest`
- Helper output includes report path and LINE/BRANCH percentages from module-local `target/site/jacoco/jacoco.xml`.

Node/docs surfaces:

- Template generator tests: `npm --prefix template-generator-node test`
- Web UI type/build checks: `npm --prefix web-ui run check`, `npm --prefix web-ui run build`
- Docs build: `npm --prefix docs run build`

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

Application-user docs:

- Build/runtime-layout docs: `docs/guide/build/runtime-layouts/`
- Build mechanics: `docs/guide/build/`
- Operations and usage: `docs/guide/operations/`, `docs/guide/development/`

Implementation/developer-internal docs:

- `docs/guide/evolve/`
- `docs/guide/evolve/tpfgo/` (TPFGo reference guide and DDD alignment)

Use links between these areas when a feature spans both implementation and app usage.
Prefer the split annotation-processor guide under `docs/guide/evolve/annotation-processor/` over older compatibility pages when both exist.

## Canonical Entry Docs

- `docs/guide/build/runtime-layouts/index.md`
- `docs/guide/build/pipeline-compilation.md`
- `docs/guide/build/operators.md`
- `docs/guide/development/testing.md`
- `docs/guide/plugins/persistence.md`
- `docs/guide/development/using-plugins.md`
- `docs/guide/evolve/annotation-processor/index.md`
- `docs/guide/evolve/compiler-pipeline-architecture.md`
- `docs/guide/evolve/plugins-architecture.md`
- `docs/guide/evolve/publishing.md`
- `docs/guide/evolve/ci-guidelines.md`
- `docs/guide/evolve/tpfgo/index.md`

## Agent Working Rules for This Repo

- Prefer `rg`/`rg --files` for searching.
- Do not perform destructive git operations unless explicitly requested.
- Do not commit/push unless explicitly requested.
- If unexpected unrelated working-tree changes appear mid-task, stop and ask.
- Keep user-facing docs (`build`/`development`/`operations`) free of internal planning terminology unless the topic is explicitly implementation-internal (`docs/guide/evolve/`).
- Prefer enriching existing guide pages over introducing standalone “feature islands” that duplicate navigation.
- Do not add “audience declaration” sections in user-facing docs. Make docs audience-fit by placing content in the right guide area:
  - `development`: implementation and usage
  - `operations`: observability and response
  - `build`: topology/configuration/application of features
  - `evolve`: internals, design notes, and backlog-oriented material
- Keep risk registers, update reports, and future-work tracking out of user-facing docs unless they are actionable operator runbooks; place backlog/planning artifacts under `docs/guide/evolve/` or external issue trackers.
- When changing operator or mapper semantics, update code + tests + docs together in the same change set.
- When changing runtime-layout, generator, or Canvas/web UI semantics, update `template-generator-node`, `web-ui`, affected docs, and tests together when applicable.

## PR Slicing Criteria

When planning or proposing PRs, optimize for cohesive slices rather than many small PRs.

- One production risk per PR (for example: transport parity, durable state, DLQ durability, or crash-recovery semantics).
- One clear validation gate per PR: 1-3 deterministic CI commands that prove the slice.
- No mixed intent in the same PR (avoid combining unrelated runtime logic, docs refactors, and generator rewrites).
- Bounded blast radius (prefer a single module or a single execution path per PR unless explicitly approved).
- Mergeable independently: each PR must be shippable without requiring hidden follow-up fixes.
- Default target is fewer, larger-cohesive PRs per workstream (typically 1-2), not many micro-PRs.
