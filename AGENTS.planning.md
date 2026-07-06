# Agent Planning Context

Use this file only for planning, architecture tradeoffs, PR slicing, roadmap shaping, docs IA strategy, or cross-surface change design. For ordinary implementation work, start from the root `AGENTS.md` and load only the local source/doc files needed for the task.

## Planning Posture

Planning work should reduce ambiguity and produce mergeable slices. Keep recommendations grounded in current code, current docs, and current examples; do not turn roadmap ideas into product claims.

When planning, separate:

- current behavior,
- desired behavior,
- violated or protected invariants,
- migration cost,
- validation gates,
- follow-up slices.

## Repowise Use

Use Repowise as an index, not authority:

1. `get_overview()` only when the task is unfamiliar.
2. `get_answer()` for focused architecture or location questions.
3. `get_why()` for rationale before changing established patterns.
4. `get_risk()` before modifying public APIs, shared runtime/compiler utilities, hotspots, high-coupling modules, or broad PR file sets.
5. Verify conclusions against active worktree files before editing.

Do not refresh or rebuild Repowise unless the user asks or refreshed indexed context is genuinely needed after meaningful edits.

## PR Slicing

Optimize for cohesive, independently mergeable PRs:

- one production risk per PR, such as transport parity, durable state, DLQ durability, crash-recovery semantics, docs IA, or generator schema compatibility;
- one clear validation gate, usually 1-3 deterministic commands;
- no mixed intent, such as runtime logic plus unrelated docs refactors plus generator rewrites;
- bounded blast radius, preferably one module, execution path, or docs surface;
- each PR should be shippable without hidden follow-up fixes;
- prefer fewer, larger-cohesive PRs over many micro-PRs when one concept spans code, tests, docs, and examples.

Before proposing a PR, identify:

- changed surfaces,
- compatibility/reference examples affected,
- docs pages that must move with the change,
- CI workflows or smoke paths that prove it,
- unresolved risks that should remain out of scope.

## Cross-Surface Change Checklist

Use this when a semantic change touches compiler/runtime behavior:

- compiler/deployment validation,
- runtime execution semantics,
- generated adapters or handlers,
- examples and E2E smoke paths,
- telemetry/replay metadata,
- replay viewer node rendering or legend,
- docs under the right top-level route,
- `tpf-mcp-bridge` when schema export, template generation, scaffold generation, or generated project behavior changes.

For `examples/csv-payments` runtime-layout work, keep all of these aligned:

1. topology POM/script,
2. runtime mapping YAML,
3. topology tests,
4. E2E IT path through `AbstractCsvPaymentsEndToEnd`,
5. relevant CI workflows,
6. docs under `docs/deploy/runtime-layouts/`.

## Docs IA Planning

Canonical docs live under:

- `docs/design/`: architecture, concepts, and user-facing design rationale;
- `docs/develop/`: implementation and usage;
- `docs/deploy/`: runtime topology and deployment mechanics;
- `docs/operate/`: observability, runtime response, and playbooks;
- `docs/evolve/`: internals, design notes, backlog-oriented material;
- `docs/value/`: public-facing value framing.

`docs/guide/**` files are redirect/noindex compatibility stubs only. Do not add real content there.

Prefer enriching existing canonical pages over creating standalone feature islands. Keep user-facing docs free of internal planning language unless the topic is explicitly implementation-internal. Keep risk registers, update reports, and future-work tracking out of user-facing docs unless they are actionable operator runbooks.

## Architecture Planning Guardrails

- Transport mode, platform mode, deployment pattern, worker invocation protocol, and wire/envelope protocol are separate layers.
- Runtime layout and build topology are related but not interchangeable.
- Mapper selection must stay pair-accurate and deterministic.
- Operator and connector contract failures should surface at build time where possible.
- FUNCTION and COMPUTE paths should preserve equivalent cardinality, mapper, rejection, failure, and lineage semantics unless documented and tested.
- Split/merge lineage is deterministic behavior, not best-effort metadata.
- New durable control-plane storage should prefer immutable records, conditional writes, and append-only event records.

## Validation Planning

Pick the smallest validation set that proves the planned slice. Common gates:

- framework: `./mvnw -f framework/pom.xml verify`;
- root: `./mvnw verify`;
- AI SDK: `./mvnw -f ai-sdk/pom.xml test`;
- docs: `npm --prefix docs test` and `npm --prefix docs run build`;
- web UI: `npm --prefix web-ui run check` and `npm --prefix web-ui run build`;
- targeted coverage: `./scripts/coverage-targeted.sh <module> <tests>`.

If a planned change affects release notes, version snapshots, routes, or docs IA, include docs build and route/link checks in the validation gate.

