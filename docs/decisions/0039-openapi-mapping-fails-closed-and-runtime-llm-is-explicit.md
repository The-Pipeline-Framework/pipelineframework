---
title: OpenAPI mapping fails closed and runtime LLM use is explicit
status: accepted
---

# ADR-0039: OpenAPI mapping fails closed and runtime LLM use is explicit

## Context

An imported OpenAPI operation can use a wire representation that does not match the application's
canonical type. TPF can validate deterministic field options and curated Mappers at build time. An
earlier design also anticipated an LLM-backed authoring Block that would generate a proposal for a
developer to review and commit, but no Maven goal, CLI, report, review UI, or example host presents
that result today. Treating that future workflow as the current fallback obscures what developers
can actually run.

Calling a model during execution is immediately composable through an ordinary application-bound
LLM Query, but it has a materially different cost and reliability profile from generated mapping.
That difference must remain visible in the authored Pipeline.

## Decision

OpenAPI representation resolution fails the build when direct mapping is impossible and the
application has not supplied a mapping. The developer must choose one of three explicit paths:

1. write deterministic `options.fields`;
2. provide a curated representation type and `Mapper`; or
3. author an LLM Query as a runtime mapping step and explicitly accept its cost.

The third path is ordinary Pipeline composition, not importer inference. It incurs one additional
model call for every item or Pipeline execution that crosses the step. The OpenAPI importer and
pinned HTTP provider never add or invoke that model call automatically.

The existing `openapi-representation-mapper` Block remains available as a possible future authoring
optimisation. It is not documented as today's usable fallback until a concrete authoring host makes
its proposal visible and reviewable.

## Rationale

Failing closed preserves compile-time guarantees and prevents a hidden cost or semantic guess from
appearing at an external boundary. The runtime option is still useful for probing an unfamiliar API
in local development or staging, and its explicit topology makes latency, cost, telemetry, replay,
and failure handling reviewable. Stable production Pipelines naturally favour deterministic
options or curated code once the provider's semantics are understood.

## Consequences

- Missing OpenAPI mappings remain build errors unless the application explicitly authors one of the
  three paths.
- `options.fields` and curated Mappers remain deterministic production defaults.
- Runtime LLM mapping is possible but adds a model call per item or Pipeline execution.
- Documentation must not imply that Maven, the importer, or the pinned HTTP provider invokes an LLM
  fallback.
- The authoring-only mapping Block is retained without promising a developer-facing workflow that
  does not yet exist.

This decision supersedes only the representation-mapping fallback described in
[ADR-0035](./0035-openapi-imports-release-pinned-capabilities-not-authority.md). ADR-0035's
release-pinning and application-authority decisions remain accepted.
