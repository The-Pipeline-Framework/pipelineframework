# Runtime Mapping (Phase 2)

Phase 2 introduces an optional runtime mapping file that lets users place pipeline steps (regular and synthetic) into modules and runtimes. The design keeps current scaffolding behavior by default, and adds a "majestic monolith" mode for teams that want a single runtime containing the orchestrator, services, and plugins.

This guide is intentionally pragmatic: most teams want fewer deployables, less operational drift, and predictable wiring. The mapping is a controlled way to get there without turning TPF into a deployment DSL.

## Phase 1 vs Phase 2

- **Phase 1 (today)**: module mapping is implicit (scaffold defaults) with optional overrides in `application.properties` (e.g., `pipeline.module.*`).
- **Phase 2 (this guide)**: explicit YAML runtime mapping that drives placement, validation, and pipeline-transport-aware wiring.

## Goals

- Allow explicit module/runtime placement via YAML.
- Preserve backward compatibility when no mapping is provided.
- Keep config minimal for the common case.
- Provide deterministic, versionable synthetic step identifiers.
- Support both gRPC and REST wiring without drift (transport remains pipeline-global).
- Enable a single-runtime "monolith" option without introducing a deployment DSL.

## Non-goals

- A full deployment or orchestration configuration language.
- Replacing existing build or runtime config formats.
- Enforcing a single architecture style.

## Contents

- [Schema and Semantics](/guide/evolve/runtime-mapping/schema)
- [Synthetic Step Identifiers](/guide/evolve/runtime-mapping/synthetics)
- [Annotation Processing and Transport](/guide/evolve/runtime-mapping/annotation-processing)
- [Validation, Migration, and Examples](/guide/evolve/runtime-mapping/validation-migration-examples)
- [Cheat Sheet](/guide/evolve/runtime-mapping/cheat-sheet)
- [TDD Plan](/guide/evolve/runtime-mapping/tdd-plan)
- [Implementation Plan](/guide/evolve/runtime-mapping/implementation-plan)

## Quick orientation

- **modular**: keep the current multi-module behavior, only place what you need.
- **pipeline-runtime**: one runtime per pipeline, orchestrator stays separate. This is the default recommendation and keeps a minimal network boundary for future splits.
- **monolith**: one runtime for everything (orchestrator + services + plugins).

## A short story (why this exists)

Most teams are not trying to win an architecture contest. They are trying to ship changes, keep costs predictable, and avoid rebuilding the world every time the pipeline grows. Runtime mapping is a small, explicit layer that lets you collapse or expand the topology without changing the code you already trust.
