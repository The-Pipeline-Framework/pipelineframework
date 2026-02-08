# Runtime Mapping

Runtime mapping is the official mechanism for controlling where orchestrator,
regular steps, and synthetic/plugin side effects are placed.

It is optional and backward compatible:

- If `pipeline.runtime.yaml` is absent, scaffold defaults remain in effect.
- If `pipeline.runtime.yaml` is present, placement/validation/wiring follow that file.

Use this section as the reference for mapping semantics.

## Goals

- Allow explicit module/runtime placement via YAML.
- Preserve backward compatibility when no mapping is provided.
- Keep config minimal for the common case.
- Provide deterministic, versionable synthetic step identifiers.
- Support both gRPC and REST wiring without drift (transport remains pipeline-global).
- Enable a single-runtime "monolith" option without introducing a deployment DSL.

## What runtime mapping controls

- Logical placement of steps and synthetics.
- Placement defaults and strictness rules.
- Generated client/server wiring aligned to placement and transport.

## What runtime mapping does not control

- Maven module topology.
- Number of physical deployables produced.
- CI lane composition/build matrix.

That separation is intentional. Runtime mapping defines logical architecture;
Maven topology defines physical artifacts.

## Non-goals

- A full deployment or orchestration configuration language.
- Replacing existing build or runtime config formats.
- Enforcing a single architecture style.

## Relationship to Canvas and Maven

Typical flow:

1. Design app in Web UI Canvas Designer and download scaffold.
2. Add or refine `pipeline.runtime.yaml`.
3. Align Maven topology to target runtime shape (`modular`, `pipeline-runtime`, `monolith`).

If steps 2 and 3 diverge, behaviour may be logically valid but operationally confusing.
For example, `layout: monolith` with modular Maven still yields modular artifacts.

See:

- [Runtime Layouts and Build Topologies](/guide/build/runtime-layouts/)
- [Using Runtime Mapping](/guide/build/runtime-layouts/using-runtime-mapping)
- [Maven Migration Playbook](/guide/build/runtime-layouts/maven-migration)

## Contents

- [Schema and Semantics](/guide/evolve/runtime-mapping/schema)
- [Synthetic Step Identifiers](/guide/evolve/runtime-mapping/synthetics)
- [Annotation Processing and Transport](/guide/evolve/runtime-mapping/annotation-processing)
- [Validation, Migration, and Examples](/guide/evolve/runtime-mapping/validation-migration-examples)
- [Build Topologies (What Is Real Today)](/guide/evolve/runtime-mapping/build-topologies)
- [Cheat Sheet](/guide/evolve/runtime-mapping/cheat-sheet)

## Quick orientation

- **modular**: keep the current multi-module behavior, only place what you need.
- **pipeline-runtime**: one runtime per pipeline, orchestrator stays separate. This is the default recommendation and keeps a minimal network boundary for future splits.
- **monolith**: one runtime for everything (orchestrator + services + plugins).

## Engineering Notes

These pages are implementation-oriented and mainly for maintainers:

- [TDD Plan](/guide/evolve/runtime-mapping/tdd-plan)
- [Implementation Plan](/guide/evolve/runtime-mapping/implementation-plan)
