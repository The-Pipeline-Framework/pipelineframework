---
title: Repository ownership follows released contracts
status: accepted
---

# ADR-0063: Repository ownership follows released contracts

## Context

TPF's compiler, portable contracts, runtime integrations, Connectors, Blocks, Expansions, examples, reference
systems, and applications previously shared one source repository. That made source proximity and Quarkus
runtime/deployment packaging appear to define ownership even where components could interoperate through stable
Maven artifacts.

The compiler owns TPF language semantics and contract generation. Portable runtime contracts must be shared by
application code and independently operated execution infrastructure without either side importing a Quarkus or
Spring implementation. Ecosystem packages need different release cadences from those semantic and runtime cores.

## Decision

TPF assigns repository ownership where the dependent side can consume a released contract instead of requiring
source-level atomicity.

- `pipelineframework-contracts` owns framework-neutral authored, semantic, serialization, runtime protocol, runtime
  API, runtime SPI, and portable core contracts.
- `pipelineframework-compiler` owns the JSR-269 build host and all compiler semantic phases and renderers over those
  contracts.
- `pipelineframework-runtime` owns the Quarkus runtime/deployment pair, Spring adapter, and foundational runtime
  plugins.
- Connector, Block, and Expansion implementations have separate ecosystem repositories.
- Examples, reference implementations, and real applications consume released artifacts from separate repositories.
- `pipelineframework` remains the documentation, cross-artifact conformance, immutable candidate-overlay and
  full-train policy, BOM, and release-coordination repository. It does not retain source mirrors or owner-local
  tests of independently published components.

Repository, Maven artifact, Java package, release, Quarkus extension pair, and runtime process boundaries remain
distinct. A future Jandex compiler source adapter must feed the same semantic model as JSR-269 rather than giving a
Quarkus deployment module separate semantic ownership.

## Rationale

Released contracts make ownership and compatibility explicit while reducing the source and context an engineer or
agent must load for a change. Keeping the Quarkus runtime/deployment pair together preserves its real build-time
coupling without letting that packaging mechanic own the compiler model. Independent ecosystem repositories let
Connectors, Blocks, Expansions, and applications evolve without rebuilding the framework core.

## Consequences

- Changes to a published boundary require compatibility evidence and an ordered multi-repository release when the
  consumer cannot remain compatible with the previous release.
- Owner-local verification is necessary but not sufficient for a released boundary. Cross-repository candidate
  overlays, compatibility sets, and full-train promotion follow
  [ADR-0062](./0062-cross-repository-system-tests-use-immutable-overlays.md).
- The product BOM records a tested component set but does not impose one release lifecycle.
- Canonical user documentation and durable architectural decisions remain in `pipelineframework`; component
  READMEs document repository-local ownership and link back to those sources.
- GitNexus cross-repository analysis uses the `tpf` group, while current source in the owning worktree remains
  authoritative.
- Quarkus deployment code may adapt compiler output but may not redefine compiler semantics.
- Shared runtime artifacts remain implementation-neutral so customer applications and separately operated workers
  can share protocol and model compatibility without sharing runtime implementation ownership.
