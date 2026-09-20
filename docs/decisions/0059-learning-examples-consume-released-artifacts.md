---
title: Learning examples consume released artifacts
status: accepted
---

# ADR-0059: Learning examples consume released artifacts

## Context

Focused proofs and learning applications exercised compiler, runtime, Connector, Block, and Expansion behavior,
but lived inside the framework source reactor. That placement made their successful build compatible with source
coupling and required agents working on examples to load unrelated framework implementation bytes. Real
applications and long-lived reference implementations also have different ownership and operational needs from
small architectural proofs.

## Decision

The `pipelineframework-examples` repository owns focused learning examples and architectural proofs. Its canonical
reactor consumes released TPF compiler, runtime, Connector, Block, and Expansion artifacts. Example-local support
modules may live beside the proof that owns them, but are not ecosystem releases or dependencies for unrelated
repositories.

Real applications and long-lived reference implementations remain outside this boundary and move under their own
ownership separately. Runtime-integration smoke tests stay with the runtime repository that owns the behavior.

## Consequences

- Example builds provide clean-consumer compatibility evidence instead of relying on same-reactor source state.
- Example work requires substantially less repository context while retaining executable proofs and focused CI.
- Cross-repository semantic changes update released artifacts first, then validate compatible example consumers.
- Examples are not published Maven artifacts and must not become an alternate owner of framework semantics,
  ecosystem capabilities, or runtime implementations.
