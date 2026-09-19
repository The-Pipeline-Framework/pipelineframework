---
title: Packaged Blocks have independent release ownership
status: accepted
---

# ADR-0057: Packaged Blocks have independent release ownership

## Context

Packaged Blocks are compiler-visible composition artifacts consumed as ordinary Maven dependencies. They use
stable TPF authored-service and runtime contracts and may require separately released Connector capabilities,
but they do not own compiler semantics, runtime implementation, framework integration, or application authority.

Keeping their source in the framework monorepo made their release identity and build graph appear atomic with
Quarkus runtime and deployment mechanics even though applications can consume a released Block contract.

## Decision

The `pipelineframework-blocks` repository owns the reusable packaged Block artifacts. It consumes released TPF
contract and Connector artifacts and publishes its own independently versioned Maven artifacts. The framework
monorepo and examples consume those released Block versions instead of rebuilding Block source in their reactor.

Block resources remain static compiler inputs. Extraction does not introduce runtime Block discovery, a Block
executor, application-owned connector bindings, or Command authority.

## Consequences

- Block releases can evolve independently while compiler compatibility remains enforced by their normalized
  contracts and example compatibility tests.
- Customer applications need Block artifacts plus the capabilities they select; they do not inherit a TPF
  runtime implementation or Quarkus deployment module from the Blocks repository.
- Cross-repository changes use snapshots and compatibility tests when a Block and a contract or Connector must
  move together.
