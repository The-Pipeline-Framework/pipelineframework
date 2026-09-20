---
title: Reference implementations consume released artifacts
status: accepted
---

# ADR-0060: Reference implementations consume released artifacts

## Context

Checkout/TPFGo, Search, and QuickBooks Collections Briefing are larger and more operationally realistic than
focused learning proofs. Keeping them in the framework repository made their bytes, deployment workflows, and
release-coupled build assumptions part of routine framework work even though they can consume released TPF
contracts and implementations.

## Decision

The standalone `pipelineframework-reference-implementations` repository owns Checkout/TPFGo, Search, and
QuickBooks Collections Briefing.

- Reference implementations consume released compiler, runtime, contract, Connector, Block, and Expansion
  artifacts.
- Their application modules, deployment assets, UIs, and owned verification workflows evolve together.
- They do not publish framework or ecosystem artifacts and do not own TPF semantics.
- Framework changes prove compatibility through released snapshots and dedicated consumer CI instead of source
  inclusion in the framework reactor.
- Full product applications such as CSV Payments and RAG Turnkey retain separate ownership from reference
  implementations.

## Consequences

- Framework and ecosystem agents no longer load these large applications unless their compatibility surface is
  relevant.
- Cross-repository changes may require publishing a snapshot before consumer verification.
- Search cloud credentials and deployment workflows belong to the reference-implementations repository.
- Documentation may describe these systems, but runnable paths link to their owning repository.
