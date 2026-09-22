---
title: Product BOM pins the tested component set
status: accepted
---

# ADR-0061: Product BOM pins the tested component set

## Context

TPF contracts, compiler, runtime integrations, Connectors, Blocks, and Expansions now have independent repository
and release ownership. Applications need one explicit statement of which released versions have been verified
together without forcing those repositories to share a release lifecycle or requiring every consumer to repeat the
same version properties.

The coordinating `pipelineframework` repository retains the cross-artifact compatibility tests and publication
metadata. That makes it the smallest owner capable of publishing the tested product-level component set without
moving implementation ownership back into an umbrella reactor.

## Decision

The `pipelineframework` repository publishes `org.pipelineframework:pipelineframework-bom`. The BOM manages the
compatible versions of released TPF contracts, compiler, customer runtimes, foundational plugins, Connectors,
Blocks, and Expansion distributions.

Each component family keeps its own version property and release lifecycle. Updating the BOM records a tested
combination; it does not imply that every repository must release together and it is not a generic semantic-version
resolver.

The coordinating compatibility reactor imports the BOM and declares its cross-artifact dependencies without
individual versions. Consumer repositories should import a released BOM version once the required component
artifacts have been published. Maven build-plugin versions remain explicit because dependency-management imports do
not manage application build plugins.

## Rationale

A released BOM is a normal Maven contract that downstream repositories can consume without source-level
atomicity. Keeping the tested set in the coordination repository preserves independent ownership while making
compatibility deliberate, reviewable, and reproducible.

## Consequences

- Component artifacts are published before a BOM version that refers to them.
- Cross-repository compatibility failures are fixed in the owning component or by selecting another compatible
  version; they are not hidden by rebuilding source in this reactor.
- Application, example, and reference repositories can remove repeated TPF dependency versions as they adopt the
  BOM.
- The BOM release and documentation identify the exact compatible component versions even when those versions
  diverge in the future.
- Maven plugins and repository-local helper artifacts remain outside the dependency BOM contract.
