---
title: Runtime owns shared Release production and resolution
status: accepted
---

# ADR-0084: Runtime Owns Shared Release Production and Resolution

## Context

[ADR-0066](./0066-build-produces-releases-and-cli-deploys-them.md) separates
release production from deployment. Preparing a Release for separate Lambda
ingress and worker packages exposed a gap. Maven could derive associations for
a single artefact, while multi-artefact callers had to repeat compiler metadata.

## Decision

Runtime owns the shared release producer and resolver. The resolver retains
`org.pipelineframework:tpf-release-resolver`. The Maven plugin and CLI use
the same producer, and the CLI consumes the resolver from Runtime.

For whole-pipeline Lambda placement, the caller identifies the ingress and
worker packages. The producer derives worker step and capability associations
from the compiled contract. Other placement shapes can continue to supply
explicit associations. Verification and deployment consume the completed
immutable descriptor.

## Rationale

The compiled contract remains the authority for the associations the producer
can derive. Co-locating the resolver and producer allows the CLI to depend on
Runtime without Runtime depending on the CLI repository.

## Consequences

The CLI can prepare a local descriptor after packaging. It does not build
application artefacts or publish them. Maven retains its existing lifecycle,
and the same descriptor format is used for both entry points.
