---
title: Generated boundaries and typed connectors
status: accepted
---

# ADR-0010: Generated boundaries and typed connectors

## Context

Generic executors, service locators, provider factories, and handwritten adapters create
parallel interpretations of pipeline semantics and conceal external-boundary behaviour.

## Decision

Prefer compiler-generated/framework boundaries over application glue. `pipeline.yaml`
is the primary semantic contract; annotations and Java signatures support discovery and
local execution but do not independently own flow shape. Invalid step shapes, type links,
mapper pairs, connector declarations, placement, and generated-boundary requirements fail
at build time whenever possible.

Connectors are versioned typed external-boundary products. Binding selects a configured
provider identity; an operation selects one capability; invocation input carries dynamic
business data. Operation configuration cannot encode branching or become a second
pipeline language. Dynamic selection remains constrained to compiler-authorized typed
identities and preserves Query/Command semantics.

This decision governs connector SPI and providers under `framework/connectors`, compiler
generation in `framework/deployment`, and generated metadata under `META-INF/pipeline/`.

## Rationale

One generated model makes boundary behaviour discoverable to runtime, replay, telemetry,
security, and validation tooling.

## Consequences

- Mapper selection is pair-accurate and deterministic.
- Extend the SPI only for a reusable semantic capability with a stable lifecycle.
- One application's policy stays application code unless a real framework gap is proven.
