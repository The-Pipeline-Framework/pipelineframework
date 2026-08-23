---
title: Runtime, deployment, configuration, and telemetry are separate dimensions
status: accepted
---

# ADR-0011: Runtime, deployment, configuration, and telemetry are separate dimensions

## Context

Runtime placement, physical packaging, invocation transport, deployment platform, wire
encoding, configuration lifetime, and observability are related but not interchangeable.

## Decision

Runtime layout is logical placement; build topology is physical JAR/module/container
arrangement; transport is `LOCAL`, `REST`, or `GRPC`; platform and wire/envelope protocol
are separate dimensions. Authors declare placement intent through the supported runtime
mapping model; the compiler validates and emits authoritative placement/call metadata.
Runtime mapping does not rewrite Maven topology or change canonical meaning.

Applications start with the simplest supported deployment unit and split only for a real
scale, security, failure-isolation, ownership, or platform need. Portable semantics belong
in `pipeline.yaml`; build-derived capability changes require regeneration; framework
policy/providers belong in runtime/deployment configuration; changing business facts are
typed input.

Telemetry boundary identity and available instrumentation are build-produced. Sampling,
enablement, exporters, backends, and operational thresholds are runtime/deployment policy.
This decision governs `framework/deployment`, runtime mapping, runtime configuration,
telemetry in `framework/runtime*`, packaging, and `docs/deploy`.

## Rationale

Separating these dimensions lets deployment evolve without rewriting business semantics.

## Consequences

- A layout flag alone does not create or merge deployable artifacts.
- Prefer global defaults and exact generated-boundary overrides for real exceptions.
- Release identity and generated semantic contracts pin execution interpretation.
