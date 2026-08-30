---
title: Functional core and explicit typed dataflow
status: accepted
---

# ADR-0001: Functional core and explicit typed dataflow

## Context

Ordinary Java applications often let services reload known data and directly own
repositories, external clients, retries, and workflow state. In TPF this hides data
coupling and prevents the framework from owning boundary semantics.

## Decision

TPF uses a functional core with a framework-owned imperative shell. Authored business
steps are small transformations over explicit typed inputs. If the pipeline already
knows a fact, it carries that fact forward immutably instead of discarding and querying
it back. I/O, persistence, retries, correlation, replay, telemetry, transport, and
deployment integration stay in framework-owned boundaries.

This decision governs `framework/api`, `framework/runtime-core`,
`framework/deployment`, `examples`, and application authoring guidance.

## Rationale

Visible typed dataflow lets the compiler validate what each decision knows and lets the
runtime apply consistent durability, replay, and operational policy around pure domain
logic.

## Consequences

- Authored services do not inject infrastructure merely for convenience.
- Pipeline state may become richer because it preserves facts needed downstream.
- Simple local code need not become a pipeline when TPF semantics add no value.
