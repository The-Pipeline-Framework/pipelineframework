---
title: Query is external observation
status: accepted
---

# ADR-0005: Query is external observation

## Context

External reads, database lookups, model inference, and provider observations need replay
and trust semantics that an ordinary transformation or injected client cannot provide.

## Decision

Query represents one genuinely new, current, or historical external observation. If a
fact is already known upstream, carry it instead. Query capture is the authority for
replaying that observation and is distinct from generic pipeline-result cache.

Trusted identifiers, permissions, policy, and prior context remain framework/application
owned. Model, browser, and provider output is untrusted new input. One LLM Query performs
one inference unless the pipeline explicitly models another turn. Required structured
output means actual provider/schema enforcement; fallback and malformed-output handling
are explicit rather than hidden repair inference.

This decision governs `framework/runtime-core` Query contracts,
`framework/runtime/src/main/java/org/pipelineframework/query`, and Query connectors.

## Rationale

Making observation explicit gives replay a precise meaning and prevents external systems
from being trusted to reproduce authoritative application context.

## Consequences

- Query is not a generic data-coupling escape hatch.
- Provider-specific tuning stays below portable Query semantics.
- Query capture identity must distinguish the semantic observation and release/version.
