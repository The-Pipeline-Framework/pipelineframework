---
title: Separate state and replay authorities
status: accepted
---

# ADR-0008: Separate state and replay authorities

## Context

TPF uses several durable state surfaces. Co-locating them in one database can tempt an
application to collapse their identities and infer authority from the wrong record.

## Decision

Each state surface has one authority:

- typed pipeline state owns facts known by the current computation;
- persistence aspects own durable business values/history at applicable typed boundaries;
- generic cache owns versioned pipeline/step result reuse;
- Query capture owns replay of external observations;
- `CommandEffectStore` owns logical effect identity and recorded outcome;
- Await storage owns suspended interaction and completion/resume;
- execution state owns runtime progress, dispatch, retry, and lifecycle.

Shared physical storage does not merge these semantics. Persistence remains a pass-through
aspect and observes only applicable typed branch values. Cache keys include semantic
input, target, release/version, and expected output rather than a broad business ID.
This decision governs `framework/plugins`, state stores in `framework/runtime`, and
generated aspect/cache/capture/effect metadata.

## Rationale

Distinct authorities make replay, idempotency, audit, and recovery claims precise.

## Consequences

- A cache hit is not a live observation, effect authorization, Await completion, or
  durable business record.
- Before/after persistence observes complementary boundaries but does not imply event sourcing.
- One database may host several authorities when schemas and identities remain separate.
