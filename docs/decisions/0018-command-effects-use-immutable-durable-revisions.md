---
title: Durable Command effects use immutable conditional revisions
status: accepted
---

# ADR-0018: Durable Command effects use immutable conditional revisions

## Context

Command is the authority for one logical external effect, but an in-process authority disappears
on restart. A production store must preserve successful replay, in-flight and unsafe-outcome
barriers, and deliberate retry attempts while preventing two runtimes from claiming the same
transition. Mutable upserts would make stale writers and partial attempt-history replacement harder
to reason about and would contradict the control-plane storage direction in ADR-0012.

## Decision

The first built-in durable `CommandEffectStore` uses DynamoDB and stores every effect state as an
immutable, monotonically numbered full revision under the tenant plus logical Command identity.
Creation and transitions conditionally create the expected revision; they do not update an existing
record or maintain a mutable current pointer. Reads of the latest authority are strongly consistent.

Each revision preserves the complete typed input/output snapshot, native outcome metadata, and
ordered attempt history. Store conflicts, connectivity failures, corrupt records, unsupported
schema versions, and type reconstruction failures remain store failures. They are never interpreted
as provider outcomes. Query capture remains a separate state authority.

## Rationale

Competing writers that observe the same revision necessarily contend for the same next key, giving
one conditional winner. Full immutable revisions also retain the exact evidence used for restart,
replay, and operator diagnosis without borrowing mutable legacy-store precedent.

## Consequences

- Successful output and non-success barriers survive process restart.
- Deliberate retry retains one logical effect identity and lossless attempt history.
- Provider selection and table provisioning are deployment concerns; authored pipelines and
  connector operations do not select storage.
- Large business content is carried by `PayloadReference` rather than expanding control-plane rows.
- Additional durable providers must satisfy the same effect-store conformance contract without
  merging Command authority with Query capture, persistence aspects, cache, or execution state.
