---
title: Query capture uses immutable durable observation revisions
status: accepted
---

# ADR-0020: Query capture uses immutable durable observation revisions

## Context

Query owns the external observation used by one managed execution. An in-memory capture can replay
within one process but loses that authority on restart and cannot coordinate replicas. Finite
streaming Query also needs to stage rows without making a partial prefix authoritative.

Generic cache, Command effects, execution state, and Query capture have different identities and
failure semantics. Reusing one of those stores would merge authorities rather than make Query
durable. Mutable DynamoDB head records would also make crash and conditional-race history harder to
audit.

## Decision

Durable Query capture uses one DynamoDB partition per execution-scoped capture identity and an
append-only numeric revision sequence. A strongly consistent read finds the latest authority event;
competing transitions conditionally create the same next revision, so only one wins.

Unary `Found` and `NotFound` observations commit in one revision, including optional bounded
provider observation metadata stored separately from the application output. Streaming capture appends a writer
lease and ordered item revisions, then makes exactly one generation visible with a terminal commit.
Abort, cancellation, expiry, and tombstones are also immutable revisions. Replay reads only the
committed generation and preserves downstream demand.

The durable representation stores typed canonical output, optional provider-neutral observation
metadata, and only a fingerprint of Query input. Observation metadata is not part of capture identity.
Provider configuration, credentials, SDK objects, runtime handles, prompts, and hidden reasoning do
not belong to the observation record.

## Rationale

The revision race is the authority decision and requires neither an upsert nor a mutable pointer.
It provides restart-safe history for unary outcomes and a clear atomic boundary for a finite stream
while leaving generic cache policy and Command effect semantics unchanged.

## Consequences

- A concurrent unary miss can call the provider more than once before one capture wins; every replay
  afterwards uses the winner. Each completed live observation is recorded before arbitration, while
  decoding the winning record does not relabel the just-completed call as replay.
- An incomplete streaming generation is never replayed. An expired lease permits a new generation
  to observe the provider again.
- DynamoDB table provisioning, IAM, retention, and maintenance clear remain deployment concerns.
- The in-memory store remains the default and intentionally provides no restart guarantee.
