---
title: Await owns durable suspension
status: accepted
---

# ADR-0007: Await owns durable suspension

## Context

Human approval, callbacks, brokered replies, and long-running provider jobs complete
after the current invocation and must survive retries, duplicates, and deployment change.

## Decision

Await is a durable suspension boundary, not a delayed function call. Await owns the
stored request, correlation, completion admission, timeout, duplicate completion,
projection, and resume semantics independently of transport. The exact suspended request
is trusted context; submitted human/browser/provider completion is untrusted observation.
Framework projection combines stored request, submitted response, and interaction
metadata into the canonical output without asking the submitter to echo authority.

Projection failure does not consume or corrupt the durable interaction. Surviving
interactions remain pinned to their release/contract interpretation. This decision
governs `framework/runtime-core` Await contracts, `framework/runtime` Await coordination,
Await stores, adapters, and generated Await boundaries.

## Rationale

Transport adapters may vary, but durable interaction correctness must not vary with them.

## Consequences

- Await storage is not application business persistence or generic cache.
- An ordinary cache hit must not replay a prior human choice as a new completion.
- Applications do not build parallel polling tables or workflow registries.
