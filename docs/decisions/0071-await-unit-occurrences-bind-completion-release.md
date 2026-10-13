---
title: Await unit occurrences bind completion release
status: draft
---

# ADR-0071: Await unit occurrences bind completion release

## Context

An immutable first admission and an immutable parent-release acknowledgement do
not establish causal coverage if both refer to an authored Await unit that can
be revisited. Intentional Command reissue reopens an execution from its retained
initial input. A preceding Await can therefore admit another interaction while
the previous cycle's aggregate and release acknowledgement remain terminal.
Clocks, current membership, item indices and execution versions cannot supply
the missing relation between that admission and the earlier release.

## Decision

The portable Await unit contract owns one exact semantic occurrence identity.
Its complete unit ID includes tenant, execution, page scope, authored step and
index, and a cause retained by the execution owner before external admission.
Existing unit-ID fields carry that same identity through interactions, immutable
completion admissions, worker suspensions, item consequences, parent waiting
state and atomic release acknowledgements. There is no parallel current-unit
representation from which an observer may infer coverage.

The execution store retains an initial cause for new executions. A successful
authorized `REISSUE_COMMAND` CAS retains a fresh cause bound to that admitted
execution version; retries, ordinary replay and page advancement preserve it.
Command logical, effect-occurrence and attempt identities remain governed by
[ADR-0025](./0025-command-reissue-uses-occurrence-identity.md).

An occurrence is sealed against new members after dispatch or terminal settlement.
Same-cycle deduplication returns the exact existing interaction. Completed item
facts identify the admitted interaction, while continuation facts remain scoped
to its occurrence and item. Parent suspension must match the retained cause and
page; release atomically acknowledges that exact waiting occurrence. A later
cycle retains its own aggregate and acknowledgement without changing the first.

The read owner observes historical occurrence coverage independently of the
execution's current cause. It checks exact admission, unit, release and consequence
evidence before reporting release or retiring outstanding work. Negative clocks
prevent a latency sample, not causal identity. Legacy unqualified units remain
readable for their existing processing but have UNKNOWN completion coverage;
neither backfill nor an empty query may invent historical identity or time.

## Rationale

One occurrence-qualified unit keeps aggregation and release in their existing
semantic owners. A per-item release transaction cannot scale beyond transaction
limits; receipts appended after an unrelated ACK merely repeat the defect.
An immutable membership manifest would require another sealing and recovery
protocol. The exact unit occurrence already provides the shared causal boundary.

## Consequences

- `pipelineframework-contracts` owns the occurrence representation, retained
  execution cause and signed transition command fields. Native runtime stores,
  worker context propagation, imports and parent CAS implement those contracts.
- Causeful parents reject unqualified or foreign worker suspensions. Retained
  legacy parents preserve legacy processing; they are not silently upgraded.
- Both memory and Dynamo providers must preserve cause through every transition
  and reject identity conflicts without overwriting retained state.
- Unit membership fencing does not make interaction creation and unit sealing one
  cross-store transaction. Rejected in-flight creation can leave orphan work; local
  seal tests do not establish its cleanup liveness. Whole-unit terminal failure
  settles that failed occurrence without proving successful item continuation.
- Matching released contracts, workers, stores and codecs are required. Legacy
  wire input lacks occurrence authority; mixed-version operation requires explicit
  compatibility and migration evidence.
- This decision does not certify production support. The
  [completion audit](/evolve/durable-coordinator/aws-durable-production-plan)
  records local execution, migration and live-provider acceptance gaps.
