---
title: Change the semantic owner boldly
status: accepted
---

# ADR-0013: Change the semantic owner boldly

## Context

High dependency count, churn, or centrality can pressure framework work toward sibling
helpers, facades, codecs, adapters, or parallel execution paths that touch fewer files
but leave two owners for one concept.

## Decision

Identify the semantic owner first, design the coherent change there, then inspect impact
and satisfy compatibility/testing obligations. Dependency count, churn, and centrality
determine review and validation rigor; they do not decide where semantics may live.

Prefer changing the owning abstraction cleanly, including a breaking change when
appropriate, over preserving accidental structure through additive compatibility layers.
Additive change is not automatically safer. Preserve compatibility when an actual promise
exists; otherwise prefer simplification, semantic consolidation, migration, and deletion.

Migration transfers responsibility. When TPF gains a native boundary, remove the
application registry, client, retry ledger, duplicated configuration, glue, and obsolete
tests it replaces. This decision governs framework-wide changes, `framework/api`,
`framework/runtime-core`, `framework/runtime*`, `framework/deployment`, connectors,
plugins, examples, and migrations.

## Rationale

Avoiding the owning abstraction can reduce a diff while increasing permanent conceptual
surface and compatibility burden.

## Consequences

- Before adding a sidecar, ask whether it only avoids migration or test work.
- A migration that adds a second path and removes nothing requires explicit justification.
- Impact analysis remains mandatory, but it serves coherent change rather than fear.
