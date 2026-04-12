---
search: false
---

# TPFGo Reference (TPF-Inspired)

This guide is a lightweight index for a "TPFGo" reference implementation in TPF, a TPF-centric interpretation focused on checkpoint pipelines, immutable state progression, and workflow semantics.

Current completion scope:
- TPFGo completion scope is SYNC-path business/workflow and transport-contract scope.
- Queue/HA delivery (`QUEUE_ASYNC`, durable providers) is tracked in a separate epic and is not a TPFGo merge blocker.

## Reading order (recommended)

1) **TPF and DDD Alignment**
   - [TPF and DDD Alignment](/versions/v26.2.5/guide/evolve/tpfgo/ddd-alignment)
   - Mapping of DDD terms to TPF, plus the workflow shape and decision boundaries.

2) **Application Design Spectrum**
   - [Application Design Spectrum](/versions/v26.2.5/guide/evolve/tpfgo/design-spectrum)
   - What good vs risky designs look like, and which guardrails TPF can provide.

3) **Roadmap (Pessimist's Notebook)**
   - [Roadmap: Checkpoint Pipelines vs FTGO](/versions/v26.2.5/guide/evolve/tpfgo/roadmap)
   - Risks, open questions, and practical next steps.

4) **Observer/Tap Contract (Diagnostics-First)**
   - [Observer and Tap Contract](/versions/v26.2.5/guide/evolve/tpfgo/observer-tap-contract)
   - Contract scope, expected diagnostics, and test-only guardrails for the current scope.

## What this reference is aiming for

- **Checkpoint pipelines** that produce stable, consistent states.
- **Explicit workflow composition** without hidden branching behavior.
- **Operational clarity** (errors, retries, and handoffs are visible and intentional).
- **Adoption-friendly** paths (including slower JSON pipelines later).

## Core terms used in this guide

- **Connector**: the runtime handoff boundary between pipelines/contexts.
  In TPF v1 the runtime boundary is enabled by a first-class framework model: YAML declaration, build-time validation, and generated startup wiring (runtime initialization code produced from the YAML declaration).
  `Bridge` remains the compatibility label for manual application beans that replace that generated Connector lifecycle.
  Responsibilities:
  - idempotency/dedup
  - backpressure policy
  - retry/failure routing
  - lineage continuity
- **Tap**: a non-primary observer branch attached to step output.
  - **Checkpoint observer**: observes stable, persisted outputs.
  - **Mid-step tap**: observes transient outputs with weaker persistence/durability guarantees.

If you are new to the conversation, start with the DDD alignment guide, then the design spectrum, and finish with the roadmap.
