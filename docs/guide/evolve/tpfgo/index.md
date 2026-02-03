# TPFGo Reference (TPF-Inspired)

This guide is a lightweight index for a "TPFGo" reference implementation in TPF, a TPF-centric interpretation focused on checkpoint pipelines, immutable state progression, and workflow semantics.

## Reading order (recommended)

1) **TPF and DDD Alignment**
   - [TPF and DDD Alignment](/guide/evolve/tpfgo/ddd-alignment)
   - Mapping of DDD terms to TPF, plus the workflow shape and decision boundaries.

2) **Application Design Spectrum**
   - [Application Design Spectrum](/guide/evolve/tpfgo/design-spectrum)
   - What good vs risky designs look like, and which guardrails TPF can provide.

3) **Roadmap (Pessimist's Notebook)**
   - [Roadmap: Checkpoint Pipelines vs FTGO](/guide/evolve/tpfgo/roadmap)
   - Risks, open questions, and practical next steps.

## What this reference is aiming for

- **Checkpoint pipelines** that produce stable, consistent states.
- **Explicit workflow composition** without hidden branching behavior.
- **Operational clarity** (errors, retries, and handoffs are visible and intentional).
- **Adoption-friendly** paths (including slower JSON pipelines later).

If you are new to the conversation, start with the DDD alignment guide, then the design spectrum, and finish with the roadmap.
