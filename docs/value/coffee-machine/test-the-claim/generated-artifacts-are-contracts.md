---
title: "Are generated artifacts reviewable or just weather?"
faq:
  id: "generated-artifacts-are-contracts"
  track: "testing"
  question: "Are generated artifacts stable enough to review in pull requests?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "codegen-carl"
      text: "Generated means nobody owns it."
    - persona: "build-barry"
      text: "A noisy diff is a sign of innovation."
    - persona: "codegen-carl"
      text: "Never inspect generated behavior."
social:
  poll:
    question: "Generated code is…"
    options:
      - "Weather"
      - "Unreviewable"
      - "A contract"
      - "Someone else’s job"
    preferred: "A contract"
fortune:
  quote: "Generated does not mean unowned; it means the source contract deserves more attention."
related:
- "connector-contract-tests"
- "test-ordering-and-cardinality"
tags:
- "testing"
- "generated"
- "artifacts"
- "contracts"
---

# Are generated artifacts reviewable or just weather?

## Elevator answer

**Generated artifacts are part of the contract: keep them deterministic, validate them from the pipeline model, and review meaningful semantic changes rather than accepting noisy output.**

<CoffeeMisconceptions />

## The real explanation

Generated code and metadata are useful only when they are traceable to a declared model. TPF treats pipeline order, telemetry, branching, platform descriptions, and semantic contracts as generated artifacts that must not drift from the flow. Determinism turns them from weather into reviewable evidence.

Teams need not read every line on every pull request. They should be able to inspect a stable semantic diff when flow shape, mapping, transport, or runtime placement changes. Contract tests and compilation checks catch the rest. If generation creates endless irrelevant noise, the generator or review surface needs improvement—not lower expectations.

## Trade-offs

TPF gains reproducible artifacts. It gives up treating generated output as untouchable magic.

## When TPF is not a good fit

If a team cannot reproduce generation in CI, do not rely on generated artifacts as a contract.
