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
search: false
---

# Are generated artifacts reviewable or just weather?

## Elevator answer

**The same flow should generate the same handler, metadata, and bindings. If a one-line mapping change rewrites 4,000 unrelated lines, the generator has produced weather, not evidence.**

<CoffeeMisconceptions />

## The real explanation

Generated files are useful when a reviewer can point from a changed mapper or branch to the exact changed handler, metadata, or binding. Deterministic output makes that possible. A generator that rearranges everything on each run has replaced hand-written drift with machine-written fog.

Teams need not read every line on every pull request. They should be able to inspect a stable semantic diff when flow shape, mapping, transport, or runtime placement changes. Contract tests and compilation checks catch the rest. If generation creates endless irrelevant noise, the generator or review surface needs improvement—not lower expectations.

## Trade-offs

TPF gains reproducible artifacts. It gives up treating generated output as untouchable magic.

## When TPF is not a good fit

If a team cannot reproduce generation in CI, do not rely on generated artifacts as a contract.
