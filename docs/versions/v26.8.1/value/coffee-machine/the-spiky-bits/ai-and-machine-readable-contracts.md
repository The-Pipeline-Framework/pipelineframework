---
title: "Can AI understand a generated contract?"
faq:
  id: "ai-and-machine-readable-contracts"
  track: "ai"
  question: "Can TPF provide machine-readable constraints to coding assistants?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "ai-ada"
      text: "The model has read the repository spiritually."
    - persona: "ai-ada"
      text: "A longer prompt is a type system."
    - persona: "codegen-carl"
      text: "Generated code reviews itself."
social:
  poll:
    question: "AI needs…"
    options:
      - "More confidence"
      - "Less context"
      - "Constraints"
      - "A production key"
    preferred: "Constraints"
fortune:
  quote: "Machine-readable contracts make an assistant more constrained, not more authoritative."
related:
- "ai-and-boundary-safety"
- "generation-and-diagnostics"
tags:
- "ai"
- "machine"
- "readable"
- "contracts"
search: false
---

# Can AI understand a generated contract?

## Elevator answer

**Yes. An assistant can inspect the step types, mappings, connector operations, and generated metadata instead of guessing from the nearest client call. Humans still decide what the boundary means.**

<CoffeeMisconceptions />

## The real explanation

Without evidence, an assistant copies the pattern three files away—even if that file is the historical mistake. A typed flow can show that `RiskRequest` maps to a declared connector operation and returns `RiskObservation`; compiler diagnostics can reject the wrong shortcut. This narrows the assistant's guessing. It does not outsource the decision that Risk is an external observation.

It is not authority. A machine-readable contract can say which connector is declared or which mapper fits; it cannot decide whether a business boundary is wise, whether a new side effect is acceptable, or whether a migration preserves a customer promise. Humans still own that judgment.

The trade-off is useful constraint instead of unrestricted generation. That is a feature when the assistant is changing code that crosses real operational boundaries.

## Trade-offs

TPF gains inspectable constraints for tools. It gives up the fantasy that AI can safely infer every rule.

## When TPF is not a good fit

If a team expects AI to replace architectural review, no framework metadata will make that safe.
