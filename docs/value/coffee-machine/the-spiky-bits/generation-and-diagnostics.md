---
title: "Does generation make builds slower and failures stranger?"
faq:
  id: "generation-and-diagnostics"
  track: "runtime"
  question: "Does code generation make builds slower and failures harder to diagnose?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "build-barry"
      text: "More generated files mean less evidence."
    - persona: "build-barry"
      text: "Production is the best compiler."
    - persona: "codegen-carl"
      text: "A stack trace is a user guide."
social:
  poll:
    question: "Build failure location?"
    options:
      - "Production"
      - "A pager"
      - "The contract"
      - "A mystery file"
    preferred: "The contract"
fortune:
  quote: "A slower build is a good trade when it prevents a slower incident."
related:
- "compiler-without-compiler-degree"
- "customization-without-forking"
tags:
- "runtime"
- "generation"
- "diagnostics"
---

# Does generation make builds slower and failures stranger?

## Elevator answer

**Generation costs build time. Its bargain is catching “no mapper from ApprovedOrder to ShipmentRequest” before a customer catches it at runtime—and saying exactly that.**

<CoffeeMisconceptions />

## The real explanation

Generation makes the build do more work. In return, a missing connector binding or incompatible mapper should fail next to the declaration, not as a `ClassCastException` after deployment. If diagnostics merely say “generation failed,” the framework has moved the mystery earlier without making it smaller.

TPF chooses to make those contracts build-time concerns. A useful diagnostic should name the pipeline, step, types, boundary, or transport requirement that is incompatible—not merely expose Java internals from a generated class. Generation earns its cost only when it makes the next action clearer.

The trade-off is build complexity. Keep generation deterministic, cacheable, and bounded; investigate diagnostics as a model problem before treating them as generated-code problems.

## Trade-offs

TPF gains earlier boundary failure. It gives up a completely simple build.

## When TPF is not a good fit

If a team cannot accept generated contract validation in its delivery loop, handwritten integration may be a more honest choice.
