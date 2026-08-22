---
title: "What is the escape hatch when the model is wrong?"
faq:
  id: "escape-hatch"
  track: "governance"
  question: "What is the escape hatch when TPF’s model does not fit a critical use case?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "functional-fran"
      text: "No escape hatch; reality should refactor."
    - persona: "framework-fred"
      text: "Bypass the model now; explain it after launch."
    - persona: "consultant-nigel"
      text: "Every exception deserves a new framework."
social:
  poll:
    question: "Escape hatch?"
    options:
      - "Never"
      - "Everywhere"
      - "Explicit exception"
      - "A secret client"
    preferred: "Explicit exception"
fortune:
  quote: "An escape hatch is healthy when it remains a boundary, not when it becomes the building."
related:
- "startup-size"
- "enterprise-restriction"
tags:
- "governance"
- "escape"
- "hatch"
---

# What is the escape hatch when the model is wrong?

## Elevator answer

**Use explicit, reviewed integration at the boundary, preserve typed ownership and observability where possible, and treat the exception as evidence for model evolution—not a reason to bypass everything.**

<CoffeeMisconceptions />

## The real explanation

An opinionated framework without an escape hatch eventually asks teams to lie. Critical integrations, unusual protocols, legacy constraints, or a business model that does not fit may require code outside the preferred path. The honest response is not to smuggle it into a business step or declare that the framework is universally wrong.

Use an explicit boundary. Name the exception, keep external I/O and operational ownership visible, apply the supported telemetry and security controls where possible, and document what semantics differ. If the exception is temporary, give it a retirement condition. If it is recurring, treat it as design input for the framework or connector model.

The escape hatch must not become a second undocumented architecture. Frequent bypasses indicate either a poor fit or a missing extension point. In both cases, more silent shortcuts make the answer harder to discover.

## Trade-offs

TPF gains honesty at its limits. It gives up the comfort of universal purity. Teams must review exceptions carefully.

## When TPF is not a good fit

If most consequential flows require bypassing the model, stop. The framework is not the right center of gravity for that application.
