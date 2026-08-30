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
search: false
---

# What is the escape hatch when the model is wrong?

## Elevator answer

**Use the escape hatch in daylight: isolate the odd protocol behind a typed adapter, name its owner, keep telemetry, and record why the normal connector path cannot do the job.**

<CoffeeMisconceptions />

## The real explanation

An opinionated framework without an escape hatch eventually asks teams to lie. Critical integrations, unusual protocols, legacy constraints, or a business model that does not fit may require code outside the preferred path. The honest response is not to smuggle it into a business step or declare that the framework is universally wrong.

First classify the requirement: is it dataflow, representation, observation, effect, suspension, persistence, replay, routing, or something the framework genuinely does not model? That question often reveals an existing boundary before an exception is needed.

Put the odd mainframe protocol behind `LegacyClaimsAdapter`, not inside `DecideClaim`. Keep its I/O, credentials, telemetry, and owner visible; document which retry or replay guarantees it cannot provide. Give a temporary exception a deletion condition. If five teams need the same escape hatch, it is no longer an exception—it is product research.

The escape hatch must not become a second undocumented architecture. Frequent bypasses indicate either a poor fit or a missing extension point. In both cases, more silent shortcuts make the answer harder to discover.

## Trade-offs

TPF gains honesty at its limits. It gives up the comfort of universal purity. Teams must review exceptions carefully.

## When TPF is not a good fit

If most consequential flows require bypassing the model, stop. The framework is not the right center of gravity for that application.
