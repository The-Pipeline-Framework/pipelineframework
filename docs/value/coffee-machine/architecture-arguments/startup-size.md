---
title: "Is TPF too much framework for a small startup?"
faq:
  id: "startup-size"
  track: "governance"
  question: "Is the abstraction too complex for a small startup?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "framework-fred"
      text: "Every prototype needs a compiler phase."
    - persona: "consultant-nigel"
      text: "Architecture is what happens after Series B."
    - persona: "consultant-nigel"
      text: "Complexity is credibility with diagrams."
social:
  poll:
    question: "Startup default?"
    options:
      - "Framework first"
      - "Diagram first"
      - "Keep it simple"
      - "Hire a council"
    preferred: "Keep it simple"
fortune:
  quote: "A framework is useful when it removes the complexity you already have, not when it introduces a more impressive kind."
related:
- "escape-hatch"
- "not-everything-is-a-pipeline"
tags:
- "governance"
- "startup"
- "size"
---

# Is TPF too much framework for a small startup?

## Elevator answer

**Often, yes. If the product is three CRUD screens and one Stripe call, ship it. Reconsider TPF when retries, callbacks, duplicate effects, and hand-built adapters become a recurring tax.**

<CoffeeMisconceptions />

## The real explanation

A small startup should be suspicious of any framework that demands a new language before it has a costly problem. A local CRUD feature, simple integration, or quickly changing hypothesis may be clearer as ordinary framework code. TPF is not a maturity badge.

It becomes useful when the same path begins collecting transport logic, mapping glue, retries, external effects, durable waits, or operational ambiguity. At that point the team is already paying architecture cost; it is merely paying it in scattered code and late failures. A typed flow can reduce that cost by making the consequential path and its boundaries explicit.

The right adoption is narrow. Pick one flow that hurts, prove that the contract removes more complexity than it adds, and keep the rest simple. If the framework’s model is more complex than the business behavior and boundaries it describes, do not use it yet.

## Trade-offs

TPF gains discipline for consequential flows. It gives up the fastest possible first implementation.

## When TPF is not a good fit

When the product is still discovering its basic behavior, or all work is local and reversible, ordinary code is usually cheaper.
