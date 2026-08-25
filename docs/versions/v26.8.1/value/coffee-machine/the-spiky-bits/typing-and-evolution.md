---
title: "Does strong typing make change slower?"
faq:
  id: "typing-and-evolution"
  track: "runtime"
  question: "Does strong typing make schema evolution slower?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "ddd-diego"
      text: "JSON is backward compatible if nobody looks."
    - persona: "ddd-diego"
      text: "A compiler error is a product veto."
    - persona: "kafka-frank"
      text: "Versioning is a topic suffix."
social:
  poll:
    question: "Typing finds change…"
    options:
      - "Later"
      - "Never"
      - "Earlier"
      - "In production"
    preferred: "Earlier"
fortune:
  quote: "Strong typing does not remove evolution; it removes the ability to postpone admitting it."
related:
- "customization-without-forking"
- "connector-governance"
tags:
- "runtime"
- "typing"
- "evolution"
search: false
---

# Does strong typing make change slower?

## Elevator answer

**Changing `amount` from cents to decimal should break a mapper in the build, not silently reinterpret €12.34 as €1,234 in a consumer. Types expose the argument; versioning resolves it.**

<CoffeeMisconceptions />

## The real explanation

Rename `customerId`, split `address`, or change money from integer cents to decimal and somebody must adapt. Untyped payloads postpone the argument until a consumer guesses wrong in production. Strong typing moves it into a broken mapper or incompatible binding where the team can version, map, and roll out deliberately.

That is not slowness for its own sake. It gives a team a deliberate place to add a compatible representation, version a public boundary, or stage a migration. TPF’s pair-accurate mapper selection and generated contracts should make ambiguity explicit rather than silently choosing a convenient conversion.

The trade-off is visible work at change time. It is usually cheaper than discovering a schema change through a downstream failure whose source is no longer obvious.

## Trade-offs

TPF gains earlier evolution feedback. It gives up invisible compatibility assumptions.

## When TPF is not a good fit

If contracts are intentionally loose and consumers cannot be identified, stronger typing may not match the integration style.
