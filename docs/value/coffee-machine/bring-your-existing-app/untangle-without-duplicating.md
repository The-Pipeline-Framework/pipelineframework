---
title: "How do we untangle a transaction script without cloning the business?"
faq:
  id: "untangle-without-duplicating"
  track: "bring-your-existing-app"
  question: "Does extracting pure business logic mean duplicating existing code?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "functional-fran"
      text: "Copy the method, delete the clients, and call the remaining compilation errors purity."
    - persona: "enterprise-edna"
      text: "Create an abstraction layer for every line before deciding what the line means."
    - persona: "jpa-jane"
      text: "The transaction script is the domain model because it has the most annotations."
social:
  poll:
    question: "What is the first extraction?"
    options:
      - "Every dependency at once"
      - "A new copy of the service"
      - "Typed flow contract"
      - "The entire database schema"
    preferred: "Typed flow contract"
fortune:
  quote: "Extraction is not duplication when the old path begins calling the new decision."
related:
- "jpa-is-not-a-crime"
- "prove-the-promise"
tags:
- "bring-your-existing-app"
- "untangle"
- "duplicating"
---

# How do we untangle a transaction script without cloning the business?

## Elevator answer

**No. Extract decisions gradually from their infrastructure dependencies, keep adapters temporary where needed, and prove behavior before retiring the old transaction script.**

<CoffeeMisconceptions />

## The real explanation

Legacy transaction scripts often combine several responsibilities because that was the shortest path to a working feature. They load data, apply rules, call remote clients, persist changes, translate exceptions, publish messages, and return an HTTP-shaped response. Asking a team to “extract the pure core” can sound like an instruction to rewrite the same behavior twice: once in the old service and once in a new idealized function.

TPF does not need a duplicate. It needs a controlled separation. Begin with the decision that can be named without its infrastructure: calculate eligibility, decide a reservation, validate a transition, choose a price. Keep the old script as the adapter while that decision is extracted and tested. The point is to move one responsibility at a time, with the old path calling the new decision, not to create a parallel application.

The seam becomes clearer when each dependency is classified by what it means:

- Data the flow already knows moves forward through typed, immutable propagation.
- Large content travels by `PayloadReference` and an explicit representation rather than bloating every step contract.
- A fresh or historical external observation becomes a Query.
- An external effect becomes a Command with explicit execution and replay safety.
- A durable wait for a person, callback, provider, or long-running job becomes an Await.
- Persistence, capture, cache, and replay belong to declared aspects or runtime authorities rather than hiding inside the business decision.

This classification avoids both extremes: a supposedly pure function that secretly reaches into the old world, and a new abstraction for every legacy method call. Existing repositories, clients, and publishers can stay behind the appropriate boundary until the team is ready to replace them.

Behavioral comparison is the safeguard. Characterization tests, golden inputs and outputs, captured Queries, and safe shadow execution can preserve what the script currently promises, including inconvenient edge cases. TPF compilation then adds another kind of evidence: mappings, connector declarations, representations, and flow shape must be compatible. Neither form of evidence is sufficient alone; together they let a team change structure without guessing that behavior followed.

Some code will resist clean extraction. Stored procedures, lazy-loading assumptions, mutable session state, and broad exception conventions are not defects a framework can wish away. Treat them as explicit migration constraints and choose the nearest honest boundary. A temporary adapter with a named retirement decision is safer than a fake pure function that secretly calls the old world.

The trade-off is a period of indirection. The team pays for adapters and comparative tests in return for a core that can be reasoned about independently of transport and persistence. That is only worthwhile for a flow whose accumulated operational complexity has made the script costly to change.

## Trade-offs

TPF gains a gradual route to typed business behavior and explicit operational boundaries. It gives up the immediate satisfaction of a spotless rewrite. Teams must maintain temporary bridges, classify them honestly, and remove them intentionally.

## When TPF is not a good fit

If the script is small, local, and unlikely to acquire boundaries, extraction may be ceremony. Keep it simple rather than manufacturing a migration story.
