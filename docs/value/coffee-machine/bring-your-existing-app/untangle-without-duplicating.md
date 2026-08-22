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

TPF does not need a duplicate. It needs a controlled separation. Begin with the decision that can be named without its infrastructure: calculate eligibility, decide a reservation, validate a transition, choose a price. Keep the old script as the adapter while that decision is extracted and tested. The script may still obtain persistence facts or invoke legacy exceptions initially; the point is to move one responsibility at a time, not to create a parallel application.

The seam becomes clearer as inputs and outputs become typed. Instead of allowing a domain decision to open a session, call a client, and publish a message, the shell supplies the facts and receives an explicit outcome. Existing code can remain behind adapters until the team is ready to replace it. That reduces duplication because the old route still executes the new decision rather than reproducing it.

Behavioral comparison is the safeguard. Characterization tests, golden inputs and outputs, and shadow execution can capture what the script currently promises, including inconvenient edge cases. TPF compilation then adds another kind of evidence: mappings, connector declarations, and flow shape must be compatible. Neither form of evidence is sufficient alone; together they let a team change structure without guessing that behavior followed.

Some code will resist clean extraction. Stored procedures, lazy-loading assumptions, mutable session state, and broad exception conventions are not defects a framework can wish away. Treat them as explicit migration constraints. A temporary adapter with a named retirement decision is safer than a fake pure function that secretly calls the old world.

The trade-off is a period of indirection. The team pays for adapters and comparative tests in return for a core that can be reasoned about independently of transport and persistence. That is only worthwhile for a flow whose accumulated operational complexity has made the script costly to change.

## Trade-offs

TPF gains a gradual route to typed business behavior. It gives up the immediate satisfaction of a spotless rewrite. Teams must maintain temporary bridges and remove them intentionally.

## When TPF is not a good fit

If the script is small, local, and unlikely to acquire boundaries, extraction may be ceremony. Keep it simple rather than manufacturing a migration story.
