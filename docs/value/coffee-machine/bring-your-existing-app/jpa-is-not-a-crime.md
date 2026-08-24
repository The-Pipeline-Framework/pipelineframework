---
title: "Will JPA become a forbidden relic?"
faq:
  id: "jpa-is-not-a-crime"
  track: "bring-your-existing-app"
  question: "What happens to our existing JPA repositories and lazy-loading behavior?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "jpa-jane"
      text: "A lazy proxy is a domain concept because it appears at runtime."
    - persona: "functional-fran"
      text: "Ban persistence immediately; the database will understand the gesture."
    - persona: "enterprise-edna"
      text: "Add one base repository and every aggregate boundary will heal."
social:
  poll:
    question: "What should a business step receive?"
    options:
      - "An open session and optimism"
      - "Every table in the schema"
      - "Typed flow contract"
      - "A proxy with a surprise query"
    preferred: "Typed flow contract"
fortune:
  quote: "Lazy loading is not evil; it is simply a very expensive way to keep a boundary implicit."
related:
- "untangle-without-duplicating"
- "prove-the-promise"
tags:
- "bring-your-existing-app"
- "jpa"
- "crime"
---

# Will JPA become a forbidden relic?

## Elevator answer

**Keep the repository. Stop making an open Hibernate session the secret co-author of the business flow. Carry known facts forward; use a Query when you genuinely need a new database observation.**

<CoffeeMisconceptions />

## The real explanation

Imagine step B has loaded a customer and already knows the delivery address. Step X needs that address. Do not throw it away, carry the customer ID for six steps, and let X heroically fetch the address from PostgreSQL six milliseconds later. Carry the bloody address.

That is ordinary immutable dataflow: X receives exactly what this execution knew, the dependency is visible in its input type, and no lazy proxy gets to place a surprise query during a retry. If the flow really needs a fresh fact—“what is the customer's current credit limit?”—make that lookup a Query. The Query adapter may use the same Spring Data repository you have today. JPA has not been excommunicated; it has been given a visible job.

Do not “fix” lazy loading by eager-fetching the known universe or inventing a shadow DTO empire. Pick one flow, watch which SQL it actually issues, carry facts that are already in hand, and query only when time or external state makes a new observation necessary. A PDF or other large immutable value should travel as a `PayloadReference` with an explicit representation provider, not as a 40 MB field copied through every step.

This distinction matters when something retries. A lazy getter can observe a different row on the second attempt. Carried data preserves what that execution knew; Query capture can preserve the observation a decision used when replay policy says to reuse it. Neither freezes the database forever. They merely stop “whatever Hibernate returned this time” from masquerading as deterministic business input.

The bill arrives as visible query design. You may need batching, a read model, or a smaller Query result. That is less convenient than an open session and considerably easier to reason about at 03:00.

## Trade-offs

TPF gains reproducible inputs and visible database observations. It gives up invisible lazy convenience, so query shape and performance become deliberate work.

## When TPF is not a good fit

If a local transaction is simple and lazy loading is well-controlled, a migration may not earn its cost. Do not treat JPA as a moral failure; treat unowned persistence behavior as a design question.
