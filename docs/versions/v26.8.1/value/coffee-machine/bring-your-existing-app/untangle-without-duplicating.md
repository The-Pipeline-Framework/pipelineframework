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
search: false
---

# How do we untangle a transaction script without cloning the business?

## Elevator answer

**No. Make the old service call one newly extracted decision, then move reads, effects, and waits behind the right TPF seams. If both implementations keep deciding, you have duplicated the business.**

<CoffeeMisconceptions />

## The real explanation

`InvoiceService.process()` loads three entities, checks eligibility, renders a PDF, calls the payment provider, saves status, publishes Kafka, translates exceptions, and returns an HTTP-shaped result. It became a transaction script because shipping the feature was more urgent than satisfying a diagram. Fair enough.

Do not copy it into `PureInvoiceServiceAdapterFactory`. Moving the repository call to a class with a longer name has not purified anything. It has given the repository call a longer commute.

Start with one decision—calculate eligibility, choose a price, validate a transition—and make the old service call it. There is now one implementation of that rule. Then classify the dependencies around it by what they actually do:

- If an earlier step already knows the customer address, carry it immutably. Do not discard it so a later step can rediscover PostgreSQL.
- Let a large immutable PDF travel by `PayloadReference` and an explicit representation provider, not as a byte array in every record.
- Use a Query to observe the current account balance or exchange rate.
- Use a Command to charge the card, send the email, or publish the record.
- Use an Await when a human click, callback, or long-running provider must suspend the flow durably.
- Put persistence, capture, cache, and replay policy in declared aspects or runtime authorities, not inside `calculateEligibility()`.

Existing repositories and clients can sit behind those seams while the old service still coordinates them. Characterization tests and safe comparison runs protect the inconvenient edge cases; compilation checks the new mappings and flow shape. Each successful extraction should delete a rule, retry loop, mapper, or orchestration branch from the old script. Otherwise the team is not untangling it—it is growing a second tangle.

Stored procedures, lazy sessions, and broad exception conventions may resist clean extraction. Name them as migration constraints and choose the nearest honest seam. A temporary ugly adapter with a retirement condition is safer than a beautiful “pure” function with a concealed `EntityManager`.

## Trade-offs

TPF gains one gradually extracted business flow rather than a parallel rewrite. The cost is temporary adapters, comparison tests, and deliberate deletion from the old script.

## When TPF is not a good fit

If the script is small, local, and unlikely to acquire boundaries, extraction may be ceremony. Keep it simple rather than manufacturing a migration story.
