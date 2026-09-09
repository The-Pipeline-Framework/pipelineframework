---
title: "Does integration testing now own my calendar?"
faq:
  id: "test-pyramid-not-test-panic"
  track: "testing"
  question: "Does TPF make integration tests more important than unit tests?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "test-terry"
      text: "Only production-like tests count."
    - persona: "mock-molly"
      text: "Mocks can represent a lost payment response."
    - persona: "build-barry"
      text: "A four-hour suite is confidence."
social:
  poll:
    question: "Best test mix?"
    options:
      - "Only E2E"
      - "Only mocks"
      - "Layered evidence"
      - "One long suite"
    preferred: "Layered evidence"
fortune:
  quote: "The right test is the cheapest one that can still falsify the claim."
related:
- "test-without-booting-the-planet"
- "connector-contract-tests"
tags:
- "testing"
- "test"
- "pyramid"
- "panic"
---

# Does integration testing now own my calendar?

## Elevator answer

**No. Unit-test the rule, compile-check the flow, contract-test the connector, and boot real infrastructure only for the boundary behavior a mock would lie about.**

<CoffeeMisconceptions />

## The real explanation

Use the cheapest honest test. A pricing rule needs direct Java. An incompatible mapper type or shape should fail contract compilation; a mapper that compiles while putting `billingAddress` into `shippingAddress` needs a dedicated mapper test. A Stripe connector needs protocol-shaped tests. A durable Await or real Kafka acknowledgement may justify focused infrastructure. Booting PostgreSQL, Kafka, and half the company to test `isEligible()` is test panic with containers.

This is not a pyramid argument by geometry. It is an argument by failure mode. Test the cheap deterministic behavior cheaply; test the risky boundary where it actually exists; avoid booting infrastructure for a rule that is just arithmetic. A small number of high-value end-to-end paths can then prove topology without becoming the only feedback mechanism.

The trade-off is a deliberate test portfolio. Teams must resist using one layer as an excuse to skip another.

## Trade-offs

TPF gains proportional evidence. It gives up a single “golden” test type.

## When TPF is not a good fit

If test environments are so unreliable that focused integration tests cannot run, improve that capability before depending on distributed execution claims.
