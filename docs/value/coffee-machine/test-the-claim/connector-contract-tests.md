---
title: "Can a fake connector tell the truth?"
faq:
  id: "connector-contract-tests"
  track: "testing"
  question: "How do contract tests apply to connectors and generated adapters?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "mock-molly"
      text: "A stubbed 200 proves the partner integration."
    - persona: "kafka-frank"
      text: "The schema registry is the whole contract."
    - persona: "platform-priya"
      text: "Credentials are configuration, not behavior."
social:
  poll:
    question: "A connector test proves…"
    options:
      - "A mock agrees"
      - "A class compiles"
      - "Boundary behavior"
      - "The network exists"
    preferred: "Boundary behavior"
fortune:
  quote: "A connector contract is where a nice type meets an impolite real system."
related:
- "test-without-booting-the-planet"
- "test-uncertain-effects"
tags:
- "testing"
- "connector"
- "contract"
- "tests"
---

# Can a fake connector tell the truth?

## Elevator answer

**Send the connector a real-shaped request and make the fake provider misbehave: 401, 429, timeout after success, duplicate callback, malformed body. “Returns 200” is not a contract suite.**

<CoffeeMisconceptions />

## The real explanation

A connector test need not boot the whole application, but `when(client.call()).thenReturn(ok)` proves very little about Stripe, SAP, or Kafka. Exercise the actual mapping, auth headers, idempotency key, timeout classification, retryable status codes, duplicate delivery, and the ugly payload the provider really sends.

Generated adapters should be included where they implement the boundary. The test then proves that the compiler model, generated code, and actual connector agree. Use a provider test environment or controlled fake when possible; the important point is to model the failure and delivery behavior the business depends on.

## Trade-offs

TPF gains boundary evidence. It gives up purely local fiction for consequential integrations.

## When TPF is not a good fit

If an external system has no testable contract or support environment, recognize that as integration risk rather than hiding it behind mocks.
