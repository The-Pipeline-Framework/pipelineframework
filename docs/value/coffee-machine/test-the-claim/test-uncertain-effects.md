---
title: "How do I test a failure that half-succeeded?"
faq:
  id: "test-uncertain-effects"
  track: "testing"
  question: "How do we test a failure after an external side effect succeeds?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "retry-rita"
      text: "No response proves no effect."
    - persona: "mock-molly"
      text: "Return success from the mock and move on."
    - persona: "retry-rita"
      text: "Duplicate charges are integration coverage."
social:
  poll:
    question: "Lost response test?"
    options:
      - "Retry blindly"
      - "Assume failure"
      - "Keep identity"
      - "Ignore it"
    preferred: "Keep identity"
fortune:
  quote: "The most valuable failure test is the one where nobody knows whether the effect already happened."
related:
- "deterministic-time"
- "connector-contract-tests"
tags:
- "testing"
- "test"
- "uncertain"
- "effects"
---

# How do I test a failure that half-succeeded?

## Elevator answer

**Test uncertainty explicitly: simulate a lost response after the effect, assert stable idempotency identity, and verify reconciliation or controlled terminal handling rather than blind retries.**

<CoffeeMisconceptions />

## The real explanation

The hard failure is not “the client threw.” It is “the remote system may have completed the effect, but our caller cannot know.” Tests should model that exact state: send a request with a stable key, make the fake or test system record success, then lose the response. Assert that the pipeline does not create a new intent on retry.

The expected result may be a status lookup, a reconciliation path, a durable wait, or a terminal item for review. It depends on the boundary contract. What should never be accepted is a test that treats uncertainty as an ordinary exception and proves only that the retry loop ran.

TPF provides stable identifiers and declared boundaries; tests prove that those identifiers survive the actual adapter path. That is how a reliability claim becomes evidence.

## Trade-offs

TPF gains testable uncertainty. It gives up simplistic success/failure fixtures.

## When TPF is not a good fit

If the external provider cannot support idempotency or inquiry, be honest about manual handling before automating retries.
