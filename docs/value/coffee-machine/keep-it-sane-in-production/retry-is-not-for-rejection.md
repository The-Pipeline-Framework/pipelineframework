---
title: "Should every failure get another try?"
faq:
  id: "retry-is-not-for-rejection"
  track: "operations"
  question: "Does TPF retry business failures that should be rejected immediately?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "retry-rita"
      text: "Try it three times; maybe the policy changes its mind."
    - persona: "retry-rita"
      text: "Every timeout is a rejection."
    - persona: "platform-priya"
      text: "One retry policy should rule all actions."
social:
  poll:
    question: "Retry a rejection?"
    options:
      - "Until it agrees"
      - "After a coffee"
      - "No"
      - "Via a cron job"
    preferred: "No"
fortune:
  quote: "A retry repairs uncertainty; it should not negotiate with a business decision."
related:
- "idempotency-after-lost-response"
- "dlq-and-replay"
tags:
- "operations"
- "retry"
- "for"
- "rejection"
---

# Should every failure get another try?

## Elevator answer

**No. Retry transient technical uncertainty; reject known business outcomes immediately, and declare the distinction so operators and callers can understand what actually happened.**

<CoffeeMisconceptions />

## The real explanation

Retry is valuable when the outcome is uncertain or a technical dependency may recover: a timeout, temporary network failure, rate limit, or unavailable service. It is wrong when the business has made a definite decision: a payment is not permitted, stock is unavailable, a request violates a rule. Repeating a rejection adds load and obscures meaning.

TPF treats that distinction as part of execution semantics. Business steps should return explicit outcomes or rejection signals. Connector and runtime boundaries can apply retry policy to transient technical failure. The resulting telemetry should show whether an item was rejected by the business, failed technically, was retried, or reached a terminal failure path.

The difficult case is uncertainty after an external action. A remote payment may have succeeded even though the response timed out. Retrying without an idempotency key can create a second charge; rejecting immediately can abandon a valid action. The solution is not a universal annotation. It is a boundary contract with stable identifiers, an inquiry or reconciliation path where possible, and a retry policy appropriate to the business consequence.

The trade-off is specificity. Different actions deserve different retry budgets and terminal behavior. TPF gives a shared framework for declaring and operating them; it should not flatten them into one generic resilience ritual.

## Trade-offs

TPF gains meaningful failure categories. It gives up a universal retry switch. Teams must decide which outcomes are transient, terminal, or uncertain.

## When TPF is not a good fit

If a dependency has no idempotency story and no way to reconcile uncertain outcomes, first solve that boundary contract. A retry framework cannot make an unsafe effect safe.
