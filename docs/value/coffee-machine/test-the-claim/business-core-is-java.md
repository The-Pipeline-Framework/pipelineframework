---
title: "Can the business core still be ordinary Java?"
faq:
  id: "business-core-is-java"
  track: "testing"
  question: "Can business rules be tested as ordinary Java functions?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "framework-fred"
      text: "A rule is not real without a container."
    - persona: "mock-molly"
      text: "Mock the domain until it agrees."
    - persona: "test-terry"
      text: "More annotations equal more coverage."
social:
  poll:
    question: "Business rules need…"
    options:
      - "A broker"
      - "A cluster"
      - "Typed inputs"
      - "More mocks"
    preferred: "Typed inputs"
fortune:
  quote: "A business rule earns trust when its test can explain it without infrastructure."
related:
- "test-without-booting-the-planet"
- "deterministic-time"
tags:
- "testing"
- "business"
- "core"
- "java"
---

# Can the business core still be ordinary Java?

## Elevator answer

**Yes. Construct the input, call the pricing rule, assert the result. No broker, container, Spring context, database, or ceremonial `Thread.sleep()` should be needed.**

<CoffeeMisconceptions />

## The real explanation

The best business test is almost rude in its simplicity: `assertThat(price(order, customer)).isEqualTo(...)`. The decision receives typed facts and returns a typed result. If explaining a discount requires Kafka, an HTTP stub, a thread pool, and an open Hibernate session, the “unit” has acquired municipal infrastructure.

TPF does not ask developers to replace normal Java testing with framework fixtures. It asks them to preserve the boundary so the business behavior is testable without the shell. A test can assert ordinary outcomes, rejected cases, and property-like invariants. The flow and runtime tests then prove that this behavior is invoked through the intended connector and execution contract.

The trade-off is that dependencies must be supplied explicitly. That is a small cost for knowing which facts actually influence a decision.

## Trade-offs

TPF gains fast tests and reusable behavior. It gives up hidden infrastructure access inside the domain.

## When TPF is not a good fit

If a rule is inherently an adapter concern, test it as an adapter. Do not pretend a network client is a pure business function.
