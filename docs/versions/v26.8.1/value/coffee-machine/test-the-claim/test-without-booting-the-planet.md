---
title: "Do I need to boot the planet to test one flow?"
faq:
  id: "test-without-booting-the-planet"
  track: "testing"
  question: "How do we unit-test a pipeline without bringing up Kafka, databases, and HTTP?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "test-terry"
      text: "A unit test needs one container per dependency."
    - persona: "kafka-frank"
      text: "A broker is the smallest useful test double."
    - persona: "build-barry"
      text: "If it finishes overnight, it is thorough."
social:
  poll:
    question: "First test layer?"
    options:
      - "All containers"
      - "Production traffic"
      - "Business step"
      - "A screenshot"
    preferred: "Business step"
fortune:
  quote: "Test the decision quickly, then test the boundary honestly."
related:
- "business-core-is-java"
- "connector-contract-tests"
tags:
- "testing"
- "test"
- "booting"
- "planet"
search: false
---

# Do I need to boot the planet to test one flow?

## Elevator answer

**Call ordinary Java for ordinary decisions. Boot Kafka, Postgres, or a provider simulator only when the test is specifically about Kafka, Postgres, or the provider—not because the rule lives in a pipeline.**

<CoffeeMisconceptions />

## The real explanation

A pipeline should not make `calculateDiscount()` wait for Testcontainers. Pass explicit inputs, assert explicit output, and test rejection as ordinary Java. Bring up Kafka only to prove offset, acknowledgement, serialization, or duplicate-delivery behavior. The planet may remain unbooted for arithmetic.

The pipeline model adds a different test layer: compilation and contract validation. Step resolution, mapper compatibility, connector declarations, cardinality, and generated artifacts can be checked before a runtime test begins. This catches structural mistakes that a perfectly isolated function test cannot see.

Then spend infrastructure where the risk lives. Test the Kafka connector against real serialization and acknowledgement behavior. Test an Await through durable suspend and duplicate completion. Test a lost payment response with a provider simulator that records the charge. Do not boot the platform for arithmetic, and do not mock away the one boundary the test claims to prove.

## Trade-offs

TPF gains a layered test strategy. It gives up the illusion that one kind of test proves everything.

## When TPF is not a good fit

If a team will only run end-to-end tests or only run unit tests, the framework cannot supply the missing evidence.
