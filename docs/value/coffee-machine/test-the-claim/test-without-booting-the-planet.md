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
---

# Do I need to boot the planet to test one flow?

## Elevator answer

**Test typed business steps directly, test flow contracts at compilation, and reserve real infrastructure for focused connector and integration tests where boundaries actually matter.**

<CoffeeMisconceptions />

## The real explanation

A pipeline should not make the business rule harder to test. Typed business steps remain ordinary Java functions: pass explicit inputs, assert explicit outputs, and exercise rejection behavior without a runtime. That is the fast feedback layer.

The pipeline model adds a different test layer: compilation and contract validation. Step resolution, mapper compatibility, connector declarations, cardinality, and generated artifacts can be checked before a runtime test begins. This catches structural mistakes that a perfectly isolated function test cannot see.

Then test actual boundaries deliberately. A connector deserves focused contract tests against a realistic dependency or controlled test double. A flow with retries, durable awaits, or external side effects deserves an integration test that proves the relevant runtime semantics. The point is proportion: do not start every test with the whole platform, but do not mock away the boundary that defines the risk.

## Trade-offs

TPF gains a layered test strategy. It gives up the illusion that one kind of test proves everything.

## When TPF is not a good fit

If a team will only run end-to-end tests or only run unit tests, the framework cannot supply the missing evidence.
