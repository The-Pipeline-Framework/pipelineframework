---
title: "Can retries and awaits be tested before lunch?"
faq:
  id: "deterministic-time"
  track: "testing"
  question: "Can we deterministically test retries, timeouts, and asynchronous completion?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "test-terry"
      text: "Add ten seconds; CI has time."
    - persona: "async-andy"
      text: "Flaky tests prove concurrency is real."
    - persona: "workflow-wendy"
      text: "A timer is a business process."
social:
  poll:
    question: "Async tests need…"
    options:
      - "Longer sleeps"
      - "Better luck"
      - "Controlled time"
      - "More threads"
    preferred: "Controlled time"
fortune:
  quote: "A timeout test should advance a state machine, not a stopwatch."
related:
- "test-uncertain-effects"
- "test-pyramid-not-test-panic"
tags:
- "testing"
- "deterministic"
- "time"
---

# Can retries and awaits be tested before lunch?

## Elevator answer

**Yes. Treat time, correlation, attempts, and completion as controllable test inputs, then assert durable state transitions rather than sleeping and hoping.**

<CoffeeMisconceptions />

## The real explanation

Time-based behavior becomes untestable when tests wait for wall-clock luck. A durable await should expose correlation, admission, timeout, duplicate completion handling, and resume behavior as observable state transitions. Retries should expose attempt identity, policy, and terminal outcome. Test doubles can control the scheduler or completion boundary so a test advances the scenario instead of sleeping.

This is more than convenience. A test that waits rarely proves which transition occurred; it merely proves that something eventually happened in one environment. Deterministic tests can assert that a duplicate completion is rejected, a timeout routes to the correct state, or a retry preserves its idempotency key.

TPF’s execution model supplies the semantic hooks; teams still need to write the cases that matter to their business promise.

## Trade-offs

TPF gains deterministic temporal tests. It gives up casual sleeps and vague asynchronous assertions.

## When TPF is not a good fit

If a team cannot control or observe time and completion in tests, first improve the boundary design.
