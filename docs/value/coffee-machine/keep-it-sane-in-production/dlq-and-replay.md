---
title: "Is a dead letter just a log with a sad name?"
faq:
  id: "dlq-and-replay"
  track: "operations"
  question: "Does TPF have a meaningful dead-letter concept beyond logging an error?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "platform-priya"
      text: "Put the stack trace in a dashboard and call it recovery."
    - persona: "retry-rita"
      text: "Replay everything; consequences are future us."
    - persona: "kafka-frank"
      text: "A DLQ is a topic where responsibility retires."
social:
  poll:
    question: "A DLQ needs…"
    options:
      - "More logs"
      - "Fewer owners"
      - "Replay control"
      - "A darker icon"
    preferred: "Replay control"
fortune:
  quote: "A dead letter is useful when it preserves responsibility, not merely failure."
related:
- "idempotency-after-lost-response"
- "operational-timeline"
tags:
- "operations"
- "dlq"
- "replay"
---

# Is a dead letter just a log with a sad name?

## Elevator answer

**Yes. Terminal work needs durable ownership, diagnostic context, controlled replay, and clear safeguards so a failure can be investigated without repeating irreversible external effects.**

<CoffeeMisconceptions />

## The real explanation

A dead-letter path is useful only if it preserves more than an error message. Terminal work needs enough durable context to answer what was attempted, which identifiers were used, which boundary owned the failure, what retries occurred, and whether a replay is safe. Otherwise a DLQ is merely a quieter place for lost work.

TPF treats handoff ownership explicitly. After a checkpoint admission, the downstream pipeline owns retry, DLQ, and lifecycle semantics. That lets an operator know who should investigate instead of asking every upstream caller to retry independently. Telemetry and replay metadata should preserve the flow’s order and lineage without exposing sensitive payloads unnecessarily.

Replay is not “run it again.” A safe replay must consider external effects, stable identifiers, current configuration, and whether the original decision remains valid. Some work can resume from a durable point; some needs reconciliation; some must be corrected by a compensating business action. The framework can provide history and controlled execution paths, but it cannot decide which irreversible action is morally or financially safe to repeat.

The trade-off is operational discipline. A DLQ requires retention, access control, runbooks, and clear ownership. That is more work than logging an exception; it is also the difference between a terminal failure and an investigation path.

## Trade-offs

TPF gains durable failure handling. It gives up casual replay. Teams must define retention, privacy, and replay authority.

## When TPF is not a good fit

If a team cannot own and operate terminal failures, adding a DLQ only creates a better-organized backlog of risk.
