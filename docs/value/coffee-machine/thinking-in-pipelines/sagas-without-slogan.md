---
title: "Is this a saga, or are we just calling it one near a whiteboard?"
faq:
  id: "sagas-without-slogan"
  track: "domain-modelling"
  question: "Does TPF replace sagas, or implement them differently?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "microservice-mike"
      text: "Any sequence of remote calls is a saga once the slide has enough arrows."
    - persona: "kafka-frank"
      text: "Publish compensating events until nobody remembers the original transaction."
    - persona: "workflow-wendy"
      text: "A compensation is a backward arrow travelling in a different colour."
social:
  poll:
    question: "What makes a saga real?"
    options:
      - "More than three network calls"
      - "A backward arrow in a diagram"
      - "Explicit ownership"
      - "A class named Saga Manager"
    preferred: "Explicit ownership"
fortune:
  quote: "A saga is a business promise under imperfect consistency, not a remote-call counting exercise."
related:
- "cross-aggregate-rules"
- "not-another-workflow-engine"
tags:
- "domain-modelling"
- "sagas"
- "slogan"
---

# Is this a saga, or are we just calling it one near a whiteboard?

## Elevator answer

**TPF can coordinate durable multi-system work, but sagas remain a business consistency strategy requiring explicit ownership, compensation, idempotency, and truthful failure semantics.**

<CoffeeMisconceptions />

## The real explanation

A saga is not a synonym for “more than one service.” It manages a business outcome that crosses independent consistency boundaries. Each participant completes a local action; if the larger outcome cannot proceed, later actions may need compensating business behavior. The hard part is not drawing the sequence. It is deciding what the business can undo, what it must correct forward, who owns the next attempt, and how duplicate messages are recognised.

TPF can help with execution. A pipeline can make a command, await a correlated completion, persist durable state at an await boundary, hand work to another pipeline, retain stable identifiers across retries, and expose telemetry or replay metadata. Those are valuable ingredients in a saga-shaped flow. They do not turn every pipeline into a saga, and they do not write credible compensation for a team.

Compensation belongs to business behavior. Cancelling a reservation may be valid; un-sending an email may not be. Reversing a charge may require a refund with different rules. A pipeline can invoke compensating behavior and give it consistent operational treatment, but it must not disguise a technical retry as a business reversal. The difference matters to customers and auditors, not only diagrams.

Ownership is equally important. Once a checkpoint handoff is admitted, the downstream pipeline owns its retry, DLQ, and lifecycle semantics. Correlation, dispatch, and idempotency identifiers remain stable so a repeated attempt is recognisably the same business intent. Without that, a retry turns into a new command wearing the old command’s coat.

This makes TPF useful when saga mechanics are otherwise improvised in listeners and callbacks. It provides one model for durable awaits, generated adapters, metadata, and boundary behavior. It does not claim the human-task tools, visual process management, or universal scheduling language of dedicated workflow products. Where those are central, use the product that owns them.

The trade-off is honesty. A team exposes the moment consistency becomes eventual, writes compensation intentionally, and tests unhappy paths. A framework cannot make a multi-system promise atomic. It can make the chosen strategy explicit enough to operate, inspect, and replay responsibly.

## Trade-offs

TPF gains consistent mechanics for saga-like coordination. It gives up the comfort of calling every distributed sequence a transaction. Teams must define compensation and ownership in business terms.

## When TPF is not a good fit

Choose a dedicated workflow engine when long-lived, human-managed process coordination is the product. Use a local transaction when one consistency boundary is sufficient; a saga is not a maturity badge.
