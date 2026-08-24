---
title: "Do I need event sourcing to be invited?"
faq:
  id: "event-sourcing-not-required"
  track: "domain-modelling"
  question: "Is the pipeline model compatible with event sourcing?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "ddd-diego"
      text: "If state is not reconstructed from seventeen events, it has not earned existence."
    - persona: "jpa-jane"
      text: "If an event appears, immediately add a mutable entity and a cascade."
    - persona: "platform-priya"
      text: "Store logs forever and call the retention policy temporal architecture."
social:
  poll:
    question: "What is event sourcing to TPF?"
    options:
      - "The required database format"
      - "A fancier retry log"
      - "Explicit persistence boundary"
      - "Kafka with a literature degree"
    preferred: "Explicit persistence boundary"
fortune:
  quote: "Event sourcing records domain truth; a pipeline records how a typed decision survives its journey."
related:
- "domain-events-not-confetti"
- "rich-domain-not-a-hostage"
tags:
- "domain-modelling"
- "event"
- "sourcing"
- "required"
---

# Do I need event sourcing to be invited?

## Elevator answer

**No. An event store may remain the source of truth for an aggregate; TPF coordinates the command, publication, and waits around it. Ordinary rows are invited too.**

<CoffeeMisconceptions />

## The real explanation

An event store rebuilds an account from `AccountOpened`, `MoneyDeposited`, and `MoneyWithdrawn`. A pipeline gets `WithdrawMoney` to the aggregate, publishes the resulting integration message, and perhaps waits for a fraud review. One records domain state; the other coordinates the journey around a decision. Nobody has to surrender a database.

An event-sourced aggregate can remain the owner of its invariant and produce domain events as a command result. A TPF pipeline can admit the command, obtain the aggregate through a declared boundary, invoke the decision, persist resulting events through the chosen store, and publish an integration representation through a connector. The flow does not need to pretend the event store is a message broker, and the event store does not become the pipeline’s universal runtime.

Keep the two replay buttons labelled. Replaying an account's event stream rebuilds domain state. Replaying pipeline computation, reusing a captured Query, or recovering a Command effect answers different operational questions. The event stream is not obliged to record every HTTP attempt; runtime telemetry is not the bank ledger. Combining them produces a record too technical for the domain and too vague for the operator.

TPF can be useful in an event-sourced system because command handling often crosses boundaries after a domain decision. A command may need validation, enrichment, external publication, or a durable await. The framework makes those semantics explicit while aggregates and event stores retain their own rules. Mappers matter because a domain event need not be the exact contract consumed by another bounded context.

TPF does not require event sourcing. A pipeline can use ordinary persistence, materialized state, or a connector-backed external system. Adopt a pipeline because a business flow needs a typed, validated execution contract; adopt event sourcing because event history is the right source of truth. Those are independent choices.

The cost is conceptual care. Teams distinguish domain event history, execution telemetry, replay metadata, and external messages. That is more vocabulary, but it prevents the phrase “replay the event” from hiding four incompatible operations.

## Trade-offs

TPF gains a consistent shell around event-sourced or conventional models. It gives up a single storage story. Teams must define which records are domain truth and which exist for operations, retry, or observability.

## When TPF is not a good fit

Do not adopt TPF simply because an event store exists. If command handling is local and the event-sourced model already captures the needed boundary, ordinary application code may be clearer.
