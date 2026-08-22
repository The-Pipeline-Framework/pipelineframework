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

**Yes. Event sourcing records domain state differently; TPF coordinates typed execution and boundaries, without requiring events to become every pipeline’s persistence model.**

<CoffeeMisconceptions />

## The real explanation

Event sourcing and pipelines solve different problems. Event sourcing represents state as a sequence of domain events and rebuilds or projects state from that sequence. A pipeline describes a typed application flow and the operational boundaries around execution. They work together because neither requires the other to surrender its central idea.

An event-sourced aggregate can remain the owner of its invariant and produce domain events as a command result. A TPF pipeline can admit the command, obtain the aggregate through a declared boundary, invoke the decision, persist resulting events through the chosen store, and publish an integration representation through a connector. The flow does not need to pretend the event store is a message broker, and the event store does not become the pipeline’s universal runtime.

This protects both models from category errors. Replay in TPF concerns reliable execution and captured operational history. It does not automatically mean replaying an event stream to rebuild domain state. Conversely, an event-sourced stream is not automatically a record of every retry, transport attempt, or adapter failure. Those can require separate telemetry and durable state surfaces. Treating them as one thing produces a record too technical for the domain and too vague for an operator.

TPF can be useful in an event-sourced system because command handling often crosses boundaries after a domain decision. A command may need validation, enrichment, external publication, or a durable await. The framework makes those semantics explicit while aggregates and event stores retain their own rules. Mappers matter because a domain event need not be the exact contract consumed by another bounded context.

TPF does not require event sourcing. A pipeline can use ordinary persistence, materialized state, or a connector-backed external system. Adopt a pipeline because a business flow needs a typed, validated execution contract; adopt event sourcing because event history is the right source of truth. Those are independent choices.

The cost is conceptual care. Teams distinguish domain event history, execution telemetry, replay metadata, and external messages. That is more vocabulary, but it prevents the phrase “replay the event” from hiding four incompatible operations.

## Trade-offs

TPF gains a consistent shell around event-sourced or conventional models. It gives up a single storage story. Teams must define which records are domain truth and which exist for operations, retry, or observability.

## When TPF is not a good fit

Do not adopt TPF simply because an event store exists. If command handling is local and the event-sourced model already captures the needed boundary, ordinary application code may be clearer.
