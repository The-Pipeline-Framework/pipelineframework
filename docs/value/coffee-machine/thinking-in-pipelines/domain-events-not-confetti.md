---
title: "Are domain events first-class, or just pipeline exhaust?"
faq:
  id: "domain-events-not-confetti"
  track: "domain-modelling"
  question: "Are domain events first-class, or merely outputs from a pipeline?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "kafka-frank"
      text: "If it was not emitted to a topic with twelve partitions, it was merely a private thought."
    - persona: "ddd-diego"
      text: "Every setter deserves a past-tense event, preferably before the entity finishes changing."
    - persona: "platform-priya"
      text: "An event is anything that reaches observability wearing JSON."
social:
  poll:
    question: "What is a domain event?"
    options:
      - "Any JSON that escapes"
      - "A Kafka record after"
      - "Typed flow contract"
      - "A meeting invitation for"
    preferred: "Typed flow contract"
fortune:
  quote: "A domain event says what happened; a connector says who, if anyone, gets to hear about it."
related:
- "bounded-contexts-without-framework-gossip"
- "cross-aggregate-rules"
- "hiding-io-without-hiding-reality"
tags:
- "domain-modelling"
- "domain"
- "events"
- "confetti"
---

# Are domain events first-class, or just pipeline exhaust?

## Elevator answer

**Domain events express business facts; pipelines may produce, route, and publish them, but the framework must not confuse those facts with transport mechanics.**

<CoffeeMisconceptions />

## The real explanation

Domain events are easy to overstate and easy to trivialize. Overstated, they become an event for every field change and an excuse to make a local model impossible to follow. Trivialized, they become whatever payload happened to be published by an integration client. Neither definition helps a team distinguish a meaningful business fact from a transport record.

TPF treats the distinction seriously. A domain event is a typed statement about something that occurred in the business domain: an order was accepted, a reservation expired, a payment was declined. It should be named in ubiquitous language and remain meaningful even if no broker, HTTP endpoint, or runtime adapter exists. A pipeline may produce that event as an outcome of domain behavior, and a connector may publish it externally. Those are related facts, but they are not the same thing.

The separation matters because an external message has a different shape and lifecycle. A Kafka record carries headers, partitioning, serialization, consumer-group behavior, and delivery semantics. A REST callback carries a route, authentication, and HTTP failure behavior. A domain event should not acquire all that vocabulary merely because an integration currently uses it. TPF’s connector and mapper boundaries let the domain fact become an external representation deliberately, with a typed contract rather than accidental entity serialization.

This avoids treating technical events as business events. MessageReceived, RetryScheduled, or HTTPCallSucceeded may be useful telemetry or operational signals. They are not automatically domain facts. They should not leak into the core merely because the runtime observes them. Conversely, an important domain event should not be downgraded to a logging line because no external consumer exists yet. The business model owns its meaning; the pipeline and shell own how it moves and is observed.

Pipelines make the handoff visible. A flow can declare where it publishes external reality, preserve order and metadata, and route failure through proper execution semantics. That is valuable when delivery is not instantaneous or may be retried. A publication boundary needs idempotency and ownership just as any other external effect does. Saying “we emitted an event” is not enough if a retry may duplicate it or a downstream failure leaves the originating pipeline uncertain about responsibility.

There is no requirement that every pipeline end with a domain event. Some flows return a query result, persist a state transition, or hand work to another boundary. There is also no requirement that every domain event be broadcast through a broker. A local event can be useful without becoming a distributed integration contract. TPF keeps those decisions explicit instead of treating an event name as a transport configuration shortcut.

The trade-off is more modeling care. Teams define events in domain language, define external contracts separately, and maintain compatible mappers between them. That is more work than serializing an internal object and hoping no consumer depends on it. It is less work than unpicking a broker schema that silently became the domain model years ago.

The useful sentence is: domain events describe what happened; connectors describe how another system hears about it. A pipeline can coordinate both without pretending they are interchangeable.

## Trade-offs

TPF gains cleaner domain language and replaceable transport contracts. It gives up the shortcut of treating every message payload as the business model. Teams must own versioning and mapping at the boundary.

## When TPF is not a good fit

Do not invent domain events merely to make an application feel event-driven. A local operation with no consequential fact to communicate may be clearer as a direct result. TPF cannot make a vague event meaningful by routing it through a connector.
