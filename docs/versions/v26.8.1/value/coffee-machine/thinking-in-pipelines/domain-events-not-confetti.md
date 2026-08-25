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
      - "A Kafka record after lunch"
      - "Typed fact: what happened"
      - "A meeting invitation for Kafka"
    preferred: "Typed fact: what happened"
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
search: false
---

# Are domain events first-class, or just pipeline exhaust?

## Elevator answer

**`PaymentDeclined` says what happened. A Kafka record says how another system hears about it. The pipeline may connect the two; it should not pretend they are the same object wearing JSON.**

<CoffeeMisconceptions />

## The real explanation

`CustomerSurnameFieldUpdated` is probably not a moment in business history. Nor is every byte array a domain event because it escaped through Kafka. Event confetti makes the model impossible to follow; calling every transport payload an event makes the word useless.

TPF treats the distinction seriously. A domain event is a typed statement about something that occurred in the business domain: an order was accepted, a reservation expired, a payment was declined. It should be named in ubiquitous language and remain meaningful even if no broker, HTTP endpoint, or runtime adapter exists. A pipeline may produce that event as an outcome of domain behavior, and a connector may publish it externally. Those are related facts, but they are not the same thing.

The external message has a different job. A Kafka record needs headers, a schema version, partitioning, and delivery rules. A REST callback needs a route, authentication, and an answer to “what does 503 mean?” Map the domain event to that representation at the connector. Do not serialize the aggregate and let five consumers discover its private fields by accident.

This avoids treating technical events as business events. MessageReceived, RetryScheduled, or HTTPCallSucceeded may be useful telemetry or operational signals. They are not automatically domain facts. They should not leak into the core merely because the runtime observes them. Conversely, an important domain event should not be downgraded to a logging line because no external consumer exists yet. The business model owns its meaning; the pipeline and shell own how it moves and is observed.

Publishing `PaymentDeclined` has consequences. The flow must preserve its identity and required metadata, and somebody must own a timeout or terminal publish failure. “We emitted an event” is optimistic narration if a retry can duplicate the Kafka record and nobody knows whether the consumer accepted the first one.

There is no requirement that every pipeline end with a domain event. Some flows return a query result, persist a state transition, or hand work to another boundary. There is also no requirement that every domain event be broadcast through a broker. A local event can be useful without becoming a distributed integration contract. TPF keeps those decisions explicit instead of treating an event name as a transport configuration shortcut.

The trade-off is more modeling care. Teams define events in domain language, define external contracts separately, and maintain compatible mappers between them. That is more work than serializing an internal object and hoping no consumer depends on it. It is less work than unpicking a broker schema that silently became the domain model years ago.

The useful sentence is: domain events describe what happened; connectors describe how another system hears about it. A pipeline can coordinate both without pretending they are interchangeable.

## Trade-offs

TPF gains cleaner domain language and replaceable transport contracts. It gives up the shortcut of treating every message payload as the business model. Teams must own versioning and mapping at the boundary.

## When TPF is not a good fit

Do not invent domain events merely to make an application feel event-driven. A local operation with no consequential fact to communicate may be clearer as a direct result. TPF cannot make a vague event meaningful by routing it through a connector.
