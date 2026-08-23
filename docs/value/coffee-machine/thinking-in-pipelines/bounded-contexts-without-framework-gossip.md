---
title: "How do bounded contexts talk without exchanging framework gossip?"
faq:
  id: "bounded-contexts-without-framework-gossip"
  track: "domain-modelling"
  question: "How do bounded contexts communicate without leaking pipeline internals?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "microservice-mike"
      text: "Give every context the same schema and call the dependency independence."
    - persona: "kafka-frank"
      text: "A shared topic is a bounded context because it has a boundary in the console."
    - persona: "consultant-nigel"
      text: "Solve translation with one enterprise canonical model, then never change it."
social:
  poll:
    question: "What should cross a bounded-context boundary?"
    options:
      - "The entire internal object"
      - "A shared base class, naturally"
      - "Explicit typed boundary"
      - "A spreadsheet curated by Dave"
    preferred: "Explicit typed boundary"
fortune:
  quote: "A handoff is healthy when the receiver owns the contract, not the sender’s implementation."
related:
- "domain-events-not-confetti"
- "cross-aggregate-rules"
tags:
- "domain-modelling"
- "bounded"
- "contexts"
- "framework"
- "gossip"
---

# How do bounded contexts talk without exchanging framework gossip?

## Elevator answer

**Send Fulfillment an order it can understand—not Order’s pipeline state, retry counters, Hibernate entity, and childhood memories. Each context translates at its own edge.**

<CoffeeMisconceptions />

## The real explanation

Order may call something “accepted” when payment is authorised; Fulfillment may reserve that word until a warehouse has claimed it. That difference is the point of the boundary. Sharing one Java record because both teams use `orderId` does not create alignment; it creates a meeting scheduled by the next field rename.

TPF should sit inside a boundary, not erase it. A pipeline declares how one context admits work, invokes its typed domain behavior, and publishes a result through a connector. Another context receives a command, event, or external representation through its own connector and translates it into its own model. The connection is a contract between contexts, not a shared pipeline definition with a different package name.

Connectors and mappers make the translation explicit. Order can publish `OrderReadyForFulfillment`; Fulfillment maps that representation into its own intake command. Headers, retry metadata, internal step results, and the Order JPA entity stay home. A handoff is healthy when the receiver owns the contract, not the sender's implementation.

Pipelines may coordinate a business journey that crosses contexts, but ownership must remain explicit. A checkpoint handoff is not just a convenient graph continuation: after admission, the downstream pipeline owns retry, DLQ, and lifecycle semantics. That answers the operational question alongside the business one: who owns failure, replay, and completion now?

The alternative is often a distributed application service. One context calls into another, shares internal types, catches its exceptions, and gradually learns its data model. It may work for years. It also makes independent evolution expensive because every local refactor becomes a negotiation with consumers who were never meant to be consumers. TPF does not forbid this; it offers a more explicit route when the boundary is real.

Translation costs effort, and a small modular monolith may not need separate contracts for every collaboration. Preserve the distinction where ownership, lifecycle, or language truly differs. Do not simulate autonomy with mapper classes when one model is actually sufficient.

The rule is: pipelines may coordinate a handoff, but no context should need to understand another context’s internals to do its job. That preserves autonomy without pretending that integrations have no cost.

## Trade-offs

TPF gains explicit ownership and evolvable contracts at real boundaries. It gives up the shortcut of sharing internal types and execution state. Teams must decide where a boundary is meaningful rather than using the framework to manufacture one.

## When TPF is not a good fit

If two modules truly share one model and lifecycle, keep them together. Do not introduce a connector boundary merely to make a modular monolith look like a federation of services.
