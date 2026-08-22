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
      - "A shared base class,"
      - "Explicit typed boundary"
      - "A spreadsheet maintained by"
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

**Bounded contexts exchange typed domain or integration contracts; pipelines coordinate each context’s execution without becoming a shared technical language across their boundary.**

<CoffeeMisconceptions />

## The real explanation

Bounded contexts exist so a model can have coherent language and ownership. Customer, order, risk, and fulfillment may use familiar words differently because they make different decisions. A framework becomes harmful when it causes those contexts to share an implementation vocabulary merely because they participate in one business journey.

TPF should sit inside a boundary, not erase it. A pipeline declares how one context admits work, invokes its typed domain behavior, and publishes a result through a connector. Another context receives a command, event, or external representation through its own connector and translates it into its own model. The connection is a contract between contexts, not a shared pipeline definition with a different package name.

Connectors and mappers make this practical. A connector marks captured external reality or a publication boundary. A mapper translates a pair of types deliberately and deterministically. They prevent one model’s internal object shape from becoming another model’s accidental API. A fulfillment context should receive a contract it can own, not the order context’s pipeline state, retry metadata, or private decisions about acceptance.

Pipelines may coordinate a business journey that crosses contexts, but ownership must remain explicit. A checkpoint handoff is not just a convenient graph continuation: after admission, the downstream pipeline owns retry, DLQ, and lifecycle semantics. That answers the operational question alongside the business one: who owns failure, replay, and completion now?

The alternative is often a distributed application service. One context calls into another, shares internal types, catches its exceptions, and gradually learns its data model. It may work for years. It also makes independent evolution expensive because every local refactor becomes a negotiation with consumers who were never meant to be consumers. TPF does not forbid this; it offers a more explicit route when the boundary is real.

Translation costs effort, and a small modular monolith may not need separate contracts for every collaboration. Preserve the distinction where ownership, lifecycle, or language truly differs. Do not simulate autonomy with mapper classes when one model is actually sufficient.

The rule is: pipelines may coordinate a handoff, but no context should need to understand another context’s internals to do its job. That preserves autonomy without pretending that integrations have no cost.

## Trade-offs

TPF gains explicit ownership and evolvable contracts at real boundaries. It gives up the shortcut of sharing internal types and execution state. Teams must decide where a boundary is meaningful rather than using the framework to manufacture one.

## When TPF is not a good fit

If two modules truly share one model and lifecycle, keep them together. Do not introduce a connector boundary merely to make a modular monolith look like a federation of services.
