---
title: "Will this generate a distributed monolith with better branding?"
faq:
  id: "no-generated-distributed-monolith"
  track: "deployment"
  question: "How do we avoid generating a distributed monolith?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "microservice-mike"
      text: "More network calls equal more independence."
    - persona: "codegen-carl"
      text: "Generated boundaries automatically become real boundaries."
    - persona: "enterprise-edna"
      text: "Put every capability behind a gateway."
social:
  poll:
    question: "What prevents monoliths?"
    options:
      - "More containers"
      - "More generators"
      - "Clear ownership"
      - "More arrows"
    preferred: "Clear ownership"
fortune:
  quote: "Generated boundaries are only useful when the ownership behind them is real."
related:
- "one-pipeline-one-container"
- "platform-team-boundary"
tags:
- "deployment"
- "no"
- "generated"
- "distributed"
- "monolith"
search: false
---

# Will this generate a distributed monolith with better branding?

## Elevator answer

**Do not turn every arrow into HTTP because a generator can. If Order and Pricing must deploy, fail, and change together, putting them in separate pods has only added weather.**

<CoffeeMisconceptions />

## The real explanation

Order calls Pricing, Pricing calls Customer, Customer reads Order's tables, and all three must deploy together on Thursday night. That is the monolith, now distributed. A generator can make the situation impressively fast to create if every internal arrow becomes a remote adapter without an ownership or isolation reason.

TPF avoids prescribing a service per pipeline. A pipeline is a logical flow, and runtime placement is a separate decision. Several flows may share a deployable. A remote boundary should exist because a team needs independent ownership, isolation, scaling, security, or lifecycle—not because the framework can render one.

The compiler and generated artifacts help keep declared boundaries honest: connectors, transport modes, mappings, and runtime descriptions are explicit. They cannot cure a bad dependency graph. If every pipeline reaches synchronously into the same database, requires the same release train, and shares retries with its neighbours, the architecture remains coupled no matter how many adapters were generated.

The remedy is ordinary architectural discipline. Keep the core focused, treat a checkpoint handoff as an ownership transfer, publish stable contracts rather than internal state, and choose placement based on operational reality. Review generated topology as a consequence of those choices, not as an authority that made them.

The trade-off is restraint. A team cannot use generation as an excuse for automatic decomposition. It must say no to remote boundaries that add latency and failure modes without buying autonomy.

## Trade-offs

TPF gains explicit runtime decisions and generated boundary code. It gives up automatic microservice architecture. Teams remain responsible for ownership and dependency direction.

## When TPF is not a good fit

If the organisation wants a tool to decompose a monolith mechanically, TPF is not that tool. Start with capability ownership and operational needs.
