---
title: "Is runtime layout just Maven topology in a nicer shirt?"
faq:
  id: "runtime-layout-not-maven"
  track: "deployment"
  question: "Is runtime layout just another name for Maven module structure?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "build-barry"
      text: "If two classes share a JAR, they share fate."
    - persona: "microservice-mike"
      text: "A new module is a service after the next build."
    - persona: "enterprise-edna"
      text: "Architecture is whatever the parent POM permits."
social:
  poll:
    question: "A Maven module is…"
    options:
      - "A runtime boundary"
      - "A tiny microservice"
      - "A build boundary"
      - "A deployment prophecy"
    preferred: "A build boundary"
fortune:
  quote: "Build topology ships code; runtime layout explains how that code behaves."
related:
- "runtime-placement-is-a-decision"
- "generation-not-a-container-factory"
tags:
- "deployment"
- "runtime"
- "layout"
- "maven"
---

# Is runtime layout just Maven topology in a nicer shirt?

## Elevator answer

**No. Runtime layout describes logical execution and invocation boundaries; Maven topology describes how artifacts are built, packaged, and physically assembled for delivery.**

<CoffeeMisconceptions />

## The real explanation

Maven modules are valuable build topology. They define dependency relationships, artifact boundaries, compilation, tests, and packaging. Runtime layout answers different questions: which components execute together, which calls cross a runtime boundary, and which transport or platform behavior applies. The two often influence each other, but a module boundary is not evidence of a network boundary, and a remote runtime does not require a particular source-tree ceremony.

TPF keeps the distinction deliberately. The pipeline model may map a step to a local runtime or a remote invocation. Generated artifacts can then implement the relevant adapter and binding. A team may package related code in one JAR while deploying several runtime components, or package several internal modules into one deployable. That flexibility is useful because build convenience and operational isolation rarely align perfectly.

Confusing the dimensions causes predictable errors. A team may split Maven modules and believe it has achieved independent scaling. Or it may place unrelated flows in one artifact and assume they must share failure behavior. TPF asks the team to state the runtime relationship directly, then choose build topology that supports it.

There is a cost: two models are more to explain than one. But pretending they are identical merely moves the explanation into deployment scripts and incident channels. The framework prefers a visible distinction to an accidental coupling.

## Trade-offs

TPF gains accuracy about execution and packaging. It gives up a single shorthand for architecture. Teams must align build and runtime changes deliberately.

## When TPF is not a good fit

For a small application whose module and runtime boundaries truly coincide, do not invent separate diagrams. The distinction matters when the dimensions begin to diverge.
