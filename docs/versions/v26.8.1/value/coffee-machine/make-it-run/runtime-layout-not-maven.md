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
search: false
---

# Is runtime layout just Maven topology in a nicer shirt?

## Elevator answer

**No. A Maven module says how code compiles and packages. A runtime boundary says a call now crosses a process, transport, failure domain, and probably somebody else’s pager.**

<CoffeeMisconceptions />

## The real explanation

Splitting `pricing` into its own Maven module may improve dependencies and tests. It has not created a service. Conversely, calling Pricing over gRPC changes latency, retries, failure, deployment, and ownership even if both sides still live in one repository. A folder is not a network, however ambitious its `pom.xml`.

TPF keeps the distinction deliberately. The pipeline model may map a step to a local runtime or a remote invocation. Generated artifacts can then implement the relevant adapter and binding. A team may package related code in one JAR while deploying several runtime components, or package several internal modules into one deployable. That flexibility is useful because build convenience and operational isolation rarely align perfectly.

Confusing the dimensions causes predictable errors. A team may split Maven modules and believe it has achieved independent scaling. Or it may place unrelated flows in one artifact and assume they must share failure behavior. TPF asks the team to state the runtime relationship directly, then choose build topology that supports it.

There is a cost: two models are more to explain than one. But pretending they are identical merely moves the explanation into deployment scripts and incident channels. The framework prefers a visible distinction to an accidental coupling.

## Trade-offs

TPF gains accuracy about execution and packaging. It gives up a single shorthand for architecture. Teams must align build and runtime changes deliberately.

## When TPF is not a good fit

For a small application whose module and runtime boundaries truly coincide, do not invent separate diagrams. The distinction matters when the dimensions begin to diverge.
