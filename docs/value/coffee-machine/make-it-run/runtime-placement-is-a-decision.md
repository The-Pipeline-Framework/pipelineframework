---
title: "Who decides where a pipeline runs?"
faq:
  id: "runtime-placement-is-a-decision"
  track: "deployment"
  question: "What determines runtime placement?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "kubernetes-kai"
      text: "Place it wherever the diagram has whitespace."
    - persona: "kubernetes-kai"
      text: "Autoscaling is an architecture decision with metrics."
    - persona: "microservice-mike"
      text: "Network distance is a sign of independence."
social:
  poll:
    question: "What decides placement?"
    options:
      - "Folder structure"
      - "Diagram whitespace"
      - "Runtime needs"
      - "The loudest service"
    preferred: "Runtime needs"
fortune:
  quote: "Placement is a behavior decision disguised as a deployment decision."
related:
- "runtime-layout-not-maven"
- "portability-without-handwaving"
tags:
- "deployment"
- "runtime"
- "placement"
- "decision"
---

# Who decides where a pipeline runs?

## Elevator answer

**Place work together until ownership, scaling, security, latency, or failure isolation gives you a concrete reason not to. Package names are not placement strategy.**

<CoffeeMisconceptions />

## The real explanation

If PDF rendering needs ten times the memory of order validation, or Fulfillment belongs to another team with its own lifecycle, a runtime boundary may earn its cost. If two steps merely sit in different packages, sending them over HTTP gives the package structure latency and a pager. Runtime placement is a production decision, not diagram feng shui.

TPF treats transport and platform as orthogonal dimensions. gRPC, REST, and LOCAL are transport modes. Function-style and container-oriented deployments are platform or deployment choices. A flow may be local today, gain a remote boundary tomorrow, or use a function pattern for a particular entry point. The business semantics should remain coherent while the generated adapters and runtime mapping reflect the chosen placement.

The right placement emerges from trade-offs. Co-locate work that needs low latency, shared state, or simple operation. Split work that has separate ownership, security, scaling, availability, or failure isolation needs. Place a high-volume connector where it can be operated without forcing unrelated business behavior to scale with it. Do not split merely because an internal step looks important on a diagram.

The framework makes this explicit because accidental placement is expensive. If a local method silently becomes a remote call, its failure, cardinality, tracing, and retry behavior change. If build topology is mistaken for runtime layout, a refactor can produce a deployment claim nobody actually validated. TPF’s model allows the compiler and generator to participate in the decision rather than letting a package boundary invent it.

The cost is that teams must make the decision in plain language. A runtime mapping is not an infrastructure footnote. It is part of how the application behaves under latency, failure, and load.

## Trade-offs

TPF gains an explicit place to reason about deployment behavior. It gives up the accidental simplicity of “the module decides.” Teams must maintain the mapping as architecture changes.

## When TPF is not a good fit

If a system has one small local runtime and no credible reason to vary placement, keep the layout simple. Explicitness should not become topology theatre.
