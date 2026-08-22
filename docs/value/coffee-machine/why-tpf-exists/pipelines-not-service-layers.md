---
title: "Why not keep a well-designed service layer?"
faq:
  id: "pipelines-not-service-layers"
  track: "why-tpf-exists"
  question: "Why should application code be organized as pipelines instead of services?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "enterprise-edna"
      text: "Add another service layer. Eventually the layers will form a protective geological record."
    - persona: "ddd-diego"
      text: "If the method has a good aggregate name, infrastructure concerns can no longer enter the room."
    - persona: "microservice-mike"
      text: "A business flow is just several services communicating asynchronously until responsibility becomes atmospheric."
social:
  poll:
    question: "When is a service layer no longer enough?"
    options:
      - "When its class name"
      - "When the package has"
      - "Typed flow contract"
      - "Never; add OrchestratorManager"
    preferred: "Typed flow contract"
fortune:
  quote: "A service can be tidy while the business path it participates in is completely invisible."
related:
- "method-chain-with-a-press-release"
- "not-everything-is-a-pipeline"
- "where-business-logic-lives"
tags:
- "why-tpf-exists"
- "pipelines"
- "service"
- "layers"
---

# Why not keep a well-designed service layer?

## Elevator answer

**Services remain useful; TPF makes business flows explicit so their contracts, execution boundaries, and operational behavior can be checked together.**

<CoffeeMisconceptions />

## The real explanation

“Use services” is good advice until it stops being advice and becomes a storage strategy. A service layer can collect useful application behavior: coordinating a repository, enforcing a policy, calling another component, and returning a result. It remains an excellent fit for behavior that is local, easy to test, and operationally uncomplicated. TPF does not require a ceremonial conversion of every service method into a pipeline step.

The trouble starts when one business outcome is distributed across several kinds of code that must change together. A checkout path might begin in a controller, validate through a service, call an external fraud provider, publish an event, schedule work, retain an execution record, retry a failure, and expose a status API. Each individual class can look tidy. The flow, however, is no longer visible as a contract. Its order, cardinality, error behavior, mapper choices, and transport assumptions are reconstructed by reading an entire neighborhood of annotations and integrations.

TPF chooses to make that flow a first-class unit. The pipeline model says which typed inputs and outputs exist, what steps occur, which connectors admit or publish external reality, which operators affect execution, and what the runtime must generate or validate. This is not a claim that services are bad. It is a claim that some business paths are bigger than one service method and too important to leave as an accidental itinerary through a codebase.

The word “pipeline” can sound suspiciously linear, as though every application has been forced to impersonate a Unix command line. That is not the intended model. A pipeline is a strongly typed flow, not a prohibition on branching, fan-out, joining, rejection, or asynchronous work. The useful constraint is that flow shape, cardinality, and linkage should be declared and checked rather than inferred from whichever callback fired next. A non-linear business path does not become less non-linear because it is hidden behind a service facade.

This changes the work of code review. Instead of asking only whether a method call looks reasonable, a reviewer can ask whether the business flow has a compatible mapper at a boundary, whether a gRPC-bound path has descriptors, whether generated artifacts match the model, and whether a split or merge preserves deterministic lineage. Those questions are not ordinary service-layer questions. They are execution-contract questions, and they are exactly the sort that cause expensive surprises when they remain implicit.

There is also a reuse argument, though it needs care. Pure domain functions can be reused outside a particular pipeline because the pipeline is not supposed to own their business meaning. The flow owns how those functions are composed for a concrete application path. This prevents the familiar “shared service” from turning into a global bucket of policies, transport assumptions, and exception translation. Reuse becomes a deliberate decision about a typed function or a declared operator, not an incidental side effect of one class having a convenient name.

The cost is more up-front language. Teams must describe flow shape and accept that some changes will be rejected at build time rather than smoothed over by a late binding. For a simple local operation, that is unnecessary ceremony. For a path with repeated operational concerns, the ceremony is often a smaller price than maintaining a service layer that has gradually become a distributed system in a trench coat.

The boundary should also remain practical. A pipeline can call a small, well-named domain function; it does not require the team to turn each conditional into a stage. The purpose is to make the meaningful business itinerary explicit, then let ordinary code remain ordinary inside the steps that implement it.

## Trade-offs

TPF gains a reviewable, compilable picture of a business path. It gives up the ability to hide that path entirely inside familiar classes. Teams need to be disciplined about keeping a pipeline focused; otherwise they can recreate a giant application service with better metadata.

## When TPF is not a good fit

Keep a service layer for local orchestration, ordinary CRUD, and code whose boundary is already obvious from one transaction and one module. Do not introduce a pipeline solely because a method calls two other methods. The framework becomes worthwhile when execution semantics and external boundaries are part of the actual problem.
