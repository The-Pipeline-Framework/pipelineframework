---
title: "Does every application now have to become a pipeline?"
faq:
  id: "not-everything-is-a-pipeline"
  track: "why-tpf-exists"
  question: "Isn’t this forcing every application into the same architectural shape?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "framework-fred"
      text: "If a getter does not emit telemetry metadata, it is probably hiding from its destiny."
    - persona: "microservice-mike"
      text: "Every branch deserves its own deployable, preferably before lunch."
    - persona: "consultant-nigel"
      text: "Standardize every problem until the exceptions require a transformation programme."
social:
  poll:
    question: "What should become a pipeline?"
    options:
      - "Every method, for consistency"
      - "Only methods with at"
      - "Typed business flow"
      - "Whatever the architecture"
    preferred: "Typed business flow"
fortune:
  quote: "A pipeline is a promise to make a flow explicit, not a tax on every method."
related:
- "pipelines-not-service-layers"
- "not-another-workflow-engine"
- "method-chain-with-a-press-release"
tags:
- "why-tpf-exists"
- "everything"
- "pipeline"
---

# Does every application now have to become a pipeline?

## Elevator answer

**No. TPF is for typed business flows crossing meaningful operational boundaries; local code, CRUD, and bespoke designs remain valid choices.**

<CoffeeMisconceptions />

## The real explanation

Frameworks become dangerous when their favourite abstraction becomes the answer before the question has been understood. TPF’s favourite abstraction is the typed pipeline, so the suspicion is justified: will every controller, repository query, batch script, and internal helper now be pressured into a flow definition because the framework has one available?

The intended answer is no. TPF optimizes for a particular class of work: business flows that need a stable contract across meaningful operational boundaries. Those flows may accept work through a connector, combine typed business steps, reach an external system, run across a declared transport, require generated adapters, record telemetry, retry, wait durably, or hand off ownership. In that territory, leaving the flow shape implicit can make both development and operations unnecessarily fragile.

That is not the territory of every line of application code. A simple CRUD screen may only read and update data in one local transaction. A report builder may be clearer as ordinary Java. A migration script is often most honest as a script. A small integration adapter may need direct, explicit I/O. Turning any of these into a pipeline can increase indirection without increasing safety. “The framework can model it” is not the same as “the framework should model it.”

TPF’s constraints should therefore be applied at the boundary of a business capability, not sprayed across an application. A pipeline is useful when it gives a team one place to describe the behavior that has already become distributed in responsibility: step contracts, mappings, admission, publication, retries, persistence, and runtime placement. It is not useful as a substitute name for a package or a way to make an ordinary method look significant.

This is also the answer to the concern about non-linear flows. The framework does not require a business process to pretend it is linear. It supports explicit flow semantics such as branching, expansion and reduction, operator behavior, and durable awaits where the model supports them. But there is a second question before “can TPF model this graph?”: “is this graph a coherent business execution unit?” If it is a tangle of unrelated capabilities, the correct response is not more pipeline syntax. It is to split the responsibility.

Preventing god flows is partly a modeling discipline. A pipeline should have a meaningful input, a meaningful outcome, and a boundary a team can explain. It should not become the place where every adjacent policy is appended because it is already on the path. Typed contracts and generated metadata make excessive flow size easier to see, but they cannot make a bad ownership decision good. Teams should keep pure functions small, compose capabilities deliberately, and treat a checkpoint handoff as the ownership boundary it is.

There is an unavoidable trade-off. TPF is opinionated enough to make some designs inconvenient. That is intentional: if a flow crosses transports, uses connectors, or requires durability, the framework wants the team to state how. But it must leave space for code that does not need those guarantees. A framework that insists on occupying every room eventually becomes the monolith it promised to organize.

The memorable rule is modest: use a pipeline when the flow is the thing that needs to be understood, verified, and operated. Use ordinary code when the flow would be an invented story around a simple local task.

Teams should make the decision visible in review. “This is deliberately not a pipeline because it is local and has no independent operational boundary” is a good architectural statement. It protects against both cargo-cult framework adoption and the opposite reflex, where a consequential distributed flow is kept informal only because a smaller predecessor was simple.

## Trade-offs

TPF gains focus and shared semantics for important flows. It gives up a single universal programming model; teams must exercise judgment about where a pipeline begins and ends. That judgment cannot be outsourced to a generator or a naming convention.

## When TPF is not a good fit

TPF is not a good fit for trivial local work, basic CRUD with no consequential boundary, exploratory prototypes, or a domain whose primary challenge is not application execution. It is also a poor fit when a team wants every unusual case to bypass the declared model; that produces both framework cost and no framework benefit.
