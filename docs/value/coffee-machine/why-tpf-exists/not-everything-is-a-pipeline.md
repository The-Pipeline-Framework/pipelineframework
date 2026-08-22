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

**No. A CRUD endpoint that changes a display name can remain a CRUD endpoint. Reach for TPF when the work crosses a boundary that can retry, wait, duplicate, or wake somebody up.**

<CoffeeMisconceptions />

## The real explanation

Frameworks become dangerous when every problem begins to resemble their logo. `trimCustomerName()` does not need an execution graph. Neither does every repository query, controller, loop, or batch helper. Sometimes the correct runtime authority is still “the current thread.”

TPF starts earning its keep when work leaves the easy local transaction: an HTTP request becomes a payment Command, a Kafka handoff, a durable Await, and a callback tomorrow. Now retries, duplicate delivery, mappings, generated adapters, and ownership are part of the feature. Leaving that itinerary inside scattered annotations makes both change and incident response unnecessarily fragile.

That is not the territory of every line of application code. A simple CRUD screen may only read and update data in one local transaction. A report builder may be clearer as ordinary Java. A migration script is often most honest as a script. A small integration adapter may need direct, explicit I/O. Turning any of these into a pipeline can increase indirection without increasing safety. “The framework can model it” is not the same as “the framework should model it.”

TPF’s constraints should therefore be applied at the boundary of a business capability, not sprayed across an application. A pipeline is useful when it gives a team one place to describe the behavior that has already become distributed in responsibility: step contracts, mappings, admission, publication, retries, persistence, and runtime placement. It is not useful as a substitute name for a package or a way to make an ordinary method look significant.

Flows need not march in a straight line. A risk result can branch through a declared union, one order can expand into line-item checks, results can reduce again, and an Await can stop until a callback arrives. But “can the graph express it?” comes after “does this belong in one business journey?” A tangle of unrelated capabilities needs scissors, not more arrows.

`ProcessCustomerEverything` is not rescued by typed steps. A good flow starts with something recognisable, such as `PlaceOrder`, and ends with a result the owning team can explain. Do not append loyalty renewal merely because the customer record is already passing through. Types make the god flow easier to inspect; they do not make it less divine.

There is an unavoidable trade-off. TPF is opinionated enough to make some designs inconvenient. That is intentional: if a flow crosses transports, uses connectors, or requires durability, the framework wants the team to state how. But it must leave space for code that does not need those guarantees. A framework that insists on occupying every room eventually becomes the monolith it promised to organize.

The memorable rule is modest: use a pipeline when the flow is the thing that needs to be understood, verified, and operated. Use ordinary code when the flow would be an invented story around a simple local task.

Teams should make the decision visible in review. “This is deliberately not a pipeline because it is local and has no independent operational boundary” is a good architectural statement. It protects against both cargo-cult framework adoption and the opposite reflex, where a consequential distributed flow is kept informal only because a smaller predecessor was simple.

## Trade-offs

TPF gains focus and shared semantics for important flows. It gives up a single universal programming model; teams must exercise judgment about where a pipeline begins and ends. That judgment cannot be outsourced to a generator or a naming convention.

## When TPF is not a good fit

TPF is not a good fit for trivial local work, basic CRUD with no consequential boundary, exploratory prototypes, or a domain whose primary challenge is not application execution. It is also a poor fit when a team wants every unusual case to bypass the declared model; that produces both framework cost and no framework benefit.
