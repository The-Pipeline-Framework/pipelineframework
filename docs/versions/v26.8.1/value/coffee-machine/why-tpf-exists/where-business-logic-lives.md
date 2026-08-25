---
title: "Why is TPF so fussy about where business logic lives?"
faq:
  id: "where-business-logic-lives"
  track: "why-tpf-exists"
  question: "Why is TPF opinionated about where business logic belongs?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "spring-sam"
      text: "Business logic belongs wherever the injected `KafkaTemplate` is closest."
    - persona: "enterprise-edna"
      text: "Put every decision in the service, every service in the base class, and every base class in a diagram."
    - persona: "functional-fran"
      text: "If the function is pure, a customer will eventually materialize somewhere out of philosophical respect."
social:
  poll:
    question: "Where should retry and transport policy live?"
    options:
      - "In whichever service found it"
      - "In a 900-line Utils"
      - "Typed business flow"
      - "In the sprint retrospective"
    preferred: "Typed business flow"
fortune:
  quote: "A business decision should not have to learn the dialect of every system that carries it."
related:
- "hiding-io-without-hiding-reality"
- "clean-architecture-with-a-fake-moustache"
- "pipelines-not-service-layers"
tags:
- "why-tpf-exists"
- "where"
- "business"
- "logic"
- "lives"
search: false
---

# Why is TPF so fussy about where business logic lives?

## Elevator answer

**The rule “decline payments over this limit” should receive facts and return a decision. REST, Kafka, retries, rows, and Stripe belong around it—not inside its `if`.**

<CoffeeMisconceptions />

## The real explanation

`amount.compareTo(limit) > 0` should mean the same thing whether the request arrived through REST, Kafka, a scheduled job, or a test. The retry counter, database row, Stripe request, and telemetry span matter enormously to the working system. They still have no vote in the payment policy.

Many applications begin with that distinction intact. Then a service method acquires a repository, an HTTP client, a message producer, a retry annotation, a transaction boundary, a metrics call, and a cache. None of those additions is individually foolish. The problem is cumulative: the code now explains a business rule and a changing set of operational policies in the same breath. The team cannot move one concern without fearing an accidental change to the other.

TPF chooses the functional-core, imperative-shell split as a practical boundary. The functional core is not an academic demand that every application become a theorem. It means business functions should focus on typed transformations and decisions. The imperative shell owns captured external reality and the operational work around it: connectors, transport adapters, retry behavior, persistence, caching, telemetry, correlation, await handling, and generated deployment integration. The point is not that I/O disappears. The point is that I/O has an address.

This matters particularly when the same business capability is reached in more than one way. An order acceptance rule may be exercised by a local test, a REST endpoint, a gRPC client, or a function-style deployment. If transport-specific details live inside the rule, every new entrance teaches the domain another dialect. If the rule stays typed and transport-neutral, the generated and declared adapters can change without turning the business decision into a museum of infrastructure choices.

The boundary also makes failure semantics less slippery. A retry is not just a try/catch loop; it can affect duplicate execution, idempotency, state capture, and what the caller sees. An await is not just a future; it needs durable wait state, correlation, timeout, duplicate completion handling, and replay-safe resume behavior. TPF locates those concerns in the shell because they must be consistent across flows and adapters. Letting every business step improvise them is flexible in the same way that every team choosing its own calendar is flexible.

Opinionated placement does not mean “business code never talks to the outside world under any circumstances.” A connector may legitimately model admission or publication at a domain-relevant boundary. A business function may need facts that originated outside the system. The framework’s question is whether that external interaction is declared and owned as a boundary, or smuggled into a step as an incidental client call. The first can be validated, observed, retried, and replaced deliberately. The second is often discovered by a timeout graph.

The cost is that developers surrender a little local convenience. It can feel slower to declare a connector or model a boundary than to inject a client and call it. That feeling is real, especially for a one-off use case. TPF argues that the delay is worthwhile when the interaction participates in a flow whose reliability and portability matter. It is not a moral verdict on the shortcut; it is a prediction about where the shortcut tends to lead.

The mental model is simple: business logic says what should be true; the shell makes that truth survive contact with systems, failures, and deployments. Both are necessary. Keeping them distinguishable is TPF’s opinion.

It is a boundary to revisit, not a purity test to pass once. When a supposedly pure step starts acquiring correlation IDs, client configuration, and retry policy, the design is signalling that a shell concern is leaking inward. When a connector quietly decides a discount policy, the opposite leak is occurring. The framework gives those conversations a shared vocabulary.

## Trade-offs

TPF gains reusable domain functions and consistent operational semantics. It gives up the immediate convenience of placing every dependency beside the method that first needs it. Teams must design connectors and boundaries thoughtfully; a poor boundary is still poor architecture, even when it is declared.

## When TPF is not a good fit

If a component is inherently an infrastructure adapter, putting infrastructure concerns there is correct. A database migration tool, an HTTP gateway, or a one-off administrative script does not become healthier by pretending it has a functional core. TPF is most useful where durable business behavior must outlive its current delivery mechanism.
