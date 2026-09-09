---
title: "Is this Clean Architecture with a code generator and a fake moustache?"
faq:
  id: "clean-architecture-with-a-fake-moustache"
  track: "why-tpf-exists"
  question: "What problem does TPF solve that Clean Architecture does not?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "hexagonal-helen"
      text: "Rename the adapter `PortAdapterPort`, then no production failure can cross the boundary."
    - persona: "enterprise-edna"
      text: "Architecture is complete once every layer has a PowerPoint colour and a package prefix."
    - persona: "codegen-carl"
      text: "Generate enough classes and the business will become decoupled by exhaustion."
social:
  poll:
    question: "What does TPF add to ports and adapters?"
    options:
      - "A longer package name"
      - "A generator that makes"
      - "Typed business flow"
      - "A mandatory hexagon-shaped"
    preferred: "Typed business flow"
fortune:
  quote: "Ports protect the core’s direction; a pipeline makes the journey between ports a contract."
related:
- "where-business-logic-lives"
- "hiding-io-without-hiding-reality"
- "method-chain-with-a-press-release"
tags:
- "why-tpf-exists"
- "clean"
- "architecture"
- "fake"
- "moustache"
---

# Is this Clean Architecture with a code generator and a fake moustache?

## Elevator answer

**Clean Architecture keeps `ChargeCard` from depending on Stripe. TPF also checks how the request reaches it, what maps into it, what happens next, and which adapter must exist at runtime.**

<CoffeeMisconceptions />

## The real explanation

Clean and Hexagonal Architecture gave us a useful rule: the pricing policy should not import Hibernate, Kafka, or Stripe's SDK. Put those behind ports and point dependencies toward the business. TPF agrees. Drawing a hexagon around the same tangled method would be an expensive fake moustache.

Agreement on dependency direction is not the same as agreement on how an application flow should be made executable. Clean and Hexagonal Architecture are primarily architectural disciplines. They tell you how responsibilities and dependencies should relate. They do not, by themselves, supply a shared build-time contract for a flow’s step order, mapper compatibility, cardinality, transport requirements, generated adapters, telemetry metadata, or replay semantics. A skilled team can build all of that on top of them. Many do, repeatedly.

TPF chooses to make those repeated concerns first-class. Its pipeline model is not a replacement diagram for ports and adapters. It is a compiled description of a typed flow that can drive validation and generation. The model can say that a connector admits an external request, a business step transforms it, an operator affects execution, another connector publishes an outcome, and a particular runtime layout or transport is required. The framework then has enough information to reject mismatches and to generate the shell consistently.

The distinction is most useful in the gaps between well-designed layers. A port may say that an external service can be called. It does not automatically say which domain/external mapper is valid, whether a split preserves lineage, whether a remote binding has a descriptor, how a failure is retried, or whether the generated telemetry describes the same step ordering as the compiler. Those are not arguments against ports. They are the places where a team often writes a small private framework, then spends several years explaining why it is almost standard.

TPF also takes a stronger position on operational semantics. A boundary that waits for a completion must retain durable wait state and correlation; an accepted checkpoint handoff transfers ownership of retry and DLQ behavior; a retry must respect stable identifiers and replay. Clean Architecture can accommodate all of that. It does not choose it for you, validate it uniformly, or generate the runtime details from a shared model. TPF does, within its supported scope.

There is a risk in this choice. A framework can mistake a useful convention for a universal ontology. TPF should not turn every port, domain object, or application use case into a pipeline artifact. Nor does code generation absolve a team from understanding its generated adapters. The code is still part of the contract and should be inspected when a boundary changes.

The practical relationship is therefore complementary. Clean or Hexagonal Architecture can tell a team why business policy should not depend on Kafka or a web controller. TPF can make a selected flow’s boundaries, mappings, and operational behavior concrete enough for the compiler and runtime to enforce. One protects the direction of the architecture; the other gives recurring execution structure a name and a machine-checkable shape.

This distinction also explains why TPF should not be introduced as an architecture-replacement programme. A team can retain its existing boundary discipline and use the pipeline model only where repetition has revealed a missing execution contract. The goal is to make a valuable architectural intention executable, not to award a new label to the same package diagram.

In other words, a port can remain a port and an adapter can remain an adapter. TPF is valuable when the relationship between them belongs to an important flow that the team wants to compile, observe, and evolve as one coherent unit rather than a chain of independently reasonable local choices.

## Trade-offs

TPF gains executable consistency beyond dependency direction. It gives up some framework neutrality at the flow boundary: teams adopt TPF’s model, compiler, and generated artifacts rather than writing every adapter themselves. This is a worthwhile exchange only when the repeated operational work is genuinely repeated.

## When TPF is not a good fit

If a team already has a small, disciplined hexagonal application with few external boundaries and no recurring execution problems, Clean Architecture may be sufficient. Do not adopt TPF to receive a more elaborate vocabulary for dependencies the team already manages well.
