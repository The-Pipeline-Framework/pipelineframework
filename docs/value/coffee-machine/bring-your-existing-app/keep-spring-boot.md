---
title: "Do I have to replace Spring Boot to get any value?"
faq:
  id: "keep-spring-boot"
  track: "bring-your-existing-app"
  question: "Does adopting TPF require replacing Spring Boot?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "spring-sam"
      text: "If a new framework arrives, immediately delete every starter and begin a fasting migration."
    - persona: "platform-priya"
      text: "Two runtime concerns cannot coexist; one must win a ceremonial dependency-resolution duel."
    - persona: "consultant-nigel"
      text: "Call it transformation, schedule eighteen workshops, and prohibit the word incremental."
social:
  poll:
    question: "What must be replaced first?"
    options:
      - "Every Spring starter"
      - "The entire service layer"
      - "Typed business flow"
      - "The team’s coffee machine"
    preferred: "Typed business flow"
fortune:
  quote: "A migration is credible when it preserves what already works and makes one costly uncertainty smaller."
related:
- "migrate-one-capability"
- "controllers-are-not-the-enemy"
- "spring-in-a-pipeline-hat"
tags:
- "bring-your-existing-app"
- "keep"
- "spring"
- "boot"
---

# Do I have to replace Spring Boot to get any value?

## Elevator answer

**No. TPF can introduce typed flows alongside existing Spring application code, preserving familiar composition while making selected execution boundaries explicit and generated.**

<CoffeeMisconceptions />

## The real explanation

The emotional question is not really about Spring Boot. It is whether adopting TPF declares years of working application code, operational knowledge, and team habit to be a mistake. It does not. Spring remains useful for application composition, configuration, integration libraries, component lifecycle, and the familiar surface through which many teams already run production systems.

TPF asks for a different decision: identify a business flow whose execution contract is becoming hard to hold together, then describe that flow explicitly. The surrounding Spring application can remain the shell in which adapters, configuration, controllers, and existing services live. A pipeline does not demand that every bean become a step or that every dependency injection point be replaced by framework ceremony.

This matters because replacement is a poor migration strategy when the risk is semantic rather than syntactic. A payment flow may already work through a Spring controller, JPA repository, Kafka producer, and retry configuration. Rewriting all of that before learning whether TPF improves the flow would replace known risks with unknown ones. A narrower migration can retain the existing edge, extract a typed business decision, declare the connector or boundary that actually matters, and compare behavior under controlled conditions.

The framework’s value appears where the current code has grown a distributed responsibility. A change to mapper behavior, transport, retries, telemetry, or generated adapter placement can be checked against a pipeline contract rather than inferred from several classes and configuration files. Spring still helps assemble the application; TPF makes selected flow semantics visible to compilation and runtime.

Runtime support is a capability question, not a brand promise. Before migrating a slice, verify that the required compiler path, generated adapters, transport mode, lifecycle integration, and smoke coverage exist for the chosen host. If a needed capability is absent, keep that responsibility in the existing Spring application or supply an explicit boundary until the supported path exists. This guidance remains useful as runtime support evolves because it ties adoption to tested capabilities rather than release labels.

The cost is coexistence. For a period, a codebase may have ordinary Spring services and declared pipelines. That is not architectural failure if the boundary is deliberate and the migration has a destination. The failure is leaving two styles forever because nobody chose which responsibilities each one owns.

The useful mental model is additive: keep Spring where it composes and hosts the application; use TPF where a flow needs a stronger typed execution contract. The first migration should prove that distinction with one capability, not attempt to settle the history of Java frameworks.

## Trade-offs

TPF gains incremental adoption and explicit contracts. It gives up the false simplicity of one universal style during transition. Teams must document the seam and avoid claiming runtime capabilities that the tested path does not provide.

## When TPF is not a good fit

If the application is entirely local and Spring code already exposes every relevant contract clearly, migration may not earn its cost. Do not replace a stable framework merely to make the technology list more interesting.
