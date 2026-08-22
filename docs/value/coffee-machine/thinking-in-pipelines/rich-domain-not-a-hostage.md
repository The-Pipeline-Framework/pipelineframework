---
title: "Will this turn my rich domain into a polite data transfer object?"
faq:
  id: "rich-domain-not-a-hostage"
  track: "domain-modelling"
  question: "Does TPF encourage an anemic domain model?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "ddd-diego"
      text: "If an entity has a getter, it is already one refactor away from moral collapse."
    - persona: "spring-sam"
      text: "Put every rule in the service layer; the entity is busy being serializable."
    - persona: "microservice-mike"
      text: "Make each aggregate a service, then invariants can negotiate over HTTP like adults."
social:
  poll:
    question: "Where should an aggregate invariant live?"
    options:
      - "In the controller, where"
      - "In the pipeline, because"
      - "Local aggregate invariant"
      - "In a shared utility"
    preferred: "Local aggregate invariant"
fortune:
  quote: "A pipeline can coordinate a decision without becoming the only place that decision has a meaning."
related:
- "application-service-in-a-trench-coat"
- "cross-aggregate-rules"
- "no-ddd-language-police"
tags:
- "domain-modelling"
- "rich"
- "domain"
- "hostage"
---

# Will this turn my rich domain into a polite data transfer object?

## Elevator answer

**No. Pipelines coordinate typed execution while entities, value objects, policies, and domain services retain the behavior and invariants that give the model meaning.**

<CoffeeMisconceptions />

## The real explanation

Anemic-domain-model anxiety is warranted because orchestration frameworks often create it by accident. Once a team has a place called “flow,” “process,” or “orchestrator,” it can become tempting to place every business decision there. Entities become records, value objects become transport bags, and the interesting rules dissolve into a long coordinator that happens to know every field. The result has a strong central narrative and weak local meaning.

TPF does not need that arrangement. Its functional core is meant to contain typed business transformations and decisions. Those can live in rich domain entities, value objects, policies, specifications, or focused domain services, depending on the language the team already uses. A pipeline coordinates the work around those concepts: it accepts a typed input, invokes domain behavior, passes a result to the next typed step, and places declared operational boundaries around the flow. It should not become the place that decides what an order, payment, or customer is allowed to mean.

Aggregates remain particularly important. An aggregate defines a consistency boundary and protects invariants that must hold together. TPF cannot make a cross-aggregate update magically transactional just because the steps appear next to one another in a pipeline definition. If an invariant belongs inside an aggregate, keep it there. The pipeline may load or receive the aggregate, call a meaningful operation, and arrange what follows. It does not get special permission to reach inside several aggregates and edit their state because it has a helpful-looking flow diagram.

This distinction gives the model two useful scales. At the small scale, a domain object can say approve, reserve, or applyPolicy and enforce its own rules without knowing whether it was called from REST, a local execution, or a replay. At the larger scale, a pipeline can say how a customer request enters, which facts are needed, where a connector boundary lies, and how the outcome is published or persisted. The domain is reusable because it does not learn the shell’s vocabulary; the shell is observable because it does not have to guess what the domain did.

Domain services and policies still have a place when a rule does not naturally belong to one entity. A pricing policy may combine facts without owning their lifecycle. A cross-aggregate rule may decide whether a request is permitted, then ask separate aggregates to perform their own legitimate transitions. TPF’s preference is not “all business logic in entities.” Its preference is “business meaning remains in typed domain behavior, rather than being disguised as client calls, transport adapters, or pipeline plumbing.”

There is a useful warning in the other direction. A rich domain model can become so self-contained that it quietly performs persistence, messaging, or remote lookups. That does not make it richer; it makes its dependencies harder to see and test. TPF’s shell boundary is a reminder that a domain object should not need to know a Kafka topic, a REST path, or a retry policy to protect an invariant. It can receive the facts it needs, make its decision, and return an explicit result.

The cost is disciplined modeling. Teams cannot use a pipeline as an excuse to avoid learning their domain, and they cannot use “rich domain” as a reason to hide external effects inside entities. That may feel slower than putting a transaction script in one familiar service. It creates code that changes along the same lines as the business, rather than code that accumulates every concern that happened to arrive through the same endpoint.

The memorable idea is that pipelines coordinate behavior; they do not supply the behavior’s meaning. If a pipeline becomes the only place an invariant can be understood, it is asking to be refactored. If an entity is reduced to a passive bag because a pipeline wants control, the framework is being used against its own boundary.

## Trade-offs

TPF gains transport-neutral business behavior and clearer aggregate boundaries. It gives up the convenience of a single coordinator that edits every object and calls every dependency. Teams must be explicit about where a policy belongs and how facts enter the core.

## When TPF is not a good fit

If the team prefers transaction scripts and has no intention of maintaining rich domain behavior, TPF’s separation may add vocabulary without producing value. It is also not a substitute for learning DDD; teams may use the framework without DDD terminology, but they still need sound ownership and invariants.
