---
title: "What happens to controllers that already work?"
faq:
  id: "controllers-are-not-the-enemy"
  track: "bring-your-existing-app"
  question: "Do we have to rewrite our REST controllers into pipelines?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "spring-sam"
      text: "A controller is a domain service after it has enough annotations."
    - persona: "spring-sam"
      text: "If HTTP status codes leave the controller, the business has gained protocol literacy."
    - persona: "enterprise-edna"
      text: "Move the controller into a package called pipeline and declare the migration complete."
social:
  poll:
    question: "What should a controller own?"
    options:
      - "Every rule that arrives"
      - "The whole distributed"
      - "Typed business flow"
      - "Nothing; delete the web"
    preferred: "Typed business flow"
fortune:
  quote: "A controller is a useful doorway; it should not have to be the whole building."
related:
- "keep-spring-boot"
- "migrate-one-capability"
tags:
- "bring-your-existing-app"
- "controllers"
- "enemy"
---

# What happens to controllers that already work?

## Elevator answer

**No. Controllers remain useful transport adapters; TPF lets them admit typed requests into selected flows without turning HTTP details into business logic.**

<CoffeeMisconceptions />

## The real explanation

Controllers are not an architectural mistake. They translate an HTTP request into application work and translate an outcome back into HTTP. They own routes, authentication integration, headers, request binding, and response status. The problem begins only when a controller becomes the place that decides business policy, talks directly to several external systems, creates retry behavior, and silently defines the order of a distributed flow.

TPF does not ask a team to hide or delete a working controller. It gives the controller a clearer job: admit a typed request into a declared flow and return the appropriate result. The controller can remain an adapter while the pipeline expresses the business execution contract behind it. Historical or fresh read concerns belong behind an explicit Query/read side, not in controller code that quietly reconstructs persisted pipeline state. This preserves the familiar web edge and makes the more consequential behavior reviewable without forcing HTTP or persistence vocabulary into every business step.

Migration can begin with the controller untouched. Put the existing request and response mapping behind an explicit boundary, extract a typed decision, and keep existing exception conventions while tests compare old and new outcomes. Only move a concern when its ownership becomes clearer. A controller that simply maps input to a flow is not redundant; it is exactly the kind of imperative shell TPF expects.

This distinction improves later change. If a business flow must also be admitted from gRPC, a function-style runtime, or a message connector, the domain behavior need not learn a second or third transport vocabulary. Each adapter can translate to the typed flow contract. That does not make transports interchangeable; REST, gRPC, and LOCAL remain distinct modes. It makes the business decision less hostage to the first endpoint that happened to call it.

The cost is another seam to understand. A team must decide where request validation ends and domain validation begins, and it must avoid duplicating rules on both sides. HTTP-shaped concerns remain at the controller. Business and execution facts move through the typed flow. Historical reads use an explicit Query/read side. The pipeline composes these responsibilities without collapsing them into the endpoint.

## Trade-offs

TPF gains transport-neutral business execution while retaining familiar web adapters. It gives up the convenience of treating the controller as the entire use case. Teams must keep mappings and validation responsibilities distinct.

## When TPF is not a good fit

For a simple local endpoint with no meaningful operational boundary, a controller and focused service may be clearer. Do not create a pipeline only because the route has a verb in it.
