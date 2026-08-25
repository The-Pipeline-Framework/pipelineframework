---
title: "How do I deploy this without learning a new religion?"
faq:
  id: "deploy-without-a-new-religion"
  track: "deployment"
  question: "How do I deploy a pipeline application to Kubernetes?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "kubernetes-kai"
      text: "Everything becomes a Helm chart after the second YAML file."
    - persona: "platform-priya"
      text: "A generated runtime must bring its own cloud constitution."
    - persona: "consultant-nigel"
      text: "First create a deployment centre of excellence."
social:
  poll:
    question: "Who owns Kubernetes?"
    options:
      - "The YAML, naturally"
      - "The application generator"
      - "Your platform team"
      - "Whoever has cluster admin"
    preferred: "Your platform team"
fortune:
  quote: "TPF defines the application boundary; Kubernetes still runs the actual application."
related:
- "one-pipeline-one-container"
- "platform-team-boundary"
tags:
- "deployment"
- "deploy"
- "new"
- "religion"
search: false
---

# How do I deploy this without learning a new religion?

## Elevator answer

**Keep the Dockerfile, Helm chart, probes, policies, and on-call habits. TPF generates application/runtime pieces; Kubernetes still decides where the pod goes and whether it is alive.**

<CoffeeMisconceptions />

## The real explanation

Your platform already knows how to build an image, inject secrets, expose a service, run probes, enforce policy, scale pods, and wake somebody when they burn. A generated TPF runtime enters that machinery as an application. It does not arrive with tablets announcing a replacement for Kubernetes.

What TPF contributes is a clearer source for the runtime’s behavior. The pipeline model owns the typed flow, declared connectors, transport and platform choices, and generated artifacts that implement the necessary edges. Kubernetes then runs the resulting deployable. The distinction is important: runtime layout is the logical shape of an application flow; build topology and containers are the physical structure used to ship it. They influence one another, but they are not interchangeable.

This means existing manifests, service meshes, ingress, security policies, secrets management, and deployment automation can remain. A platform team can apply its normal controls without needing to own business rules. The generator should produce an application that fits into that environment, not an opaque machine that operators are asked to accept on faith.

There are still choices. A flow that includes a highly loaded connector may need different scaling behavior from a local in-process path. A function-style deployment may have different lifecycle and startup constraints from a container runtime. TPF makes those decisions visible in the model; it does not select replicas or capacity targets by reading the team’s hopes.

The trade-off is that teams must learn where the framework stops. TPF does not create a production topology simply because it generated adapters. Kubernetes does not understand business cardinality merely because it can scale pods. A credible deployment joins the two: typed flow semantics from the application and operational controls from the platform.

## Trade-offs

TPF gains generated, declared application boundaries. It gives up a fantasy of infrastructure-free deployment. Teams retain responsibility for manifests, capacity, policy, and operations.

## When TPF is not a good fit

If a team wants a product that owns the whole Kubernetes platform, TPF is the wrong scope. It is an application framework, not a replacement cluster control plane.
