---
title: "What exactly does code generation generate?"
faq:
  id: "generation-not-a-container-factory"
  track: "deployment"
  question: "Does code generation create containers or only application code?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "codegen-carl"
      text: "The compiler now owns production because it wrote Java."
    - persona: "kubernetes-kai"
      text: "A generated class is practically a container image."
    - persona: "build-barry"
      text: "CI is optional once code generation has feelings."
social:
  poll:
    question: "Generation creates…"
    options:
      - "A new cloud provider"
      - "A container orchestra"
      - "Application adapters"
      - "Fewer excuses"
    preferred: "Application adapters"
fortune:
  quote: "Generation makes the application contract executable; it does not replace the platform that delivers it."
related:
- "deploy-without-a-new-religion"
- "runtime-layout-not-maven"
tags:
- "deployment"
- "generation"
- "container"
- "factory"
search: false
---

# What exactly does code generation generate?

## Elevator answer

**TPF can generate the handler, adapter, metadata, and binding implied by the flow. It does not build the image, create the cluster, choose the subnet, or negotiate with Finance.**

<CoffeeMisconceptions />

## The real explanation

Generation is neither a glorified getter factory nor a small cloud provider. From a declared flow, TPF can produce handlers, adapters, bindings, metadata, and runtime descriptions that would otherwise drift across hand-written files. Your build still packages them; Terraform still provisions things; Kubernetes still runs them. The generator has enough responsibility already.

That generated code is part of the contract, not disposable decoration. Pipeline order, telemetry metadata, branching information, platform and transport descriptions, and semantic step descriptors should align with the compiler model. This is why a missing binding or incompatible mapper is better discovered during generation than as a surprise behind a remote call.

Containers are still containers. Your normal build produces artifacts and images; your normal deployment tooling applies manifests, policies, secrets, and promotion rules. TPF does not make a sidecar appear because a pipeline has an await, nor does it decide that a service mesh is necessary because a connector is remote. It makes the application boundary explicit enough that those platform choices can be made against something real.

Generated code also deserves normal engineering discipline. It should be reproducible, reviewable where useful, stable enough for build checks, and tested through the supported compiler/runtime paths. Teams should not have to read every generated line in an incident, but they must be able to trace generated behavior back to a declared flow and its configuration.

The trade-off is responsibility. A generator reduces repeated glue; it does not absolve a team from understanding the contract it declared or the runtime it deploys. That is a healthier bargain than handwritten integration code that nobody owns consistently.

## Trade-offs

TPF gains repeatable adapter generation and contract metadata. It gives up the illusion that generation owns delivery. Teams retain CI, image, security, and deployment responsibilities.

## When TPF is not a good fit

If a team needs a managed platform that provisions cloud resources and runs every delivery stage, use an infrastructure platform. TPF’s scope is the generated application boundary.
