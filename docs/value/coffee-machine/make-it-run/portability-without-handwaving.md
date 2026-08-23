---
title: "Can one flow run everywhere without becoming vague?"
faq:
  id: "portability-without-handwaving"
  track: "deployment"
  question: "Can the same pipeline run locally, in Kubernetes, and as a function?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "kubernetes-kai"
      text: "Portability means never reading the deployment docs."
    - persona: "kubernetes-kai"
      text: "Every workload is a function after a timeout."
    - persona: "kubernetes-kai"
      text: "If it works on my laptop, topology has been solved."
social:
  poll:
    question: "Portability means…"
    options:
      - "Same code, no checks"
      - "One cloud forever"
      - "Supported mappings"
      - "A bigger diagram"
    preferred: "Supported mappings"
fortune:
  quote: "Portable semantics are useful; portable assumptions are usually expensive."
related:
- "runtime-placement-is-a-decision"
- "generation-not-a-container-factory"
tags:
- "deployment"
- "portability"
- "handwaving"
---

# Can one flow run everywhere without becoming vague?

## Elevator answer

**The pricing decision can stay Java while its host changes. REST, gRPC, LOCAL, containers, and functions are not interchangeable costumes; each needs a supported adapter and an honest runtime mapping.**

<CoffeeMisconceptions />

## The real explanation

`CalculatePrice` should not learn HTTP because production uses REST or Lambda because somebody wants functions. That is useful portability. Claiming REST, gRPC, LOCAL, containers, and functions behave identically is hand-waving with billing consequences. Each target needs a compiler path, generated adapter, lifecycle, and representative test that actually exists.

The boundaries remain real. LOCAL, REST, and gRPC are transport modes with different invocation behavior. A function-style deployment is a platform or deployment pattern, not another transport mode. Latency, startup, concurrency, credentials, and operational limits still vary. TPF makes these choices explicit so the generated adapter and runtime contract can match the intended environment.

This allows a team to begin locally, deploy a supported container runtime to Kubernetes, or select a function platform for a suitable entry point without rewriting the core. It does not justify a claim of parity where tests, bindings, or runtime support do not exist. Honest portability is specific about which semantic guarantees travel and which operational characteristics must be revalidated.

The trade-off is less magical language. “Write once, run anywhere” sounds better than “declare the supported runtime mapping and verify it.” The second is how a team avoids discovering that a blocking dependency, missing descriptor, or unsupported integration changed the flow after deployment.

## Trade-offs

TPF gains portable typed semantics across supported environments. It gives up universal parity claims. Teams must validate each selected transport and platform path.

## When TPF is not a good fit

If an application is deeply tied to one platform primitive and portability has no value, keep that coupling explicit. Do not introduce a framework abstraction solely to hide a deliberate decision.
