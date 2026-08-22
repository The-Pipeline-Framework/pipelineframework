---
title: "Does hiding I/O make the important bits harder to see?"
faq:
  id: "hiding-io-without-hiding-reality"
  track: "why-tpf-exists"
  question: "Doesn’t hiding I/O make side effects less visible?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "kafka-frank"
      text: "If the domain does not contain a topic name, nobody can prove the event really happened."
    - persona: "hexagonal-helen"
      text: "Add two interfaces and a factory; the side effect has now been ethically sourced."
    - persona: "kubernetes-kai"
      text: "The I/O boundary is the cluster. Everything inside the cluster is technically a pure function."
social:
  poll:
    question: "What makes I/O visible enough?"
    options:
      - "A TODO: retry later"
      - "The word async in"
      - "Explicit typed boundary"
      - "A dashboard discovered during"
    preferred: "Explicit typed boundary"
fortune:
  quote: "I/O is not clearer because it is injected; it is clearer because someone owns its consequences."
related:
- "where-business-logic-lives"
- "clean-architecture-with-a-fake-moustache"
- "not-another-workflow-engine"
tags:
- "why-tpf-exists"
- "hiding"
- "io"
- "reality"
---

# Does hiding I/O make the important bits harder to see?

## Elevator answer

**TPF does not hide I/O; it names and declares it at connectors and runtime boundaries, separating reality from business decisions.**

<CoffeeMisconceptions />

## The real explanation

The fear behind this question is healthy. Side effects are where systems become real. Money is charged, messages are published, records are stored, suppliers are called, and a human eventually asks why the system did any of it. An architecture that makes those things invisible is not elegant; it is merely harder to debug.

TPF does not try to erase external reality. It tries to stop external reality from masquerading as ordinary business code. A connector models a typed I/O boundary: how work is admitted from outside, how something is published outward, or how captured external facts enter a flow. The runtime and generated adapters own the mechanics of transport, retries, correlation, telemetry, and persistence where those mechanics are part of the declared execution contract. This makes I/O visible at the level where an operator and a reviewer can reason about it.

Compare two forms of visibility. In the first, a domain service injects an HTTP client, a repository, and a message producer. The method body visibly calls all three. That is local visibility, and for small code it can be enough. But the flow-level picture is dispersed: which calls are retried, what happens if the message is published after persistence fails, whether a duplicate is safe, which transport admitted the request, and what telemetry belongs to the outcome may be defined elsewhere or nowhere.

In the second, the business step stays focused on its typed decision and the external admissions and publications are declared as boundaries. The local method contains less machinery, but the application-level I/O map becomes clearer. A reader can find the connector, understand the contract it crosses, inspect the adapter behavior, and see it in the generated metadata. Visibility has moved from “all calls are in one method” to “all operational boundaries are named in one model.” For distributed systems, that is usually the more valuable kind.

This is especially important for failure. Direct I/O in business code invites each call site to invent its own conventions for timeouts, retries, idempotency, exception translation, and observability. Some will be careful; others will be three lines shorter and become a production story. TPF treats the shell as the place to make those policies explicit and reusable. It does not guarantee that every external system behaves nicely. It guarantees that the framework does not pretend an external call is just another deterministic function.

There is a useful nuance: not every read from the outside world should become a mystical event. A pipeline may need external facts, and an application needs persistence. The design question is whether the dependency is a typed, declared connector or an unbounded technical detail hidden inside the domain. Declared does not mean verbose for sport. It means the system can validate and operate the boundary as a boundary.

The trade-off is a change in navigation. Developers accustomed to following a method from top to bottom must learn to follow a flow to its connector and adapter declarations. That can feel less direct at first. It becomes more direct when the question is not “what did this method call?” but “what external reality can change this business outcome, and how does the application respond when it fails?”

TPF’s position is therefore not “hide I/O.” It is “do not let I/O hide in plain sight.” Make it explicit enough to be owned, observed, and tested without contaminating every business decision with a transport vocabulary.

That distinction improves incident conversations, too. Instead of asking which of twenty services happened to call a client, an operator can begin at the declared boundary: what was admitted, what connector was involved, which attempt ran, and whether a retry or replay is legitimate. The answer may still be difficult; at least the system has not made the question accidental.

## Trade-offs

TPF gains visible, typed boundaries and consistent handling of external failure. It gives up the simplicity of a single method that contains every call. The model must remain honest: undeclared client calls in a business step undermine the very visibility the framework is trying to provide.

## When TPF is not a good fit

For a tiny integration adapter or a short-lived script, direct I/O is often exactly what readers should see. Use TPF when the I/O participates in an application flow that needs shared execution semantics, not as a prohibition on using an HTTP client.
