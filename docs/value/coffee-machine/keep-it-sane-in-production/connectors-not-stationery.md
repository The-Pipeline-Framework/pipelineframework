---
title: "Is a connector just an adapter with better stationery?"
faq:
  id: "connectors-not-stationery"
  track: "connectors"
  question: "Is a connector just an adapter with a more fashionable name?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "hexagonal-helen"
      text: "Two interfaces and a factory solve every external boundary."
    - persona: "kafka-frank"
      text: "A connector is a topic wearing a bow tie."
    - persona: "enterprise-edna"
      text: "Name it GatewayManager and the boundary is governed."
social:
  poll:
    question: "A connector owns…"
    options:
      - "Fancy interfaces"
      - "All client libraries"
      - "An I/O boundary"
      - "More package names"
    preferred: "An I/O boundary"
fortune:
  quote: "A connector is where external reality becomes an application responsibility."
related:
- "connector-plugin-or-step"
- "retry-is-not-for-rejection"
tags:
- "connectors"
- "stationery"
---

# Is a connector just an adapter with better stationery?

## Elevator answer

**A connector is a declared typed I/O boundary: it admits or publishes external reality with ownership, mapping, and operational semantics an ordinary adapter may leave implicit.**

<CoffeeMisconceptions />

## The real explanation

An adapter is a broad and useful idea: code that translates one interface or technology into another. A connector is more specific. TPF uses it for a typed boundary where external reality is admitted into a flow or published from it. That distinction matters because external I/O carries consequences beyond method shape: credentials, mapping, retries, duplicate delivery, traceability, partial data, and ownership when something fails.

A business function should decide from typed facts. A connector can obtain or admit those facts from a REST request, message, database, legacy service, or another external system, then translate them into a declared contract. On the way out, it can publish a result without teaching the business core about a topic, route, client library, or serialization rule.

This is not an argument for wrapper classes around every dependency. A connector earns its name when the interaction is a meaningful boundary for the application: an external admission, a publication, or captured reality that must be operated consistently. A small local helper may remain an adapter. The distinction prevents teams from hiding consequential I/O inside a service simply because injecting a client is quicker.

The trade-off is explicitness. A connector must be designed, tested, and owned. That is more work than a direct call, but it creates a place to state what happens on timeout, duplicate delivery, stale data, authentication failure, or a lost response.

## Trade-offs

TPF gains an explicit operational boundary. It gives up the shortcut of treating all I/O as a private method detail.

## When TPF is not a good fit

For a one-off local tool or a trivial isolated call, a connector can be needless ceremony. Use it where external reality affects a flow that needs shared semantics.
