---
title: "Who owns a connector everybody depends on?"
faq:
  id: "connector-governance"
  track: "connectors"
  question: "How do we govern connector changes that affect many teams?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "platform-priya"
      text: "A major version is a confident commit message."
    - persona: "kafka-frank"
      text: "Consumers will adapt after the schema deploys."
    - persona: "enterprise-edna"
      text: "One shared connector means one eternal version."
social:
  poll:
    question: "Shared connector needs…"
    options:
      - "Silent upgrades"
      - "One eternal version"
      - "A contract owner"
      - "More interfaces"
    preferred: "A contract owner"
fortune:
  quote: "A connector becomes shared infrastructure when its change can alter someone else’s business promise."
related:
- "shared-pipeline-ownership"
- "security-and-compliance"
tags:
- "connectors"
- "connector"
- "governance"
---

# Who owns a connector everybody depends on?

## Elevator answer

**Treat widely used connectors as versioned products: give providers and operations stable contracts, name owners, test compatibility, and migrate consumers deliberately rather than changing shared behavior silently.**

<CoffeeMisconceptions />

## The real explanation

A shared connector is not merely a library. A provider packages reusable integration capabilities; its operations identify what can be invoked, for which kind of boundary, under a major version. Typed input and output contracts, configuration, and declared capabilities make that promise more precise than a shared client API alone.

Applications select those operations through named bindings that supply deployment-specific configuration without redefining the operation contract. That separation matters: the provider and operation contract can evolve as shared infrastructure, while each application still owns where the boundary appears in its flow and which configured binding it uses.

Governance should then be ordinary product discipline: a named owner, a compatibility promise, focused provider and operation tests, and a migration path for incompatible changes. Different teams may need different major versions temporarily. The goal is not a central approval queue; it is to make broad impact visible before it becomes a production dependency failure.

## Trade-offs

TPF gains safer shared integration evolution. It gives up invisible breaking changes.

## When TPF is not a good fit

If no team can maintain a shared connector as a product, keep integrations owned locally until that capability exists.
