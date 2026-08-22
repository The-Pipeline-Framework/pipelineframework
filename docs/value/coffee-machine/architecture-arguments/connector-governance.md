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

**Treat widely used connectors as versioned products: publish compatibility contracts, test consumers, name owners, and evolve mappings deliberately rather than changing a shared client silently.**

<CoffeeMisconceptions />

## The real explanation

A shared connector is not merely a library. It encodes an external boundary, mappings, credentials, delivery behavior, and operational assumptions that several teams may depend on. Changing it silently can alter more business flows than its implementation suggests.

TPF makes connector declarations explicit, which makes the consumer relationship more discoverable. Governance should then be ordinary product discipline: a named owner, a compatibility promise, versioned contract where needed, focused connector tests, and a migration path for incompatible changes. Different teams may need different versions temporarily; pretending one upgrade suits every consumer creates an accidental shared-monolith boundary.

The goal is not to create a central approval queue. It is to make broad impact visible before it becomes a production dependency failure.

## Trade-offs

TPF gains safer shared integration evolution. It gives up invisible breaking changes.

## When TPF is not a good fit

If no team can maintain a shared connector as a product, keep integrations owned locally until that capability exists.
