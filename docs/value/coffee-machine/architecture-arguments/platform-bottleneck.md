---
title: "Does the platform become a bottleneck?"
faq:
  id: "platform-bottleneck"
  track: "governance"
  question: "Does TPF create a central platform bottleneck for every application change?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "platform-priya"
      text: "All change requests are platform requests."
    - persona: "consultant-nigel"
      text: "No standards means no waiting."
    - persona: "consultant-nigel"
      text: "Every exception needs a board meeting."
social:
  poll:
    question: "Platform review for…"
    options:
      - "Every method"
      - "Shared contracts"
      - "Broad impact"
      - "Nothing"
    preferred: "Broad impact"
fortune:
  quote: "Autonomy works when teams can move locally and coordinate only where the contract is genuinely shared."
related:
- "shared-pipeline-ownership"
- "autonomy-without-anarchy"
tags:
- "governance"
- "platform"
- "bottleneck"
---

# Does the platform become a bottleneck?

## Elevator answer

**It should not: teams change their own typed flows independently, while the platform owns supported runtime and connector contracts whose broader impact justifies coordinated evolution.**

<CoffeeMisconceptions />

## The real explanation

Centralization occurs when a framework owns business meaning it does not understand. TPF should not do that. Application teams define their flows, domain decisions, and use of supported boundaries. A platform team supplies runtime conventions, security controls, generated integration patterns, and shared connectors where those services reduce repeated operational work.

Coordination is appropriate when a change affects a shared contract. A connector version, generated runtime behavior, or security boundary can have many consumers and should evolve with compatibility review. Requiring the same process for a team’s local business step would be a platform bottleneck disguised as consistency.

The boundary is therefore impact, not hierarchy. Teams should be autonomous inside their owned flows and deliberate at shared interfaces. TPF’s typed contract model makes the interface visible enough to support that distinction.

## Trade-offs

TPF gains safer shared evolution. It gives up completely invisible cross-team change.

## When TPF is not a good fit

If an organization wants no common runtime or contract surface at all, a shared framework cannot provide value without creating unwanted coordination.
