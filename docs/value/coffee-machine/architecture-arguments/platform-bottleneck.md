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

**It should not: application teams own their typed flows, while shared connector contracts and framework runtime semantics receive coordination proportional to their wider impact.**

<CoffeeMisconceptions />

## The real explanation

Centralization occurs when a framework owns business meaning it does not understand. TPF should not do that. Application teams define their flow composition, domain decisions, and use of supported boundaries. They should be able to change application-owned steps and policies without asking a platform team to interpret the business.

Coordination is appropriate when a change affects a shared contract. A connector provider or operation contract can have many consumers; framework runtime semantics can affect every pipeline that relies on them. Their identities, versions, capabilities, compatibility promises, and generated behavior deserve deliberate evolution. Requiring the same process for a team’s local business step would be a platform bottleneck disguised as consistency.

The boundary is therefore impact, not hierarchy: application-owned flow meaning stays local, shared connector contracts receive product ownership, and framework runtime semantics remain a platform concern. TPF’s typed contract model makes those interfaces visible enough to support the distinction.

## Trade-offs

TPF gains safer shared evolution. It gives up completely invisible cross-team change.

## When TPF is not a good fit

If an organization wants no common runtime or contract surface at all, a shared framework cannot provide value without creating unwanted coordination.
