---
title: "Will AI bypass the boundary to get it working?"
faq:
  id: "ai-and-boundary-safety"
  track: "ai"
  question: "Will AI agents bypass architectural boundaries to get it working?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "ai-ada"
      text: "It compiled, therefore the boundary is valid."
    - persona: "framework-fred"
      text: "A direct client call is only temporary."
    - persona: "enterprise-edna"
      text: "Prompt the model not to leak data."
social:
  poll:
    question: "AI should…"
    options:
      - "Bypass clients"
      - "Invent side effects"
      - "Follow contracts"
      - "Ship Friday"
    preferred: "Follow contracts"
fortune:
  quote: "The useful AI assistant respects the boundary it cannot fully understand."
related:
- "ai-and-machine-readable-contracts"
- "connectors-not-stationery"
tags:
- "ai"
- "boundary"
- "safety"
search: false
---

# Will AI bypass the boundary to get it working?

## Elevator answer

**Yes—an assistant can make the test green by injecting an HTTP client straight into a business step. Typed connector rules and review must make that shortcut loud.**

<CoffeeMisconceptions />

## The real explanation

Ask an assistant to “check the customer's risk score” and the shortest solution may be `riskClient.get()` inside `ApproveOrderStep`. The feature works locally. Retry identity, credentials, capture, telemetry, and ownership have quietly vanished into a method call. AI did not invent that shortcut; it can merely type it before the architect finishes inhaling.

If the assistant places `stripeClient.charge()` inside `CalculateTotal`, the declared flow has no matching Command or connector operation. That gives the compiler, review tooling, and a human something objective to reject. The problem is no longer “I dislike this style”; it is “this effect has no identity, recovery rule, or owner.”

The framework cannot prevent bad changes by itself. Teams need guardrails, reviews, tests, and a culture that treats generated suggestions as proposals. AI is most useful when it helps implement a clear contract, not when it invents a new integration path because that was faster.

## Trade-offs

TPF gains visible safety rails. It gives up some shortcut convenience.

## When TPF is not a good fit

If teams routinely accept unreviewed generated changes in consequential flows, first solve that delivery practice.
