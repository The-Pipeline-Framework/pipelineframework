---
title: "How do we know the new flow did not change the old promise?"
faq:
  id: "prove-the-promise"
  track: "bring-your-existing-app"
  question: "How do we prove that a migrated flow behaves exactly like the legacy one?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "test-terry"
      text: "If the new unit test passes, historical behavior has politely updated itself."
    - persona: "test-terry"
      text: "Production traffic is the most realistic acceptance environment."
    - persona: "ai-ada"
      text: "Ask a model whether the two implementations feel semantically similar."
social:
  poll:
    question: "What proves a migration?"
    options:
      - "A prettier architecture"
      - "A successful compilation alone"
      - "Typed flow contract"
      - "The new framework’s logo"
    preferred: "Typed flow contract"
fortune:
  quote: "A migration is complete when the promise is proven, not when the old package is deleted."
related:
- "migrate-one-capability"
- "untangle-without-duplicating"
tags:
- "bring-your-existing-app"
- "prove"
- "promise"
---

# How do we know the new flow did not change the old promise?

## Elevator answer

**Record what the old path actually returns, rejects, writes, and publishes. Run the new path against the same ugly cases, and compare the promise—not the class diagram.**

<CoffeeMisconceptions />

## The real explanation

“Exactly like” is already suspicious. The old path probably contains a promise, a quirk somebody depends on, and a bug everybody has learned to walk around. Decide which HTTP responses, DB rows, Kafka records, emails, timings, and failures callers are actually entitled to before declaring them equivalent.

Start with characterization tests. Feed both paths ordinary orders and the ugly ones: duplicate messages, a missing customer, a payment timeout after the provider charged the card, a lazy-load failure, and the exception that becomes 409 only on Tuesdays. Capture results, rejections, writes, observations, and published records. Happy-path equivalence produces excellent demos and terrible confidence.

TPF compilation contributes structural evidence: incompatible steps, ambiguous mappings, missing connectors, invalid cardinality, and unavailable generated artifacts can fail early. It cannot prove that the discount is still 7.5%. Better types do not cause historical behavior to update itself politely.

Use replay tools according to what they replay. A generic cache can reuse stable pipeline computation. Query capture can reuse the exchange rate or account row a decision observed. A Command that charged a card is an effect, not test data; recovery belongs to effect identity and the `CommandEffectStore`, not to blindly running it again. Shadow the decision with a controlled adapter, not the customer's credit limit.

When safe, compare the new decision in shadow or route a small bounded slice of traffic. Stable logical IDs make retries and correlations legible across versions. Once evidence is good enough, retire the old path. Leaving two implementations forever does not preserve safety; it creates a disagreement service.

## Trade-offs

TPF adds compile-time checks, selective replay, and observable comparison. The cost is characterization work and effect-safe evidence before the old path can go.

## When TPF is not a good fit

If the business cannot define what behavior matters or cannot observe it safely, first improve tests and telemetry. A pipeline is not a substitute for an acceptance contract.
