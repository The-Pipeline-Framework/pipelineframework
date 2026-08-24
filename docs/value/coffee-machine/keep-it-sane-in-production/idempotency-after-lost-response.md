---
title: "What if the payment worked but the answer vanished?"
faq:
  id: "idempotency-after-lost-response"
  track: "runtime"
  question: "What happens when a remote system succeeds but the response is lost?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "retry-rita"
      text: "No response means it definitely failed."
    - persona: "retry-rita"
      text: "Charge it again; accounting enjoys surprises."
    - persona: "retry-rita"
      text: "TCP was reliable, so the business outcome is known."
social:
  poll:
    question: "Lost response means…"
    options:
      - "Definitely failed"
      - "Definitely worked"
      - "Outcome unknown"
      - "Retry forever"
    preferred: "Outcome unknown"
fortune:
  quote: "A timeout is evidence of uncertainty, not evidence that nothing happened."
related:
- "retry-is-not-for-rejection"
- "dlq-and-replay"
tags:
- "runtime"
- "idempotency"
- "after"
- "lost"
- "response"
---

# What if the payment worked but the answer vanished?

## Elevator answer

**The timeout does not mean the charge failed. Reuse the same effect identity, ask the provider what happened when possible, and do not turn network uncertainty into a second payment.**

<CoffeeMisconceptions />

## The real explanation

The provider receives `charge €120`, commits it, and its response dies between load balancers. Your client sees a timeout; the customer sees money leave. That is not a failed charge. It is an unknown outcome, and blindly constructing a new Command is how one purchase becomes two payments.

TPF cannot infer an external business outcome from a timeout. It can preserve the identifiers that make a safe response possible: idempotency keys, dispatch IDs, correlation IDs, and stable request identity. A connector contract can use those identifiers to make a repeated request safe, query the external system for status, or route the item into a reconciliation path rather than a blind retry.

This is why idempotency belongs at the real external boundary. A pipeline retry may repeat execution, but it must not produce a new business intent on each attempt. Stable identifiers must travel through adapters and survive replay. The external system must cooperate where the effect is irreversible; otherwise the honest answer may be manual review rather than automated certainty.

The trade-off is that teams have to model uncertainty. That adds state and operational paths, but it replaces a silent duplicate with a visible decision.

## Trade-offs

TPF gains stable execution identity. It gives up the fiction that timeouts explain outcomes. Teams must design reconciliation for consequential effects.

## When TPF is not a good fit

If an external system cannot accept idempotency keys or report status for irreversible actions, do not promise automatic retry safety.
