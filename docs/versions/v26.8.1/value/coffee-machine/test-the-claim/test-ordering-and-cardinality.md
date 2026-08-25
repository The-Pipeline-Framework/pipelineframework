---
title: "How do we test ordering without trusting luck?"
faq:
  id: "test-ordering-and-cardinality"
  track: "testing"
  question: "How do teams prove that ordering and cardinality are correct?"
  added: "2026-08-22"
coffeeMachine:
  personas:
    - persona: "async-andy"
      text: "Parallel output order is a personality trait."
    - persona: "test-terry"
      text: "One happy-path item proves fan-out."
    - persona: "kafka-frank"
      text: "Partition order solves every merge."
social:
  poll:
    question: "Ordering needs…"
    options:
      - "Luck"
      - "More threads"
      - "A contract"
      - "A slower test"
    preferred: "A contract"
fortune:
  quote: "If ordering matters, it must be a promise before it can be a test."
related:
- "deterministic-time"
- "generated-artifacts-are-contracts"
tags:
- "testing"
- "test"
- "ordering"
- "cardinality"
search: false
---

# How do we test ordering without trusting luck?

## Elevator answer

**Split one order into awkwardly numbered items, duplicate one, delay another, then prove the merge returns the promised count and order after retry. “Usually sorted” is not cardinality.**

<CoffeeMisconceptions />

## The real explanation

One order becomes twelve line-item checks; item 7 retries, item 4 completes twice, and item 2 returns last. Does the merge emit twelve results, eleven, thirteen, or “whatever arrived”? Split and merge turn a tidy arrow into promises about count, lineage, duplicates, and order. Test the promises with hostile inputs.

TPF makes cardinality, linkage, and ordered step descriptors part of the contract. Tests should use deliberately awkward inputs: several branches, duplicate-like values, slow branches, rejection, and retries. Assert both values and lineage or ordering metadata. A test that only checks the final collection size can miss the bug that will confuse replay or downstream consumers.

The trade-off is more focused test cases. That is cheaper than treating nondeterminism as an operational characteristic discovered in production.

## Trade-offs

TPF gains testable flow semantics. It gives up vague assumptions about concurrent order.

## When TPF is not a good fit

If ordering is not a business requirement, do not impose it merely for prettier tests. State the weaker contract and scale accordingly.
