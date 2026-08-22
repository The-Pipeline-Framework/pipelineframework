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
---

# How do we test ordering without trusting luck?

## Elevator answer

**Declare ordering and cardinality in the flow model, then test deterministic split, merge, mapper, and lineage behavior with inputs designed to expose duplicates and reordering.**

<CoffeeMisconceptions />

## The real explanation

Split and merge are where a flow stops looking like a sequence and starts accumulating mathematical promises. How many outputs may one input create? Which lineage belongs to each one? When results merge, does business ordering matter, and is it deterministic after retry or replay?

TPF makes cardinality, linkage, and ordered step descriptors part of the contract. Tests should use deliberately awkward inputs: several branches, duplicate-like values, slow branches, rejection, and retries. Assert both values and lineage or ordering metadata. A test that only checks the final collection size can miss the bug that will confuse replay or downstream consumers.

The trade-off is more focused test cases. That is cheaper than treating nondeterminism as an operational characteristic discovered in production.

## Trade-offs

TPF gains testable flow semantics. It gives up vague assumptions about concurrent order.

## When TPF is not a good fit

If ordering is not a business requirement, do not impose it merely for prettier tests. State the weaker contract and scale accordingly.
