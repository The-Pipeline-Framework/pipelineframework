---
title: Expansion and Reduction
search: false
---

# Expansion and Reduction

If you come from an imperative background, think of a pipeline as a loop over items. Each step sees items in order and can emit zero, one, or many items for each input.

The four step shapes describe that behavior in plain terms:

## One-to-One (1 → 1)

For every input item, produce exactly one output item.

Think: `for each item, return one item`.

Typical use: validation, enrichment, format conversion.

## One-to-Many (1 → N)

For every input item, emit multiple outputs.

Think: `for each item, split into many`.

Typical use: splitting a batch file into records, expanding a summary into details.

## Many-to-One (N → 1)

Consume a stream of items and return a single result.

Think: `read many, aggregate into one`.

Typical use: rolling up payments into a single report, generating a file.

## Many-to-Many (N → N)

Consume a stream and emit a transformed stream.

Think: `filter + transform + emit`.

Typical use: filtering, joining, or fan-out with transformation.

## Why This Matters

- Expansion steps increase the number of items in flight.
- Reduction steps collapse the stream into a final result.
- The shape determines how you place side effects and where you expect backpressure.

If you can reason about a loop and a list, you already understand these shapes.

