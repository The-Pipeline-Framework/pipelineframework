---
title: Nested composition and bounded recursion
status: accepted
---

# ADR-0004: Nested composition and bounded recursion

## Context

Applications need reusable subflows and repeated decision-making without introducing an
imperative workflow registry, graph jumps, or hidden provider-owned agent loops.

## Decision

A named nested pipeline is a typed pipeline step. Local nested invocation remains inside
the root execution and release contract rather than creating a second admission,
workflow row, or terminal publication boundary.

Direct self-recursion is TPF's structured looping mechanism. A typed union and `accepts`
select either the recursive case or the base case. Each invocation progresses forward
and later returns through its caller's remaining steps. The runtime bounds recursive
depth and fails explicitly before the bound is exceeded.

Agentic iteration uses the same model: carry trusted typed state; use Query for one new
model or read-only tool observation, Command for a tool effect; return a typed
continue/complete decision; recurse explicitly for the next turn. One Query/provider
call does not hide a recursive tool, memory, or repair loop.

This decision governs `framework/deployment/src/main/java/org/pipelineframework/processor/composition`,
`framework/runtime/src/main/java/org/pipelineframework/invocation`, and the v3 DSL.

## Rationale

Typed recursion makes iteration compiler-visible, release-pinned, bounded, and
observable while preserving ordinary forward dataflow inside every invocation.

## Consequences

- TPF has looping without a general `goto` or arbitrary workflow graph.
- Base cases are explicit typed branches.
- Exact cardinality, depth defaults, mutual-recursion, nested Await, and remote support
  remain release constraints documented in the current DSL and focused tests.
