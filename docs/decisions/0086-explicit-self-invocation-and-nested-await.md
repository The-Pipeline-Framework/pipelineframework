---
title: Explicit self-invocation and nested Await
status: accepted
---

# ADR-0086. Explicit self-invocation and nested Await

An author expresses recursion at its position in the step sequence. A pipeline reference of `self` resolves to the enclosing definition, including the root. An explicit reference to the enclosing named definition is equivalent. Returning from the invocation executes the caller's remaining steps, as specified by [ADR-0004](./0004-nested-composition-and-bounded-recursion). Allowing the root as a self-call target extends that decision.

The compiler checks ordinary consumers and self-call continuations separately in each definition. Each concrete canonical type may select at most one of each. Calls to other definitions are ordinary consumers. Both kinds of calls must satisfy the invoked definition's input and output contracts. This refines [ADR-0081](./0081-one-authored-consumer-per-concrete-input).

Durable Await can suspend an operation in a LOCAL unary invocation chain. The execution retains the suspended position, each caller's return position and the invocation occurrences. Resume resolves those positions against the pinned compiled pipeline, completes the child and returns through its callers. Recursive depth counts self-calls. The owning root retains terminal publication.

In PadelPot, negotiation policy produces Review when owner authority is needed. The persistence aspect records the prompt, selective Await admits the correlated answer, and the answer policy produces a result or an updated TurnState. An explicit self-call takes that state into the next turn. The root can call negotiation like any other named pipeline.

This decision supersedes the definition declaration from [ADR-0083](./0083-definition-owned-bounded-recurrence) and the root-only suspension model from [ADR-0087](./0087-root-recurrence-preserves-durable-await). Existing declarations migrate to a pipeline step whose selector names the continuing concrete types.
