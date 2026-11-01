---
title: Root recurrence preserves durable Await
status: accepted
---

# ADR-0084: Root recurrence preserves durable Await

## Context

[ADR-0081](./0081-one-authored-consumer-per-concrete-input.md) requires one authored consumer per
concrete canonical input. [ADR-0083](./0083-definition-owned-bounded-recurrence.md) supplies local
unary recurrence, but its initial scope excludes root recurrence and durable Await. PadelPot needs
to ask two policy-controlled human questions in one execution, while preserving typed terminal
results and bounded agent turns.

## Decision

A root version 3 definition can declare concrete continuing outcomes with `recur: [Type]`, using
the same lowering, linked invocation, and recursion policy as a nested local unary definition.
Authored-consumer validation runs first. The generated `$recur` continuation is framework lifecycle
data; explicitly authored calls retain the ordinary consumer constraint.

An operation in that root can suspend through Await. Shared runtime contracts carry an immutable
invocation path, while runtime configuration continues to own the maximum depth. The interaction,
interaction stores, resumed input snapshot, and queue transition codec retain that path. Resume
starts after the completed Await boundary and restores the path under the current configured
bound. It does not re-run earlier operations to reconstruct that path.

Qualified recurrent unit identities include a bounded fingerprint of the invocation path.
Live completion can revisit the same operation without reusing an earlier unit, even while the
execution owner's durable cause is unchanged. Retries at the same path retain their unit identity.
Existing six-component qualified IDs remain byte-for-byte unchanged; extended IDs retain the same
qualified prefix so older parsers fail closed.

Durable release retains the existing atomic continuation and immutable first acknowledgement.
The invocation fingerprint distinguishes recurrent units independently of the execution owner's
cause. Existing causal acknowledgement and reopen rules continue to apply to retained units.
Scalar and itemized continuations use the same qualified identity.

Native Commands that complete a meaningful effect can use the ordinary post-operation transport
Await modifier. The Command produces and records its immediate result before the interaction is
registered; recovery can reuse that result. Callback-capable effects keep the existing
registration-before-dispatch and joined-settlement semantics of ADR-0037.

## Consequences

- Contracts own the root declaration and portable path data. Compiler owners validate authored
  consumers and lower recurrence; runtime owners preserve suspension, resume, depth, and release
  atomicity. No second loop executor or new operation kind is introduced.
- Support remains `LOCAL` and `ONE_TO_ONE`. Await inside nested definitions, Await on a pipeline
  invocation, streaming recurrence, and mutual recursion remain outside this slice.
- Replay retains the operation's Await overlay and exposes the generated root invocation and its
  continuation edge. Canonical union expansion applies to replay routing as well as validation.
- PadelPot publishes approved review/result history through real native Commands, attaches human
  Await to review publication, and handles the response with one policy-owning operation. Its
  trusted review round is business state. Identity review/return services and nominal loop wrappers
  are removed. Its migrated YAML and executable fixture tests provide positive migration evidence.
- Qualified unit readers use the shared occurrence parser, including the optional invocation
  fingerprint; existing non-recurrent identities and causal release rules remain unchanged.
