---
title: Select canonical outcomes for Await and observation
status: accepted
---

# ADR-0085: Select canonical outcomes for Await and observation

## Context

A policy operation can produce a continuing state, a review request, or a terminal result.
Only the review request needs an external answer. Persistence of approved review and result
records belongs to the persistence plugin. The initial PadelPot migration introduced publication
Commands solely to provide an Await attachment point, moving that plugin responsibility into
application code.

[ADR-0081](./0081-one-authored-consumer-per-concrete-input.md) requires one authored consumer per
concrete canonical input. Expressing branch lifecycle behavior must preserve that routing rule
and the ownership of persistence and Await in the framework shell.

## Decision

An operation's Await modifier may select concrete canonical outcomes from its immediate output
with `await.accepts`. `await.output` names the selected branch's completed canonical outcome.
The step's final output contract contains the unchanged continuing outcomes together with that
completed outcome. Union names describe these actual leaf sets; the migration does not introduce
new concrete canonical types to distinguish otherwise identical consumers.

The selected payload is the trusted Await request and projector context. Unselected outcomes
continue without registering a wait. Durable completion stores and resumes the canonical completed
payload through ordinary typed routing. The operation input selector, `steps[].accepts`, remains
separate and continues to determine the authored consumer checked by ADR-0081.

An aspect may select canonical payloads with `config.targetTypes`. Its generated observer keeps
the pipeline carrier intact, extracts the selected payload for the plugin, and preserves the
carrier on return. Each occurrence retains its authored owner and definition scope in generated
order and metadata. Sharing a typed plugin implementation does not remove an observer occurrence.

For ordinary post-operation Await, output observations run after the operation and before the
Await modifier. A persistence aspect can therefore observe a Review request before suspension
and a Result outcome before checked definition completion. Definition completion remains the
boundary described by [ADR-0082](./0082-definition-output-completes-typed-branches.md).

## Consequences

- Compiler contracts own outcome selection, Java union projection, generated ordering, and
  matching routing and replay metadata. Runtime Await retains correlation, admission, durable
  snapshots, and resume authority.
- An observer's runtime input is the actual carrier accepted by its generated adapter. Replay topology
  records its authored owner; compiler IR retains the observed payload separately.
- PadelPot's policy operations initiate review Await; its persistence aspects store approved
  conversation records. Application publication helpers and identity services are unnecessary.
- Current supported shapes and selector syntax belong in the pipeline reference and validation
  tests. Callback initiation retains registration-before-dispatch semantics.
