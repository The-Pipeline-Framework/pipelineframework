---
title: LLM clarification is inert protocol data
status: accepted
---

# ADR-0026: LLM clarification is inert protocol data

## Context

A one-turn LLM Query may lack information needed to propose an operation or produce an
application result. The model must be able to return a typed request for human information
without creating a universal Conversation or Agent runtime, and without conflating
clarification with Command authorization.

## Decision

The LLM Query connector contributes `<tpf.llm.AskUser>` to the ordinary version 3 type
universe. `AskUser` contains a display `prompt` and a repeated `choices` field; an empty
choice list requests free text. Applications compose it into their own closed decision
unions beside `<tpf.llm.AgentCall>` and application-authored result types.

`AskUser` is inert, untrusted protocol data returned by one Query inference. It owns no
conversation memory, application state, Agent execution identity, correlation identity,
authorization, effect, or suspension state. Provider and runtime observation metadata
remain outside the typed payload, undeclared fields fail canonical validation, and display
boundaries escape its model-authored text. Sensitive application fields must be excluded
from model input rather than repaired after inference.

Returning `AskUser` ends the current Query invocation. An application routes that value to
a boundary capable of interacting with a person. If durable suspension is needed, the
general deferred-completion boundary owns correlation and resumption through existing
pipeline/interaction identity. The answer re-enters an application-authored typed state
before any later LLM Query turn.

Command confirmation and approval remain native Command authority. An `AskUser` value can
request missing facts, but cannot approve, authorize, dispatch, or retry an effect.

This decision governs the LLM Query contributed protocol vocabulary, its one-turn decision
materialization, and application composition guidance. It does not choose the eventual
surface syntax or storage realization of general deferred completion.

## Rationale

Treating clarification as one typed decision alternative preserves the functional core and
the existing Query boundary. It lets applications own their state machine while keeping
durable suspension and Command effect authority in their established semantic owners.

## Consequences

- Closed version 3 unions may combine `AgentCall`, `AskUser`, and application result types.
- LLM adapters still perform exactly one inference and no hidden repair or waiting loop.
- A response contract is bounded by `choices`; an empty list explicitly permits free text.
- Correlation and durable waiting cannot be implemented with a new Agent ID or Agent ledger.
- A later turn receives the human answer only through ordinary application-authored state.
