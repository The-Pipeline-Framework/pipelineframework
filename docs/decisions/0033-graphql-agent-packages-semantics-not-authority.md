---
title: GraphQL Agent Blocks package semantics, not authority
status: accepted
---

# ADR-0033: GraphQL Agent Blocks package semantics, not authority

## Context

ADR-0029 pins GraphQL Query and Mutation documents in the consuming application's connector
binding. ADR-0032 lets a packaged Block expose a finite, application-bound callable catalogue with
trusted context and arguments. Together they make a reusable GraphQL agent loop possible without a
new Agent, GraphQL, or Block runtime.

The reusable part is larger than transport. It includes the operation guide presented to the model,
Query-versus-Mutation tools, response and error interpretation, trusted effect identity, bounded
history, reduction, recursion, and typed completion. The package must not turn that know-how into
authority to invent GraphQL documents or choose endpoints, credentials, tenants, Command identity,
or Command policy.

## Decision

The `org.pipelineframework.blocks:graphql-agent` artifact exports the qualified
`org.pipelineframework.graphql/graphql-agent` Block. It requires an application-bound LLM Query,
GraphQL Query, and GraphQL Command. Its model tools accept only a persisted operation key and
validated variables. The Mutation tool additionally receives `effectKey` as a trusted,
model-invisible argument.

`GraphQlAgentState` contains the objective, typed operation guide, application effect scope,
bounded observation history, logical turn, and a `maxTurns` value from 1 through 16. Before each
decision, the Block rejects an exhausted turn and derives:

```text
effectKey = SHA-256(application effect scope + NUL + logical turn)
commandId = "graphql:" + persisted operation key + ":" + effectKey
```

The effect scope and derived key are excluded from model input. The model may propose an operation
key and variables, but it cannot provide or replace the effect key. The application explicitly
selects the Command ID generator, duplicate policy, and Command policy in `blockBindings`.

The Block uses ordinary one-turn LLM Query, native dynamic Query or Command dispatch,
`OperationObservation`, authored reduction, branching, and bounded self-recursion. The reducer
reconstructs trusted state from `contextJson`, records canonical final arguments and normalized
GraphQL data/errors, increments the logical turn, and either recurs or returns typed completion.
Reaching `maxTurns` completes before another LLM or GraphQL call.

The typed operation guide is model context, not authority. The application-owned persisted-operation
catalogue from ADR-0029 remains the executable allowlist and rejects missing keys, wrong operation
kinds, or document digest drift before dispatch.

## Rationale

GraphQL-specific state and reduction are useful reusable semantics, while operation eligibility and
external access are application decisions. Packaging the former and statically linking the latter
keeps the functional core reusable without transferring imperative-shell authority.

The design also tests that ADR-0032 is domain-neutral. A future MCP agent must be able to reuse the
same callable, projection, dispatch, and authority model without GraphQL changes to that substrate.

## Consequences

- Applications gain one ordinary `pipeline: graphql-agent` invocation instead of owning routing,
  reduction, recursion, request construction, or GraphQL result normalization.
- Raw GraphQL documents, endpoints, headers, credentials, tenants, accounts, and effect keys are not
  model-authorable inputs.
- Query capture and Command duplicate/ambiguity behavior remain owned by the existing native runtime
  paths; there is no Agent or GraphQL execution subsystem.
- Runtime effect scope, GraphQL documents, credentials, and raw connector configuration are excluded
  from generated metadata. Linked Block and sanitized capability provenance remain release inputs.
- The existing `graphql-query` and `graphql-mutation` Blocks remain independent and do not acquire an
  LLM requirement.
- Pagination follows the framework's paging work. Subscriptions/Await, schema introspection, raw
  GraphQL, runtime catalogue changes, and a production generic-agent Block remain out of scope.

This decision succeeds ADR-0029 for packaged agentic GraphQL composition. ADR-0029's persisted
operation authority and native Query/Command mapping remain in force.
