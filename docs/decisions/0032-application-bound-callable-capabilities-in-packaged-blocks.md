---
title: Packaged callable catalogues preserve application authority
status: accepted
---

# ADR-0032: Packaged callable catalogues preserve application authority

## Context

ADR-0028 permits imported Blocks to contain statically linked operation-first Query and Command
steps whose bindings and Command authority come from the consuming application. It deliberately
left callable catalogues and dynamic operation selection outside the Block boundary.

Reusable agentic and other decision-driven composition needs a Block to describe a finite catalogue,
carry trusted orchestration state across one external call, and dispatch the selected operation. The
Block must still not acquire endpoints, credentials, tenant identity, connector discovery, or
Command authority. Treating this as an LLM or Agent runtime would duplicate ordinary v3 composition,
branching, recursion, Query capture, and Command effect semantics.

## Decision

An imported Block may place an explicit `callables` catalogue on an operation-first Query step. The
decision Query and every callable use requirements declared by the same qualified Block definition.
The consuming application's existing `blockBindings` resolves those requirements to connector
bindings and supplies all Command ID, duplicate, and policy choices.

The decision input has three independent compile-time-validated projections. `modelInputExcludes`
is a list of typed record paths removed only from model-visible input. `callContext` maps typed source
paths into inert `AgentCall.contextJson`. A callable's `trustedArguments` maps typed source paths into
top-level fields of that callable's canonical input. Trusted targets are absent from the model tool
schema; model output containing one is rejected, and trusted values are merged before full canonical
input validation. Paths traverse record fields only.

An imported dynamic operation step may select only from an explicit catalogue produced by a step in
the same definition. Compilation resolves every alias to its binding, provider operation and
versions, canonical input/output contracts, Query capabilities or Command authority, fixed operation
configuration, and trusted mappings. The resulting compiler IR is generic dynamic-operation
selection; it has no dependency on LLM configuration or model behavior.

Generated adapters embed `OperationDispatchDescriptor` directly and invoke
`OperationDispatchSupport`, which delegates to the existing `QueryStepSupport` or
`CommandStepSupport`. Final canonical arguments and trusted context are returned in
`OperationObservation`; context is never supplied to a connector. Qualified Block and authored step
identity determine generated classes, capture identities, and Command step identities. Runtime does
not load YAML, rediscover callables, or reinterpret authority.

Schema 3 release metadata records sanitized callable provenance and the linked definition
fingerprint. Catalogue aliases, requirements, bindings, provider operations and versions, canonical
contracts, trusted mappings, Command authority, and connector-configuration digests participate in
the contract hash. Runtime context values, credentials, prompts containing application data, and raw
binding configuration do not.

## Rationale

The reusable capability is a static linking substrate, not a generic Agent abstraction. Existing
Block composition, typed branching, and bounded self-recursion already express a callable loop. A
domain Block may own its state, reducer, observation interpretation, and completion semantics while
the application grants only the concrete capabilities that release is allowed to use.

Keeping the IR producer-neutral allows future decision sources and a future MCP-domain Block to use
the same capability without GraphQL or LLM assumptions. Exact callable target contracts and native
Query/Command dispatch preserve the framework's established authority and replay boundaries.

## Consequences

- Blocks may package finite application-bound callable catalogues, trusted context and arguments,
  and ordinary dynamic Query/Command dispatch.
- `modelInputExcludes` is a list of typed paths; the former map shape is not supported.
- A Block cannot provide connector bindings or Command authority and cannot bypass the compiled
  catalogue with runtime provider, operation, endpoint, credential, or tenant data.
- `AgentCall.contextJson` and observation argument/context fields are inert canonical data, not
  execution authority.
- There is no new loop, Agent, Block, or dynamic-operation runtime, and no production generic-agent
  Block or generic reducer/state API.
- Await, imported runtime operation discovery, cross-definition callable sources, pagination, and
  per-invocation Block binding remain outside this decision.

This decision supersedes ADR-0028's prohibition on imported callable catalogues and dynamic
operation steps. ADR-0028's application-bound capability and no-Block-runtime decisions remain in
force.
