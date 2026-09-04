---
title: GraphQL operations are application-pinned
status: accepted
---

# ADR-0029: GraphQL operations are application-pinned

## Context

ADR-0028 lets an imported Block require ordinary Query or Command capabilities while the consuming
application supplies their connector bindings and Command authority. GraphQL is the first external
protocol to exercise both kinds from a Block. A reusable GraphQL Block can own request validation,
normalization, and topology, but it must not let package code or request data select an arbitrary
document, endpoint, credential, tenant, or mutation authority.

GraphQL application errors also do not prove that a mutation had no effect. A server may return both
data and errors after applying part or all of a mutation. Retrying such a response as though dispatch
had definitely failed could duplicate an external effect.

## Decision

TPF defines provider-neutral `execute.query` and `execute.mutation` Connector operations with
canonical types under `tpf.graphql`. Requests contain only an application-defined operation key and
nominal validated JSON-object variables. Mutation requests additionally contain an application-owned
semantic effect key. They cannot carry a GraphQL document, endpoint, headers, credentials, account,
or tenant selector.

The consuming application's connector binding owns a static persisted-operation catalogue. Every
entry pins an operation key to one classpath resource, SHA-256 digest, operation name, and Query or
Mutation kind. Provider activation loads and parses that resource without contacting an endpoint,
then rejects missing resources, digest drift, multiple operations, name mismatch, kind mismatch, and
subscriptions. Runtime dispatch accepts only keys from the activated catalogue.

Provider `graphql.smallrye` implements the operations with SmallRye's asynchronous dynamic client.
The host resolves an `AuthenticatedGraphQlConnection` for each live invocation through the existing
`ConnectionResolver`; the connector neither creates, configures, authenticates, nor closes that
client. Query maps to TPF Query and Mutation maps to TPF Command. Captured Query replay and recorded
Command duplicate replay occur before live connection resolution.

A syntactically valid GraphQL response, including one containing GraphQL errors, is a successful
Command outcome with `PROVIDER_ACKNOWLEDGED` confirmation because an effect may have occurred.
Unknown or wrong-kind keys fail before dispatch. A connection-resolution failure is retryable because
dispatch has not started. A transport failure or invalid response after dispatch is ambiguous. The
provider advertises no retry-redrive, provider-idempotency, or reconciliation capability.

The `org.pipelineframework.blocks:graphql` artifact exports `graphql-query` and `graphql-mutation`.
They require `graphql.read` Query and `graphql.write` Command capabilities respectively and normalize
through the ordinary v3 pipeline model. Applications bind those requirements as described by
ADR-0028; the Block contains no binding or Command policy.

## Rationale

An operation key is safe only because the application release fixes what it means. Resource and
digest verification makes that authority reproducible and inspectable, while keeping GraphQL syntax
out of the TPF DSL. Host-owned connection resolution preserves tenant-aware credential selection.
Mapping Query and Mutation to the existing TPF semantic owners retains capture, effect, duplicate,
retry, and ambiguity behavior without a GraphQL or Block runtime.

## Consequences

- Applications explicitly choose every GraphQL document and mutation authority included in a
  release; model-generated or caller-supplied raw GraphQL cannot widen that set.
- Connector metadata records only the sanitized binding-configuration digest. Documents, endpoint
  configuration, credentials, and connection values are not copied into generated metadata.
- The portable contract is runtime-neutral, while the first provider implementation and proof are
  Quarkus/SmallRye only. No Spring parity is claimed.
- Pagination, subscriptions/Await, schema introspection or generation, runtime catalogue mutation,
  raw-document execution, provider-specific business semantics, and packaged agentic GraphQL remain
  outside this decision.
- A consumer-local agent pipeline may compose the same Blocks with existing one-turn Query,
  operation observation, authored reduction, and bounded recursion without gaining new runtime
  semantics.
