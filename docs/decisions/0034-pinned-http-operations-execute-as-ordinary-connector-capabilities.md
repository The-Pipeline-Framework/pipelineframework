---
title: Pinned HTTP operations execute as ordinary Connector capabilities
status: accepted
---

# ADR-0034: Pinned HTTP operations execute as ordinary Connector capabilities

## Context

TPF can import external capability catalogues into standard Connector provider manifests, then use
the selected operations through ordinary Query and Command steps. REST APIs need the same release
boundary without generating one Connector for every vendor or parsing an external description at
runtime.

An HTTP description alone is not authority. A method, path, server, security declaration, extension,
or operation identifier cannot decide whether an operation is a Query or Command, select a runtime
server, grant credentials, or choose Command identity and policy. The runtime nevertheless needs a
deterministic wire contract after those application decisions have been made.

## Decision

Provider `http.client` version 1 exposes operations from a standard provider manifest and requires an
exact matching immutable HTTP operation pin. The private pin contains only the selected method,
origin-relative path, parameter serialisation, JSON request and response schemas, explicit status and
outcome mappings, selected security compatibility constraint, representation mapping keys, optional
Command idempotency-header projection, and sanitized fingerprints. It contains no source contract,
base URI, credential, tenant, or runtime header.

The compiler resolves an operation boundary through the existing representation-provider lifecycle.
The new operation request is source-neutral: it identifies the selected Connector operation,
canonical contracts, normalised wire schemas, and ordinary named type mappings. The HTTP provider
accepts an equivalent direct mapping, bounded deterministic mapping options, or an exact curated
`Mapper<Canonical, External>`. Generated sources and the immutable operation-binding resource are
written by the existing compiler-owned artifact host.

At runtime the application host resolves `HttpClientConnection` through `ConnectionResolver`. The
connection supplies a borrowed asynchronous `HttpClient`, application-selected base URI, declared
security capabilities, and a callback for already-resolved authorization material. Credential
acquisition, OAuth consent and refresh, tenant/account choice, and client lifecycle remain outside
the Connector. Automatic redirects must be disabled; the Connector combines only pinned relative
paths with the host authority.

The provider creates ordinary `QueryOperation` and `CommandOperation` instances. Query execution
uses the existing capture/replay path and is initially live-only. Command execution preserves normal
effect identity, policy, duplicate handling, retry and ambiguity semantics. Only explicit response
pins can report success. Failures known to precede dispatch may be retried; connection loss, bounded
body rejection, malformed or unclassified responses after dispatch are ambiguous. Provider
idempotency is advertised only when the pin explicitly maps
`CommandDispatchIdentity.providerIdempotencyKey()` to a header.

## Rationale

The immutable pin is the smallest runtime representation of an already-authorised HTTP capability.
It lets release-time importers disappear after normalisation while keeping the standard Connector
manifest authoritative for TPF identity and contracts. A source format such as OpenAPI can therefore
remain build tooling rather than becoming a runtime or dispatch model.

## Consequences

- Imported HTTP operations use existing Query, Command, dynamic dispatch, capture and effect paths.
- Applications and hosts own classification, connection selection, credentials, OAuth lifecycle,
  tenants, Command identity, duplicate policy and Command policy.
- Runtime request and response values are validated against bounded, fingerprinted wire schemas.
- Request and response mappings converge on the existing canonical type and `Mapper` model.
- The connector does not follow redirects, create or close clients, choose an origin, or infer
  idempotency from an HTTP method or external extension.
- This decision introduces neither an OpenAPI runtime nor OpenAPI-specific execution. Acquisition,
  selection, import provenance and callbacks are separate decisions.
