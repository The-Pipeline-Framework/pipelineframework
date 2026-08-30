---
title: Host-owned connector authentication
status: accepted
---

# ADR-0021: Host-owned connector authentication

## Context

Connectors increasingly need authenticated access to systems such as Gmail, GitHub, Salesforce,
cloud APIs, and databases. TPF knows the tenant, execution, pinned pipeline/release contract, step,
connector binding, operation, and correlation context of an outbound invocation. Host ecosystems
know how application identities, connected accounts, token stores, refresh, and SDK clients are
configured. Neither side can safely infer the other's semantic information.

Passing tokens or connected-account identifiers through ordinary pipeline data would make
credential selection caller-controlled, persistable, and observable. Conversely, making each
connector rediscover execution-to-host-security mapping would duplicate sensitive integration
logic and lose tenant isolation.

## Decision

TPF exposes an immutable `ConnectorExecutionContext` for every live native Query or Command
invocation. It carries optional tenant, execution, pipeline, contract, release, step, correlation,
trace, and deadline information plus the binding and stable provider/operation identity. Managed
queue workers populate the pinned pipeline, contract, release, and trace identity from the
transition envelope; local invocations retain the target identity even when no managed execution
exists.

`ConnectionResolver` is a host application seam. It accepts one typed
`ConnectionResolutionRequest<C>` containing a deployment-owned `ConnectionRef`, the requested
`ResolvedConnection` type, and the invocation context. Quarkus CDI and Spring Boot auto-
configuration each install zero or one application resolver in `ConnectorRuntimeContext` and
reject ambiguous resolver beans. The resolver may use framework security facilities, a connection
broker, or application infrastructure, but TPF does not prescribe or implement those systems.

Connectors request an authenticated, typed runtime handle. They do not receive portable bearer
tokens and do not implement OAuth/OIDC flows, callbacks, refresh-token persistence, or application
user authentication. A connection is resolved for live invocation only. Query capture and Command
effect replay precede provider activation and therefore do not resolve credentials again.

The first proof is provider `google.gmail` with read-only `list.messages`, `get.message`, and
`search.messages` Query operations. The host returns an `AuthenticatedGmailConnection` containing
an already-authenticated Google `Gmail` SDK client. The connector declares Google's read-only
scope as a connector-local constant; TPF adds no portable authentication or scope metadata.

This decision does not schedule outbound-authority controls as a follow-on feature. If a concrete
connector later demonstrates a need for destination, account, or egress restrictions, that
security policy remains host/runtime deployment configuration. `pipeline.yaml` continues to
select a configured connector binding and typed operation; it does not acquire security-policy
fields.

## Rationale

The request joins the two facts required for safe resolution: the deployment-selected logical
connection and TPF's current invocation semantics. The typed result lets each connector retain its
provider SDK and error translation without exposing credential material through TPF APIs. Keeping
the resolver host-supplied allows Quarkus, Spring, and managed platforms to use their existing
identity, token, and connection infrastructure.

This is the smallest abstraction demonstrated by both host adapters and a real provider SDK. It
does not attempt to describe every authentication mechanism or solve the separate outbound-
authority problem of caller-controlled destinations.

## Alternatives considered

- A context-free compatibility overload or a separate authenticated-resolution sidecar would
  preserve two resolution models. Neither can make tenant-aware invocation semantics mandatory,
  so the owning `ConnectionResolver` contract changes directly.
- A portable bearer-token wrapper would expose credential material and make connectors responsible
  for token attachment, refresh races, and redaction. Typed authenticated handles keep those
  concerns in the host integration.
- A host callback that performs external requests would either split provider semantics between
  the connector and host or require TPF to define a generic authenticated HTTP executor. That
  belongs to a future outbound-authority decision if centralized egress becomes a requirement.
- Portable scope strings and generic authentication metadata are deferred until multiple
  authenticated connector types demonstrate common semantics that both host integrations can use.
  Provider-specific requirements remain connector documentation in the meantime.

## Consequences

- `ConnectionResolver` is a clean SPI break; there is no context-free compatibility overload.
- Resolver implementations must select connected access from deployment configuration and
  invocation context, especially `tenantId`, never from ordinary pipeline payload fields.
- Resolver ambiguity and missing tenant context fail without exposing credentials in payloads,
  durable records, telemetry, or error codes.
- The Gmail connector depends on the Google SDK but contains no authorization-code flow, callback,
  client secret, token store, or refresh-token code.
- Application developers remain responsible for OAuth client registration, consent, scopes,
  connected-account persistence, and the Quarkus/Spring security configuration they choose.
- Portable scope strings, generic authentication metadata, live secret-backed tests, and full
  Spring connector execution remain roadmap items, not implied current support.
- Caller-controlled destination widening is not solved by this resolver seam and has no planned
  follow-on work. Reconsider it only when a concrete connector, such as generic HTTP with a
  caller-selected destination, demonstrates the risk.
