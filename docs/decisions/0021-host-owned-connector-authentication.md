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

The older `SecretRef`, `SecretResolver`, and `ResolvedSecret` connector APIs are deprecated for
removal and retained only for source compatibility. Their lookup is context-free: it has no tenant,
execution, connector target, or operation identity. They must not be used to authenticate external
connector access. This warning is part of the public Javadoc and connector authoring documentation
so generated code and coding agents do not mistake the legacy secret API for the authentication
seam defined by this decision.

Connectors request an authenticated, typed runtime handle. They do not receive portable bearer
tokens and do not implement OAuth/OIDC flows, callbacks, refresh-token persistence, or application
user authentication. A connection is resolved for live invocation only. Query capture and Command
effect replay precede provider activation and therefore do not resolve credentials again.

The first proof is provider `google.gmail` with read-only `list.messages`, `get.message`, and
`search.messages` Query operations. The host returns an `AuthenticatedGmailConnection` containing
an already-authenticated Google `Gmail` SDK client. The connector declares Google's read-only
scope as a connector-local constant; TPF adds no portable authentication or scope metadata.

Provider `llm.query.openai.compatible` is the second proof. Its provider configuration contains a
model, optional base URL, and logical `ConnectionRef`; it contains no API key or secret-store
selector. The host returns an `AuthenticatedOpenAiCompatibleConnection` whose factory creates an
authenticated LangChain4j `ChatModel`. Resolution occurs for every live invocation so tenant
selection and credential rotation remain host-owned, while captured replay performs no resolution.

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

- Treating the deprecated context-free secret SPI as an authentication compatibility overload
  would preserve two resolution models. It remains functional only to avoid an unannounced source
  break; it is explicitly excluded from authenticated connector design. New work changes the
  owning `ConnectionResolver` contract directly rather than extending the legacy path.
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

- `ConnectionResolver` is the only supported connector-authentication SPI. The context-free secret
  SPI remains deprecated compatibility surface and is scheduled for removal.
- Resolver implementations must select connected access from deployment configuration and
  invocation context, especially `tenantId`, never from ordinary pipeline payload fields.
- Resolver ambiguity and missing tenant context fail without exposing credentials in payloads,
  durable records, telemetry, or error codes.
- Gmail and OpenAI-compatible LLM connectors contain no authorization-code flow, callback, client
  secret, token store, or refresh-token code. Neither receives portable credential material.
- Application developers remain responsible for OAuth client registration, consent, scopes,
  connected-account persistence, and the Quarkus/Spring security configuration they choose.
- Portable scope strings, generic authentication metadata, live secret-backed tests, and full
  Spring connector execution remain roadmap items, not implied current support.
- Caller-controlled destination widening is not solved by this resolver seam and has no planned
  follow-on work. Reconsider it only when a concrete connector, such as generic HTTP with a
  caller-selected destination, demonstrates the risk.
