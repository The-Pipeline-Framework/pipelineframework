---
title: Host-platform connection lifecycle
status: accepted
---

# ADR-0030: Host-platform connection lifecycle

## Context

[ADR-0021](./0021-host-owned-connector-authentication.md) establishes the supported Connector
authentication boundary. Gmail, hosted LLM, MCP and GraphQL now consume that boundary, but host
applications still assemble acquisition, durable authorization state and renewal coordination.
The QuickBooks MCP guide's `QuickBooksMcpClients` intentionally leaves that infrastructure to its host.

The first Gmail proof assembled Google's provider OAuth library directly. That duplicated
authorization-code work already supplied by the host platform. Host ownership should reuse the
platform's security facilities rather than create another authorization implementation inside TPF.

## Decision

TPF may ship optional host integration libraries that combine platform-managed authorization with
host-controlled storage and lifecycle. This extends ADR-0021's exclusion of framework-supplied host
lifecycle helpers; it does not change its Connector runtime boundary or application authority.

Quarkus owns authorization-code redirects, callback/state/PKCE validation, identity verification and
token-endpoint requests. Optional TPF integration uses public Quarkus APIs. Spring will use its own
security facilities when supported; no portable OAuth SPI is introduced now.

`ConnectionRef` remains a deployment-owned logical connection identity. `ConnectionResolver` remains
the invocation-time host seam. Connectors borrow initialized `ResolvedConnection` capabilities and
never acquire grants, refresh credentials, choose accounts from payloads, or close host clients.
No portable OAuth provider SPI or scope model is added to `runtime-core`.

Connection management is a separate security-state authority. Application hosts own tenant and actor
authorization, OAuth registration, encryption keys, database provisioning and operational policy.
`host-oidc-quarkus` bridges durable connection lifetime, while `host-gmail` contains only the
authenticated Gmail SDK factory and `host-microsoft-graph` contains a bounded Microsoft Graph
client factory. These remain provisional host APIs.

The connection-authentication tenant is distinct from application sign-in. Quarkus's completion
action hands verified identity and tokens to the bridge. A one-use host attempt binds the original
application actor, browser and `ConnectionRef` to that Quarkus flow. The connection session retains
only an ID token and does not refresh; after completion it is locally logged out. Durable connection
management is the sole refresh authority and calls Quarkus `OidcClient` for renewal.

Quarkus's database `TokenStateManager` manages session lifetime and is not the durable connection
registry. Connection security state remains separate from pipeline persistence and replay authorities.

Lifecycle transitions create immutable conditional revisions. Encrypted payloads are authenticated
against connection identity and revision. Superseded payloads are deleted while non-secret revision
metadata is retained. A refresh must win durable authority before calling the provider. Lost remote
outcomes cannot be repaired by claiming exactly-once refresh; unresolved claims fail closed and
eventually require reauthorization. Disconnect and reconnect supersede old writers.

One provider account per OAuth registration has one managed logical owner in the shared store. An
additional alias or tenant cannot create another refresh authority for the same grant. Ownership
transfer is explicit administrative work. Existing brokers and external process-owned token stores
remain valid alternatives; they must not independently refresh a grant also managed by this library.

The bridge requires a single resolvable `AuthenticationCompletionAction` bean. One bridge hook
dispatches all registered flows, and ambiguous bridge wiring fails startup.

The original Quarkus 3.33.1 proof found that setting token-client `connection-retry-count=0` threw
`maxAttempts must be greater than zero` before exchange. The default retries socket failures, which
can redispatch a refresh after an unknown remote outcome. A separately installed public
`OidcRequestFilter` therefore guards managed token requests against retry subscriptions before
Quarkus sends them. It is initialized without the connection manager or `OidcClients` to avoid a
client/filter creation cycle. The lost-response proof checks exactly one provider request and
uncertain durable state. This guard and proof remain active on the Quarkus 3.39.2 baseline; the guard
replaces no request encoding, token parsing, provider endpoint or protocol flow.

## Rationale

Provider differences include account identity, callback validation, scope interpretation, refresh
rotation and recovery, token-cache formats, and revocation breadth. Host-platform security layers
should own OAuth protocol behavior. Common connection lifecycle can reduce host plumbing without
pretending these differences are a declarative list of endpoint URLs.

Gmail is the first proof because its existing typed Connector and captured-replay tests isolate
connection management from MCP process supervision. Hosted MCP and STDIO require different host
adapters but no changes to MCP operation contracts.

The local-development QuickBooks STDIO adapter, `framework/host-quickbooks-mcp`, preserves
Node-owned authorization end-to-end. Java manages only host-attested tenant-to-instance bindings,
initialized-client reuse and process lifetime. It neither inspects credentials nor validates realms
through Node's private storage. It adds no credential schema, token-file locks, grant manager or
authorization-status protocol. Node's own workflow owns acquisition, account selection, persistence,
refresh and reauthorization. The production hosted Streamable HTTP integration is separate and
is not designed or implemented by this local-development adapter.

## Consequences

- Optional host endpoints are mounted explicitly with application authentication and permission checks.
- Secrets and account selectors remain absent from pipeline inputs, `AgentCall`, Query captures,
  Command effects, generated operation metadata and telemetry.
- Local disconnect and remote grant revocation are different operations. The Gmail helper performs
  local disconnect; wider Google grant revocation requires an explicit host action.
- Query replay and recorded Command replay still bypass live connection resolution.
- Gmail keeps only authenticated SDK construction. Existing users reconnect; there is no parallel
  legacy authority or migration of the provisional Gmail grants.
- Microsoft delegated access is supplied through Quarkus provider configuration and the bounded
  `host-microsoft-graph` `GET /v1.0/me` client factory. No MSAL path, token cache or general Microsoft
  Connector catalogue is added.
- Claims challenges require interaction; full protected-claims forwarding is not implied.
- A dedicated security database may share physical infrastructure with other stores but not their
  schemas, authorization or retention semantics; see [ADR-0008](./0008-separate-state-and-replay-authorities.md).
- Full provider catalogues, Spring execution parity, multi-alias grants and general connection UI are
  not implied by the first proof.
- The integration is verified on the framework's Quarkus 3.39.2 baseline.

The discarded provider-library-first Gmail proof and the earlier MSAL-first Microsoft reconnaissance
are retained here as rejected implementation directions. The host runtime boundary from ADR-0021 and
the Node-owned local QuickBooks authorization boundary remain unchanged.
