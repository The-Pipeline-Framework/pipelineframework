---
title: Optional host connection lifecycle
status: accepted
---

# ADR-0030: Optional host connection lifecycle

## Context

[ADR-0021](./0021-host-owned-connector-authentication.md) establishes the supported Connector
authentication boundary. Gmail, hosted LLM, MCP and GraphQL now consume that boundary, but host
applications still assemble acquisition, durable authorization state and renewal coordination.
The QuickBooks MCP guide's `QuickBooksMcpClients` intentionally leaves that infrastructure to its host.

## Decision

TPF may ship optional host integration libraries that assemble maintained provider OAuth libraries
with host-controlled storage and lifecycle. This extends ADR-0021's exclusion of framework-supplied
host lifecycle helpers; it does not change its Connector runtime boundary or application authority.

`ConnectionRef` remains a deployment-owned logical connection identity. `ConnectionResolver` remains
the invocation-time host seam. Connectors borrow initialized `ResolvedConnection` capabilities and
never acquire grants, refresh credentials, choose accounts from payloads, or close host clients.
No portable OAuth provider SPI or scope model is added to `runtime-core`.

Connection management is a separate security-state authority. Application hosts own tenant and actor
authorization, OAuth registration, encryption keys, database provisioning and operational policy.
The first optional library, `framework/host-gmail`, coordinates read-only Gmail access using Google's
Java OAuth library and a dedicated JDBC store. Its interfaces are provisional host APIs.

Lifecycle transitions create immutable conditional revisions. Encrypted payloads are authenticated
against connection identity and revision. Superseded payloads are deleted while non-secret revision
metadata is retained. A refresh must win durable authority before calling the provider. Lost remote
outcomes cannot be repaired by claiming exactly-once refresh; unresolved claims fail closed and
eventually require reauthorization. Disconnect and reconnect supersede old writers.

One Google subject per OAuth registration has one managed logical owner in the shared store. An
additional alias or tenant cannot create another refresh authority for the same grant. Ownership
transfer is explicit administrative work. Existing brokers and external process-owned token stores
remain valid alternatives; they must not independently refresh a grant also managed by this library.

## Rationale

Provider differences include account identity, callback validation, scope interpretation, refresh
rotation and recovery, token-cache formats, and revocation breadth. Existing maintained libraries
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
- A dedicated security database may share physical infrastructure with other stores but not their
  schemas, authorization or retention semantics; see [ADR-0008](./0008-separate-state-and-replay-authorities.md).
- Full provider catalogues, Spring execution parity, multi-alias grants and general connection UI are
  not implied by the first proof.
