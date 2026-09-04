---
title: MCP imports pinned Connector operations
status: accepted
---

# ADR-0027: MCP imports pinned Connector operations

## Context

MCP servers can describe tools and their JSON Schemas, but discovery says only that a server
advertised a protocol capability at one moment. It does not decide whether an application release
imports that capability, whether an LLM may call it, whether it is a Query or Command, or which
authority and replay semantics apply.

TPF already has the required ownership boundaries: canonical v3 types, Connector provider and
operation metadata, named configured bindings, release-pinned callable catalogues, Query capture,
Command effects, and binding-aware dynamic invocation. A separate MCP-shaped Agent execution path
would duplicate those boundaries and make protocol transport state model-visible.

## Decision

MCP is one external source of Connector operation contracts. An explicit Maven refresh connects to
a configured MCP server, discovers all tool pages, and imports only author-selected mappings into
the standard Connector provider manifest under provider `mcp.client`. Each mapping explicitly owns
the TPF operation ID, Query or Command kind, major version, and canonical input/output type names.
MCP annotations are not authority.

The refresh also writes a private execution pin from the imported TPF identity to the exact MCP
tool name. Normal builds consume these committed resources and do not contact an MCP server.
The importer-v1 JSON Schema subset is deliberately lossless: unsupported schema forms fail refresh
with path diagnostics. Those restrictions are importer limitations; they do not narrow or weaken
canonical v3.

At runtime, the existing named binding selects `mcp.client`. A host `ConnectionResolver` supplies
an initialized `McpClientConnection`. The host owns credentials, sessions, transports, an STDIO
process when used, and client/process shutdown. The connector neither creates nor closes them.
Query operations declare `LIVE_ONLY`, which disables cache reuse but does not bypass Query capture
or captured replay. Command results pass through the ordinary effect state machine, and uncertain
post-dispatch outcomes remain ambiguous.

The three admission stages remain distinct:

```text
discovered MCP tool -> explicitly imported Connector operation -> explicitly exposed callable
```

Only the final release-pinned callable catalogue authorizes model selection. Runtime MCP discovery
is absent from this first slice, so no runtime snapshot identity is required.

## Rationale

Normalizing at import time preserves one Agent protocol and one execution model for native and
MCP-backed capabilities. It keeps releases reproducible and reviewable while allowing the bridge
to contain protocol adaptation. Explicit classification prevents discovery metadata or prompt text
from acquiring authority over Query, Command, policy, or confirmation semantics.

## Consequences

- MCP credentials, endpoints, headers, sessions, transport objects, and client handles never enter
  callable metadata, `AgentCall`, or `OperationObservation`.
- Imported operations use canonical v3 contracts and the ordinary Connector binding/configuration
  boundary.
- A discovered-only or imported-but-unexposed tool cannot execute through dynamic Agent dispatch.
- Runtime-varying discovery or grants require a separately designed immutable snapshot seam before
  committed proposals can safely refer to them.
- Supporting more JSON Schema and MCP extensions expands the importer; it does not create an
  MCP-specific public Connector schema vocabulary.
