---
title: OpenAPI callbacks are pinned Connector completion contracts
status: accepted
---

# ADR-0038: OpenAPI callbacks are pinned Connector completion contracts

## Context

[ADR-0035](./0035-openapi-imports-release-pinned-capabilities-not-authority.md) deferred callback import.
[ADR-0037](./0037-command-deferred-completion-joins-effect-and-callback.md) defines how an ordinary
Command joins effect dispatch with correlated completion. An initiating operation's OpenAPI callback
can describe that boundary while leaving deployment and authentication authority with the application.

## Decision

Explicitly selected initiating-operation callbacks become immutable completion contracts of the
ordinary `http.client` Command. Pins record injection targets, inbound schemas and mappings,
security compatibility, and acknowledgement. Provider manifests advertise the same input-only
contract. The compiler selects it through `await.callback` and the existing representation system.

The application resolves a trusted public base URI; the framework appends its fixed route and signed
resume token. The HTTP Connector injects the URI after mapping, rejects authored values at the
reserved target, and validates the completed wire value before dispatch.

A generic runtime ingress authenticates bounded raw material using application policy, validates
and maps the callback payload, and invokes ordinary Await admission. Await owns durable correlation,
duplicates, dispatch races, terminal states, and continuation. No separate callback store, mapper
registry, or execution path is introduced.

This succeeds ADR-0035's callback deferral and preserves
[ADR-0034's authority boundary](./0034-pinned-http-operations-execute-as-ordinary-connector-capabilities.md).
Independent OpenAPI webhooks remain descriptive admission possibilities, not Command completions.

## Consequences

- Callback contracts and selected mappings participate in release identity; runtime URIs, tokens,
  payloads, credentials, and tenant/account choices do not.
- Imported security declarations constrain compatibility. Application authentication establishes
  provider identity before admission.
- Callback payload, Command acknowledgement, and final pipeline output remain distinct contracts.
- Runtime consumes pins and generated mappings without OpenAPI parsers or documents.
