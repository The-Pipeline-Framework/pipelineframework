---
title: OpenAPI imports release-pinned capabilities, not authority
status: accepted
---

# ADR-0035: OpenAPI imports release-pinned capabilities, not authority

## Context

Many SaaS APIs publish useful OpenAPI contracts. Requiring one runtime Connector per vendor repeats
HTTP projection, response interpretation, capture, effect, and callable-dispatch work that TPF
already owns. OpenAPI nevertheless describes wire possibilities rather than application authority:
an HTTP method, `operationId`, server, security declaration, or vendor extension cannot decide that
an operation is safe to observe, approve an effect, or select a credential and tenant.

The source contract may also differ from an application's canonical types. That difference must use
TPF's existing deterministic representation boundary rather than introducing an OpenAPI type or
runtime interpretation model.

## Decision

OpenAPI 3.0.0–3.0.4 and 3.1.0–3.1.2 are release-time capability sources. Acquisition is an explicit,
network-capable action that vendors a bounded HTTPS reference closure and records original byte
digests. Discovery is local and descriptive. Import requires an author to select each operation and
choose a stable TPF identity and major version, Query or Command kind, canonical contracts, request
media, response outcomes, one advertised security alternative, and application-bound server policy.

Import emits a standard `http.client` provider manifest, immutable HTTP operation pins, and
sanitized provenance. It does not emit executable vendor code. Normal compilation resolves direct,
bounded-option, or curated representation mappings and generates immutable mapper bindings. An
optional packaged mapping Block may ask an application-bound LLM Query for a bounded proposal, but
deterministic validation, author review, committed mapping options, and compiler acceptance remain
authoritative.

Runtime consumes only the ordinary Connector manifest, HTTP pins, generated mapper bindings, and
application connector binding. It neither contains nor parses the OpenAPI contract. The host owns
the base URI, borrowed HTTP client, authorization and credential lifecycle, tenant/account choice,
and connection policy. Commands additionally retain application-owned identity generation,
duplicate policy, and `CommandPolicy`.

Import and callable exposure are distinct. An imported operation becomes model-callable only when
an existing LLM Query catalogue or packaged Block explicitly exposes it. Dynamic calls continue
through the existing native Query/Command dispatch path.

## Rationale

This treats OpenAPI as a rich release input while making the normalized source format irrelevant to
execution. Explicit classification prevents method-based authority escalation, committed pins make
wire behavior reproducible, and the generic HTTP Connector removes the need for routine
vendor-specific adapters without weakening host or Command ownership.

## Consequences

- A usable SaaS OpenAPI contract can supply selected ordinary Query and Command capabilities without
  a vendor Connector.
- `acquire` is the only network-capable goal; discovery, import verification, compilation, and
  runtime remain offline with respect to the source contract.
- Local and explicitly allowed HTTPS references are bounded and fingerprinted. OpenAPI 3.2 is
  rejected until separately qualified.
- Server declarations and security schemes are compatibility hints only; no endpoint or credential
  is imported.
- Ambiguous schema constructs progress through direct mapping, an author-reviewed LLM-assisted
  proposal, or an explicit curated DTO/Mapper. No model executes at runtime.
- Generated capability provenance and accepted mapping fingerprints participate in the existing
  schema-3 pipeline contract hash without exposing documents or secrets.
- OpenAPI callbacks are not redefined here. A later decision may map suitable correlated callbacks
  onto existing Command and Await semantics.

The callback deferral above is succeeded by
[ADR-0038](./0038-openapi-callbacks-are-pinned-connector-completion-contracts.md); the capability and
authority distinctions in this decision remain accepted.
