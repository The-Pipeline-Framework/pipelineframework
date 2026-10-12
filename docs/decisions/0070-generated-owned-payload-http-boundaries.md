---
title: Generated HTTP boundaries for owned payloads
status: draft
---

# ADR-0070: Generated HTTP boundaries for owned payloads

## Context

Applications need to receive and return large content while their Pipelines carry
canonical `PayloadReference` values. A multipart parser, storage client, or HTTP response
inside a business step would give that step transport and storage responsibilities.
The existing Object Target write session supports incremental writes. Repository
materialisation produces bytes for a different representation concern and cannot be
the download transport for large content.

## Decision

The compiler will own a pinned declaration of each HTTP upload or download boundary,
including its route identity, canonical reference field, Connector binding, allowed
media types, maximum upload size, and authorisation scope. These are boundaries around
an ordinary typed Pipeline, not new business step kinds or implicit executions.

Generated adapters will call runtime-safe framework APIs. The runtime will check tenant
and business or execution scope before resolving the named Connector binding. Uploads
will pass bounded body chunks to `ObjectTargetProvider` with one outstanding provider
write at a time, enforce size and media policy, and abort incomplete sessions. The
provider-issued reference will retain its content metadata and Connector provenance.
Downloads will use a bounded object-source read session, close it on cancellation, and
will never accept a provider locator as independent authority. A provider must verify
its own issued-reference capability before opening content. Ranges require a separate
explicit capability and are outside the initial boundary.

The first generated boundary will target the service-style `COMPUTE` platform with REST
transport. The compiler must reject unsupported deployment shapes and provider
capabilities at build time. Transport, platform, and Connector operation remain
separate choices.

## Rationale

The Pipeline keeps one typed claim-check identity while the framework owns HTTP parsing,
authorisation, storage lifecycle, and streaming. Provider-held provenance prevents a
caller from changing visible locator fields to read another object.

## Consequences

- The portable read-session contract belongs to `pipelineframework-contracts`; filesystem
  and S3 implementations belong to `pipelineframework-connectors`.
- HTTP lifecycle and authorisation belong to `pipelineframework-runtime`, and declaration,
  diagnostics, generated adapters, OpenAPI, and contract metadata belong to
  `pipelineframework-compiler`.
- Provider recreation or key rotation may invalidate outstanding capabilities. A host
  must fail closed when the issuing authority is unavailable.
- The public boundary cannot ship until filesystem and S3 provenance, generated-route
  authorisation, cancellation, and generated-application proofs are complete.
