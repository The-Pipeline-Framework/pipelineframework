---
title: Diskless owned-payload delivery by deployment capability
status: draft
---

# ADR-0071: Diskless owned-payload delivery by deployment capability

## Context

ADR-0070 established generated HTTP boundaries around Connector-owned
`PayloadReference` values. Its first COMPUTE upload adapter used Quarkus's staged
`FileUpload`, which consumes local temporary storage. A function host has a separate
limit: AWS Lambda's synchronous request body is capped at 6 MB, regardless of whether
the Java adapter streams it after invocation.

## Decision

The compiler derives the payload delivery shape from the platform and the bound object
provider. REST/COMPUTE uses one live multipart request stream and bounded provider
writes. REST/FUNCTION requires S3 and generates authenticated control operations for
initiation, part capabilities, completion, abort, and download capability. Other
FUNCTION/provider combinations fail compilation. Transport, platform, and object
provider remain separate dimensions.

The S3 multipart upload and its signed reservation are durable across function
instances. Each part URL is short-lived and confined to one upload ID and part number.
Completion lists the actual parts, verifies size and SHA-256 by reading the completed
object in bounded chunks, then issues the provider-signed `PayloadReference`. The
presigned URLs are temporary transfer credentials; they are never accepted as a
canonical payload identity. A download capability is issued only after normal owner,
binding, location, and reference checks. Clients verify its returned size and checksum.

## Rationale

Direct streaming uses no local payload file where the HTTP host can deliver a live
request. Delegation is necessary when the ingress platform buffers and limits an
invocation before application code runs. Keeping ownership and reference issuance
in the framework and Connector preserves the typed Pipeline's claim-check contract.

## Consequences

- A FUNCTION control-plane deployment needs an authenticated HTTP API front door and
  an S3 binding. It does not receive large payload bytes through Lambda.
- The object-reference HMAC key must be durable and shared by all instances.
- S3 lifecycle rules must expire reservation markers and abort incomplete multipart
  uploads after abandoned sessions. Abort remains available for explicit cancellation.
- AWS response streaming is not assumed. FUNCTION downloads use short-lived S3 GET
  capabilities; the canonical `PayloadReference` remains the authority checked first.
- A deployed API Gateway HTTP API and Lambda smoke test is required before the draft
  decision can be marked accepted.
