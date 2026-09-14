---
title: Remote worker protocols are shared runtime contracts
status: accepted
---

# ADR-0050: Remote worker protocols are shared runtime contracts

## Context

Customer-hosted runtimes and separately deployed transition workers must agree on more than transition request and result payloads. They also exchange worker capability and release identity, transport protocol versions, payload encodings, REST and gRPC signature paths, SQS wrapper envelopes, and authenticated-request canonicalization.

Those values lived beside the Quarkus runtime clients, resources, and pollers. That ownership would require another process to depend on the runtime implementation or duplicate security-sensitive wire rules. The Java values and canonicalization themselves have no Quarkus, HTTP-client, AWS-client, dependency-injection, configuration, tenant, or connection dependency.

## Decision

`pipelineframework-runtime-protocol` owns:

- `PipelineWorkerCapability` and its protocol version;
- REST, gRPC, and SQS transition-worker protocol identities, paths, and encodings;
- signed SQS request and response envelope records; and
- transition-worker signature header names, canonicalization, HMAC generation, timestamp parsing, and constant-time comparison.

These are public interoperability contracts. Their packages and serialized shapes remain stable, and deterministic signature vectors protect canonicalization compatibility.

Transport clients and servers, Quarkus-generated gRPC adapters, HTTP and AWS SDK usage, secret resolution, nonce replay state, worker availability policy, worker selection, configuration, and release identity resolution remain in `pipelineframework`. `PipelineBundleCapabilities` remains in `pipelineframework-runtime-core` because it is generated pipeline-contract semantics rather than a transport-specific worker advertisement.

Tenant and connection context, credentials, runtime provider internals, and worker lifecycle state must not be added to the protocol artifact.

## Rationale

Both sides of a remote worker boundary can consume a released protocol artifact instead of changing source atomically. Keeping protocol identities and signature canonicalization with their wire values prevents transport implementations from redefining compatibility or authentication semantics.

## Consequences

- A worker implementation can advertise compatibility and implement REST, gRPC, or SQS framing without loading the TPF runtime implementation.
- Source and binary compatibility apply to the public Java API; serialized, route, protocol-version, and cryptographic canonicalization compatibility additionally apply across processes.
- Maven artifact versions select compatible protocol code. Pipeline, contract, release, artifact, and digest fields retain their existing release-pinned identities.
- Changing a path, encoding, envelope shape, or signature canonicalization requires an explicit protocol compatibility decision and matching client/server coverage.
- Runtime configuration defaults remain runtime-owned and must continue to agree with the protocol constants.
