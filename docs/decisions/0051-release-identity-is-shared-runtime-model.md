---
title: Release identity is shared runtime model
status: accepted
---

# ADR-0051: Release identity is shared runtime model

## Context

Pipeline contract identity, pinned deployable artifacts, release lifecycle state, and the immutable record exchanged with a release registry are consumed by execution admission, durable payload resolution, worker compatibility checks, and release storage. Those value contracts lived beside Quarkus runtime implementations even though they depend only on JDK types and the existing shared pipeline contract model.

Leaving them in `pipelineframework` would force other runtime hosts and separately deployed infrastructure to load the Quarkus runtime implementation merely to agree on release and artifact identity.

## Decision

`pipelineframework-runtime-core` owns `PipelineReleaseDescriptor`, `PipelineReleaseArtifactDescriptor`, `PipelineReleaseRecord`, and `PipelineReleaseStatus`. They retain their packages, constructors, validation, collection snapshotting, schema version, and lifecycle transition behaviour.

Release registries, artifact stores, loaders, registration and activation services, admin resources, codecs, persistence implementations, provider selection, configuration, and Quarkus/CDI wiring remain in `pipelineframework`.

The release registry interface is not promoted to the general provider SPI by this decision. It coordinates self-hosted control-plane lifecycle and requires a separate ownership decision if another runtime implementation needs to provide it. Artifact stores also remain implementation-owned because their current contract exposes local `Path` mechanics rather than a portable artifact-source abstraction.

## Rationale

Release identity is shared semantic data, not a runtime implementation detail and not a transport protocol. Runtime core is the existing owner for JDK-only generated pipeline contracts and is therefore the narrowest released artifact both execution sides can consume.

Keeping lifecycle services and storage implementations out preserves the customer-runtime/infrastructure boundary and avoids treating current Quarkus construction or storage choices as interoperability contracts.

## Consequences

- Runtime hosts and workers can compare contract, release, artifact, and digest identity without depending on `pipelineframework`.
- Java source and binary compatibility apply to these public records and enum.
- Release descriptor schema compatibility and any persisted or serialized record representations require explicit compatibility review.
- Maven artifact versions select compatible model code; pipeline, contract, release, artifact, digest, and checksum fields remain release-pinned identities.
- Runtime implementation tests continue to prove registration, activation, file, Dynamo, and artifact-verification behaviour.
