---
title: Release descriptors are produced after packaging
status: accepted
---

# ADR-0064: Release Descriptors Are Produced After Packaging

## Context

The compiler emits the semantic Pipeline Contract before packaging completes. At that point the final deployable
bytes and their address do not yet exist, so the compiler cannot truthfully produce an immutable Release identity.
Consumers had to construct `pipeline-release.json` themselves even though the shared schema already described the
required contract, artifact, URI, and digest identities.

## Decision

The `pipelineframework-runtime` distribution owns a framework-neutral Maven plugin that produces
`pipeline-release.json` after packaging. It reads the compiler-produced Pipeline Contract, hashes the exact local
artifact bytes, records their release addresses, and serialises the existing shared release model.

The compiler continues to own `pipeline-contract.json`. `pipelineframework-runtime-core` continues to own the
public `PipelineReleaseDescriptor` and `PipelineReleaseArtifactDescriptor` records. Coordinators and hosted control
planes ingest and validate the produced descriptor; they do not manufacture the Release.

The descriptor remains external to the artifact it identifies. Embedding it would change the bytes being hashed
and create a circular identity.

## Rationale

Maven's post-package lifecycle is the narrowest build boundary that knows both Compiled Truth and the final local
artifact bytes. Keeping the producer independent of Quarkus lets ordinary JAR, native binary, and Lambda ZIP builds
use the same release contract without moving packaging knowledge into the compiler or Cloud.

## Consequences

- Applications opt in to the release goal and provide an explicit immutable release version.
- The producer can infer a local primary JAR, while modular releases configure an ordered artifact list.
- OCI image and external-endpoint digests remain with post-push tooling that can observe their authoritative remote
  identity.
- Changing artifact bytes changes the descriptor digest; reusing an existing release identity with different local
  descriptor content is rejected where the previous descriptor remains available.
- Deployment Plans, Activation, infrastructure provider state, and hosted-service identity stay outside the Release
  Descriptor.
