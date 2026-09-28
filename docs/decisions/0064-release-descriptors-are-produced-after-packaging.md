---
title: Release descriptors are produced after packaging
status: accepted
---

# ADR-0064: Release Descriptors Are Produced After Packaging

## Context

The compiler emits semantic Compiled Truth before packaging finishes. It cannot know the final deployable bytes or
their immutable repository addresses. A deployment consumer must nevertheless be able to start with only
`pipeline-release.json`, resolve every artefact, verify every digest, and recover the complete
`META-INF/pipeline/**` tree without locating build outputs by convention.

## Decision

The shared Release Descriptor is the single authoritative Release model. It identifies every independently
deployable artefact, names one of its artefacts as `compiledTruthArtifactId`, and pins each artefact with a supported
kind, canonical URI, lowercase SHA-256 digest, step associations, and capability associations.

The named Compiled Truth carrier must be an inspectable JAR or archive containing the complete compiler-produced
`META-INF/pipeline/**` tree. A native binary therefore has a separate `compiled-truth` archive unless another
deployable archive carries the same resources. Directory-shaped deployments such as Quarkus fast-JAR are
materialised as one deterministic `application-archive` before hashing.

The `pipelineframework-runtime` distribution owns a framework-neutral Maven plugin that produces the descriptor
after packaging for local byte artefacts. Shared release validation, canonical URI parsing, digest verification,
artefact resolution, and Compiled Truth recovery remain in `pipelineframework-runtime-core` so producers and
independent consumers apply one contract.

Canonical locations are absolute `file:` URIs for explicitly local releases, opaque `maven:` coordinates for
repository artefacts, and digest-qualified `oci:` URIs for images. Credentials, mirrors, repository roots, cloud
accounts, regions, and deployment targets belong to the resolver or Deployment Plan, not the Release Descriptor.

The descriptor remains external to the artefacts it identifies. Embedding it in a hashed artefact would create a
circular identity.

## Rationale

The post-package boundary is the first point that knows both Compiled Truth and exact local bytes. A closed Release
lets Cloud or another deployment consumer reconstruct the same immutable input in different environments by
changing only resolver configuration. Keeping resolution policy outside the descriptor also permits Maven and OCI
repositories without changing Release semantics.

## Consequences

- Applications provide an explicit immutable Release version and stable repository URIs for promotable releases.
- Every authored step is assigned to exactly one deployable artefact, and declared runtime capabilities are covered
  by the deployable artefacts.
- `file:` is an explicit local-development mode and is not a promotable Release location.
- The local-byte producer supports JARs, application archives, native binaries, Lambda ZIPs, and Compiled Truth
  archives. Tooling that can observe a pushed OCI digest produces image entries using the same shared model.
- Deployment environment, credentials, Cloud identity, and target configuration stay outside Release semantics.
- There is no second bundle or deployment manifest alongside `pipeline-release.json`.
