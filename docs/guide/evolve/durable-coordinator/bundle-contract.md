# Bundle Contract

A pipeline bundle is best understood as a versioned orchestration contract: pipeline intent, step graph, type ids, codecs, mapper metadata, await metadata, runtime mapping, and compatibility identity.

An executable bundle is one implementation form of that contract: the contract plus step code/runtime packaged as a JAR, image, or deployable artifact. Current local JAR registration proves the executable-bundle path for local/dev and self-host experiments; it is not the only intended product model.

## Manifest

Generated builds emit `META-INF/pipeline/bundle-manifest.json`.

Manifest v1 records pipeline id, bundle version id, bundle hash, runtime metadata, ordered step descriptors, await transport metadata, and declared worker capabilities.

Workers validate command envelope identity before decoding payloads or executing business code. A command targeting another `pipelineId` or `bundleVersionId` returns a failed transition result.

## Registry And Pinning

The local executable-bundle path has two public pieces:

1. `PipelineBundleRegistry` stores bundle records and the active bundle pointer for each tenant and pipeline.
2. `PipelineBundleArtifactStore` copies validated executable JARs into a coordinator-owned content-addressed store and verifies size/checksum/manifest integrity.

Hosted-style execution submission requires `pipelineId`. The coordinator resolves the active bundle, verifies the stored artifact, verifies worker availability, and stores `pipelineId + bundleVersionId` on the `ExecutionRecord`.

Existing executions, retries, await resumes, and result reads stay pinned to the bundle version stored on the execution record even if an administrator activates a newer bundle later.

## Current Limits

Workers must already host matching code. The coordinator does not load registered artifacts into a worker runtime.

Future contract-only and hybrid execution should allow independently deployed services or functions to implement the same contract without requiring every path to be an executable JAR.
