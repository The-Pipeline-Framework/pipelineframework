# Bundle Contract

The current durable coordinator uses `META-INF/pipeline/bundle-manifest.json` as its v1 identity artifact. It is enough to validate local executable artifacts, pin executions, and reject workers that host the wrong `pipelineId + bundleVersionId`.

The target concept is broader: a pipeline contract plus a release descriptor. See [Pipeline Contract And Release Model](/guide/evolve/durable-coordinator/pipeline-contract-release-model) for the design direction covering container images, native binaries, functions, local files, external endpoints, and independently deployed step artifacts.

## Manifest

Generated builds emit `META-INF/pipeline/bundle-manifest.json`.

Manifest v1 records pipeline id, bundle version id, bundle hash, runtime metadata, ordered step descriptors, await transport metadata, and declared worker capabilities.

Workers validate command envelope identity before decoding payloads or executing business code. A command targeting another `pipelineId` or `bundleVersionId` returns a failed transition result.

## Current Registry And Pinning

The local executable-artifact path has two runtime pieces:

1. `PipelineBundleRegistry` stores bundle records and the active bundle pointer for each tenant and pipeline.
2. `PipelineBundleArtifactStore` copies validated executable JARs into a coordinator-owned content-addressed store and verifies size/checksum/manifest integrity.

Hosted-style execution submission requires `pipelineId`. The coordinator resolves the active bundle, verifies the stored artifact, verifies worker availability, and stores `pipelineId + bundleVersionId` on the `ExecutionRecord`.

Existing executions, retries, await resumes, and result reads stay pinned to the bundle version stored on the execution record even if an administrator activates a newer bundle later.

## Current Limits

Workers must already host matching code. The coordinator does not load registered artifacts into a worker runtime.

Future release registration should allow independently deployed services or functions to satisfy the same pipeline contract without requiring every path to be a registered executable JAR.
