# Bundle Manifest Identity

The durable coordinator now uses the pipeline contract and active release as its admin/execution concept. `META-INF/pipeline/bundle-manifest.json` remains the v1 executable-worker identity artifact.

The manifest is still required for local executable JAR artifacts because workers need to reject transition commands targeting code they do not host. See [Pipeline Contract And Release Model](/guide/evolve/durable-coordinator/pipeline-contract-release-model) for the release model covering container images, native binaries, functions, local files, external endpoints, and independently deployed step artifacts.

## Manifest

Generated builds emit `META-INF/pipeline/bundle-manifest.json`.

Manifest v1 records pipeline id, bundle version id, bundle hash, runtime metadata, ordered step descriptors, await transport metadata, and declared worker capabilities.

Workers validate command envelope identity before decoding payloads or executing business code. A command targeting another `pipelineId` or `bundleVersionId` returns a failed transition result.

## Release Registry And Pinning

The local executable-artifact path has three runtime pieces:

1. `PipelineReleaseRegistry` stores release records and the active release pointer for each tenant and pipeline.
2. `PipelineReleaseRegistrar` validates `pipeline-release.json` and verifies artifact identity.
3. `PipelineBundleArtifactStore` copies validated executable JAR artifacts into a coordinator-owned content-addressed store and verifies size/checksum/manifest integrity.

Hosted-style execution submission requires `pipelineId`. The coordinator resolves the active release, verifies the stored artifact, verifies worker availability, and stores `pipelineId + contractVersion + releaseVersion + bundleVersionId` on the `ExecutionRecord`.

Existing executions, retries, await resumes, and result reads stay pinned to the release identity stored on the execution record even if an administrator activates a newer release later.

## Current Limits

Workers must already host matching code. The coordinator does not load registered artifacts into a worker runtime.

The legacy `/bundles` admin endpoints remain local compatibility helpers. New self-host flows should register and activate releases.
