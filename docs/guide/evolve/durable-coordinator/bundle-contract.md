# Contract And Release Identity

The durable coordinator uses two identities:

1. `pipeline-contract.json` describes the semantic pipeline contract produced by compilation.
2. `pipeline-release.json` pins the deployable artifacts that satisfy that contract.

The release is the coordinator admin concept. New executions are pinned to the active `pipelineId + contractVersion + releaseVersion`, and retries, await resumes, and result reads keep that pinned identity.

## Generated Contract

Generated builds emit `META-INF/pipeline/pipeline-contract.json`.

The contract records pipeline id, contract version/hash, runtime metadata, ordered step descriptors, await transport metadata, and declared worker capabilities. Workers validate command envelope identity before decoding payloads or executing business code.

## Release Registry And Pinning

The self-host release path has three runtime pieces:

1. `PipelineReleaseRegistry` stores release records and activation history for each tenant and pipeline.
2. `PipelineReleaseRegistrar` validates `pipeline-release.json` and verifies artifact identity.
3. `PipelineReleaseArtifactStore` stores local executable artifacts in a coordinator-owned content-addressed store where applicable.

Hosted-style execution submission requires `pipelineId`. The coordinator resolves the active release, verifies the stored artifact, verifies worker availability, and stores `pipelineId + contractVersion + releaseVersion` on the `ExecutionRecord`.

Existing executions, retries, await resumes, and result reads stay pinned to the release identity stored on the execution record even if an administrator activates a newer release later.

## Current Limits

Workers must already host matching code. The coordinator validates, activates, pins, and dispatches releases; it does not dynamically load registered artifacts into a worker runtime.

Local artifact storage is still local to the coordinator host. Multi-host artifact replication remains deployment-owned until a shared artifact-store provider exists.
