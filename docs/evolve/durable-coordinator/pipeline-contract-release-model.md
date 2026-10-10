# Pipeline Contract And Release Model

The durable coordinator needs a versioned thing to execute, but that thing should not be a JAR. A JAR is one artifact form. The strategic unit is a pipeline contract, and each deployable version is a release that pins the artifacts satisfying that contract.

The runtime uses generated `pipeline-contract.json`, local `pipeline-release.json` registration, active release pointers, execution pinning to contract/release identity, and release-aware worker availability checks. Release identity is the coordinator and worker compatibility model.

## Core Terms

| Term | Meaning |
| --- | --- |
| Pipeline contract | Generated semantic contract derived from YAML plus compiled metadata: graph, step ids, cardinalities, type ids, mapper and boundary metadata, await metadata, and compatibility identity. |
| Release descriptor | Build-produced closure that selects one deployable version of a pipeline contract, pins every artefact by digest, and names its Compiled Truth carrier. |
| Artifact descriptor | A closed, byte-addressable runtime artefact that satisfies part or all of the Release. |
| Deployment plan | Platform-specific actioning layer: Helm, Kustomize, ECS task definitions, Terraform, Lambda aliases, Azure Functions configuration, or local scripts. |
| Activation | Coordinator decision that new executions should use a specific release. |
| Pinning | Execution record stores the contract/release identity it started with; retries, awaits, and resumes keep that identity. |

## Contract Descriptor

`META-INF/pipeline/pipeline-contract.json` is the generated semantic description of the pipeline, independent of where the code is deployed.

It includes the current compiled metadata TPF can derive deterministically:

1. pipeline id and contract version,
2. ordered graph and step ids,
3. authored step names and kinds,
4. cardinality for each step,
5. input and output type ids,
6. mapper, boundary, and transport metadata needed for compatibility checks,
7. await correlation and transport metadata,
8. compatibility hash over canonical contract content.

The contract is produced from both YAML and code-derived compiler metadata. A YAML-only hash is not enough, because step types, mapper bindings, generated codecs, and await/boundary metadata can change without a visually large YAML diff.

Inter-pipeline handoff contracts remain a follow-up extension.

## Release Descriptor

`pipeline-release.json` is emitted after packaging by the
[Pipeline Release Maven plugin](/deploy/release-descriptors), or by post-push tooling that can observe an
authoritative remote digest. It is sufficient for an independent consumer to resolve and verify every artefact and
recover all Compiled Truth. TPF does not force every artefact through one store.

It includes:

1. pipeline id,
2. contract version,
3. release version,
4. the `compiledTruthArtifactId`,
5. ordered artefact descriptors with kind, canonical URI, SHA-256 digest, step associations, and capability
   associations.

Deployment target metadata, credentials, repository roots, cloud identity, and environment configuration are not
Release semantics.

Example:

```json
{
  "schemaVersion": 1,
  "pipelineId": "payments.csv",
  "contractVersion": "sha256:contractabc",
  "releaseVersion": "2026.06.07.1",
  "compiledTruthArtifactId": "payment-provider-worker",
  "artifacts": [
    {
      "artifactId": "payment-provider-worker",
      "kind": "jar",
      "stepIds": ["await-payment-provider"],
      "uri": "maven:com.example:payment-provider-worker:2026.06.07.1",
      "digest": "sha256:2222222222222222222222222222222222222222222222222222222222222222",
      "capabilities": ["grpc"]
    }
  ]
}
```

An image cannot itself expose ZIP resources to a generic consumer, so an image Release also lists a JAR,
application archive, Lambda ZIP, or `compiled-truth` archive as `compiledTruthArtifactId`.

The coordinator validates and activates releases. It does not become the deployment engine. Platform-specific tools deploy the artifacts, then the coordinator verifies that workers report matching contract/release capability before accepting work.

## Artifact Form Factors

The release descriptor accepts these artifact kinds:

| Kind | Example | Primary backing system | Typical use |
| --- | --- | --- |
| `jar` | `maven:com.example:worker:1.2.3` | Maven repository or local filesystem | JVM worker process and possible Compiled Truth carrier. |
| `application-archive` | `maven:com.example:fast-app:zip:1.2.3` | Maven repository or local filesystem | Closed directory-shaped application such as fast-JAR. |
| `native-binary` | `maven:com.example:worker:bin:1.2.3` | Maven repository or local filesystem | Native worker paired with a Compiled Truth carrier. |
| `container-image` | `oci://ecr.example/payments/worker@sha256:...` | OCI registry: ECR, GHCR, JFrog, Harbor, Docker registry | Kubernetes, ECS, and production container platforms. |
| `lambda-zip` | `maven:com.example:payment-worker:zip:1.2.3` | Maven repository or local filesystem | AWS Lambda ZIP deployment. |
| `lambda-image` | `oci://ecr.example/payment-lambda@sha256:...` | OCI registry, usually ECR for AWS Lambda | AWS Lambda container image. |
| `compiled-truth` | `maven:com.example:payment-truth:zip:1.2.3` | Maven repository or local filesystem | Non-deployable carrier for `META-INF/pipeline/**`. |

Absolute canonical `file:` URIs are valid only for explicitly local releases. Promotable releases use opaque
`maven:` coordinates or digest-qualified `oci:` URIs. External endpoints are Deployment Plan configuration, not
immutable Release artefacts.

Producer and consumer apply the same shared validation: every authored step is assigned to exactly one deployable
artefact, declared runtime capabilities are covered, and the carrier exposes the exact compiler-produced resource
tree.

### S3 Is A Blob Store, Not The Artifact Repository Strategy

The S3-compatible release artefact store is coordinator-owned storage for already resolved and verified blobs. It
does not introduce an `s3:` Release URI profile or replace the artefact's canonical `maven:` identity.

It is not the preferred target for container images. Tools such as Jib produce OCI images; those should be pushed to an OCI registry and referenced from `pipeline-release.json` by immutable digest. TPF should not copy those images into S3.

The release descriptor is the integration point across repositories. A single Release can pin a Jib-produced image
in ECR, a JVM helper artefact in JFrog, and a Lambda ZIP in a Maven repository while the coordinator validates the
contract/Release identity and worker capability reports.

## Ownership Models

The model must not assume one repo, one team, or one artifact.

### One Team Owns The Pipeline

One repository and build can emit the contract, release descriptor, and all artifacts. Activation promotes a single release version.

### Several Teams Own Steps

The central pipeline contract defines required step interfaces and graph position. Each team publishes an artifact that satisfies one or more step contracts. The release descriptor is the integration point that pins the exact artifacts used together.

### Teams Own Chained Pipelines

Each pipeline owns its own contract and release lifecycle. Handoff boundaries become explicit contracts between pipelines, so upstream output compatibility and downstream input compatibility can be checked before activation.

## Drift Detection

The release model should make drift visible at several levels:

| Drift type | Detection target |
| --- | --- |
| Contract drift | YAML graph, step order, cardinality, type ids, mapper metadata, await metadata, or boundary metadata changed. |
| Code drift | A running worker artifact digest differs from the artifact pinned in the release descriptor. |
| Deployment drift | The active release expects endpoints/images/functions that are not deployed or reachable. |
| Runtime drift | Worker capability reports a different pipeline id, contract version, release version, or artifact digest. |

The current worker capability check verifies `pipelineId + contractVersion + releaseVersion`. Artifact digest drift is validated for local/JAR registration and activation, and worker-reported artifact id/digest are matched when both release and worker provide them.

## Standards To Reuse

TPF should define only the pipeline-specific contract and release semantics. The surrounding supply-chain model should reuse common standards and conventions:

1. [OCI Image and Distribution specifications](https://opencontainers.org/) for container image and generic artifact addressing.
2. [SLSA](https://slsa.dev/) and [in-toto](https://in-toto.io/) for build provenance.
3. [SPDX](https://spdx.dev/) or [CycloneDX](https://cyclonedx.org/) for SBOMs.
4. [Sigstore/cosign](https://docs.sigstore.dev/cosign/) for signing.
5. Helm, Kustomize, Terraform, ECS task definitions, Lambda aliases, and cloud-native deployment tools for platform actioning.

CNAB, Open Application Model, Serverless Workflow, and CDEvents are useful references, but they should not replace TPF's typed compiled pipeline contract.

## Relationship To Current Runtime

The self-host runtime retains local file/JAR registration and uses the shared verifier for closures containing
canonical `maven:` locations. It resolves the closure through environment-owned repository configuration, verifies
every artefact digest and reads the contract from the named Compiled Truth carrier before storing verified bytes.
Native Coordinator admission does not configure an OCI resolver. Independent consumers can supply supported Maven
and OCI resolver profiles without changing Release identity. See the current
[Coordinator admission configuration](/deploy/release-descriptors#register-with-a-self-hosted-coordinator).

The coordinator validates, activates, pins, and dispatches releases. Platform-specific tools still deploy artifacts outside TPF.
