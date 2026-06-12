# Pipeline Contract And Release Model

The durable coordinator needs a versioned thing to execute, but that thing should not be a JAR. A JAR is one artifact form. The strategic unit is a pipeline contract, and each deployable version is a release that pins the artifacts satisfying that contract.

The runtime uses generated `pipeline-contract.json`, local `pipeline-release.json` registration, active release pointers, execution pinning to contract/release identity, and release-aware worker availability checks. Release identity is the coordinator and worker compatibility model.

## Core Terms

| Term | Meaning |
| --- | --- |
| Pipeline contract | Generated semantic contract derived from YAML plus compiled metadata: graph, step ids, cardinalities, type ids, mapper and boundary metadata, await metadata, and compatibility identity. |
| Release descriptor | Build-produced descriptor that selects one deployable version of a pipeline contract and pins exact artifacts by digest or immutable reference. |
| Artifact descriptor | A concrete runtime artifact that satisfies part or all of the release: local file, JAR, native binary, container image, function package, or external endpoint. |
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

`pipeline-release.json` is emitted by a build or release process after artifacts are built and addressable in the system that naturally owns that artifact form. TPF should not force every artifact through one store.

It includes:

1. pipeline id,
2. contract version,
3. release version,
4. artifact descriptors for the coordinator-facing worker or individual steps,
5. artifact digests.

Expected worker capability identities, deployment target metadata, provenance, SBOM, and signature references remain follow-up fields.

Example:

```json
{
  "schemaVersion": 1,
  "pipelineId": "payments.csv",
  "contractVersion": "sha256:contractabc",
  "releaseVersion": "2026.06.07.1",
  "artifacts": [
    {
      "artifactId": "payment-provider-worker",
      "kind": "container-image",
      "scope": "step",
      "stepIds": ["await-payment-provider"],
      "uri": "oci://123456789012.dkr.ecr.eu-west-1.amazonaws.com/payment-provider@sha256:image222",
      "digest": "sha256:image222",
      "runtime": "jvm",
      "capabilities": ["transition-worker", "step:await-payment-provider"]
    }
  ]
}
```

The coordinator validates and activates releases. It does not become the deployment engine. Platform-specific tools deploy the artifacts, then the coordinator verifies that workers report matching contract/release capability before accepting work.

## Artifact Form Factors

The release descriptor accepts these artifact kinds:

| Kind | Example | Primary backing system | Typical use |
| --- | --- | --- |
| `local-file` | `/var/lib/tpf/artifacts/worker.jar` | local filesystem or managed blob store | Local/self-host pilots and air-gapped installs. |
| `jar` | Maven coordinate plus checksum or `file:///opt/tpf/restaurant-worker.jar` | Maven repository, JFrog Artifactory, Nexus, or managed blob store | JVM worker process. |
| `native-binary` | `/opt/tpf/workers/payment-worker` | local filesystem, generic OCI artifact, or managed blob store | Quarkus native worker. |
| `container-image` | `oci://ecr.example/payments/worker@sha256:...` | OCI registry: ECR, GHCR, JFrog, Harbor, Docker registry | Kubernetes, ECS, and production container platforms. |
| `lambda-zip` | `s3://bucket/payment-worker.zip#sha256:...` | S3 or S3-compatible object store | AWS Lambda zip deployment. |
| `lambda-image` | `oci://ecr.example/payment-lambda@sha256:...` | OCI registry, usually ECR for AWS Lambda | AWS Lambda container image. |
| `external-endpoint` | `https://payments.internal/step` | existing service deployment and service discovery | Pre-existing service that satisfies a step contract. |

Local artifacts are valid, but they still need a digest. Current runtime validation is strongest for local/JAR artifacts. Container, function, and external endpoint artifacts are descriptor-level identities until platform-specific deployers and richer worker capability metadata mature.

Production releases should prefer immutable references in the artifact's native repository: OCI digests for images, Maven coordinates plus checksum for JVM artifacts, S3 object version plus checksum for ZIP/blob artifacts, or a signed local manifest in air-gapped deployments.

### S3 Is A Blob Store, Not The Artifact Repository Strategy

The S3-compatible release artifact store exists so a self-hosted coordinator can copy and verify blob-like artifacts that do not already live in a better artifact repository. Good fits are local/JAR/native artifacts in small self-host installs, Lambda ZIP packages, release descriptor blobs, provenance attachments, and MinIO/LocalStack development setups.

It is not the preferred target for container images. Tools such as Jib produce OCI images; those should be pushed to an OCI registry and referenced from `pipeline-release.json` by immutable digest. TPF should not copy those images into S3.

The release descriptor is the integration point across repositories. A single release can pin a Jib-produced image in ECR, a JVM helper artifact in JFrog, and a Lambda ZIP in S3, while the coordinator validates the contract/release identity and worker capability reports.

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

The current self-host runtime registers releases directly. For local/JAR artifacts, registration can inspect embedded `META-INF/pipeline/pipeline-contract.json` and rejects artifacts whose contract identity does not match the release descriptor. For container images, the runtime treats the digest as release identity and relies on platform deployment plus worker capability checks; it does not pull, unpack, or copy image layers.

The coordinator validates, activates, pins, and dispatches releases. Platform-specific tools still deploy artifacts outside TPF.
