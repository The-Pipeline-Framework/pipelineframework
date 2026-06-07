# Pipeline Contract And Release Model

The durable coordinator needs a versioned thing to execute, but that thing should not be a JAR. A JAR is one artifact form. The strategic unit is a pipeline contract, and each deployable version is a release that pins the artifacts satisfying that contract.

This page describes the target model. Current runtime code still uses `META-INF/pipeline/bundle-manifest.json` and local executable JAR registration as the first implementation form.

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

`pipeline-contract.json` should be the generated semantic description of the pipeline, independent of where the code is deployed.

It should include:

1. pipeline id and contract version,
2. ordered graph and step ids,
3. authored step names and kinds,
4. cardinality for each step,
5. input and output type ids,
6. mapper, boundary, and transport metadata needed for compatibility checks,
7. await correlation and transport metadata,
8. inter-pipeline handoff contracts when a pipeline publishes or consumes another pipeline boundary,
9. compatibility hash over canonical contract content.

The contract is produced from both YAML and code-derived compiler metadata. A YAML-only hash is not enough, because step types, mapper bindings, generated codecs, and await/boundary metadata can change without a visually large YAML diff.

## Release Descriptor

`pipeline-release.json` should be emitted by a build or release process after artifacts are built and addressable.

It should include:

1. pipeline id,
2. contract version,
3. release version,
4. artifact descriptors for the coordinator-facing worker or individual steps,
5. expected worker capability identities,
6. deployment target metadata when known,
7. artifact digests and optional provenance, SBOM, and signature references.

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

The coordinator should validate and activate releases. It should not become the deployment engine. Platform-specific tools deploy the artifacts, then the coordinator verifies that workers report matching contract/release capability before accepting work.

## Artifact Kinds

The release model should support these artifact kinds:

| Kind | Example | Typical use |
| --- | --- | --- |
| `local-file` | `/var/lib/tpf/artifacts/worker.jar` | Local/self-host pilots and air-gapped installs. |
| `jar` | `file:///opt/tpf/restaurant-worker.jar` or Maven coordinates plus checksum | JVM worker process. |
| `native-binary` | `/opt/tpf/workers/payment-worker` | Quarkus native worker. |
| `container-image` | `oci://ecr.example/payments/worker@sha256:...` | Kubernetes, ECS, and production container platforms. |
| `lambda-zip` | `s3://bucket/payment-worker.zip#sha256:...` | AWS Lambda zip deployment. |
| `lambda-image` | `oci://ecr.example/payment-lambda@sha256:...` | AWS Lambda container image. |
| `external-endpoint` | `https://payments.internal/step` | Pre-existing service that satisfies a step contract. |

Local artifacts are valid, but they still need a digest. Production releases should prefer immutable references such as OCI digests, S3 object version plus checksum, Maven coordinate plus checksum, or a signed local manifest in air-gapped deployments.

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

The current worker capability check already proves the start of runtime drift detection through `pipelineId + bundleVersionId`. The target model should extend that to contract version, release version, and artifact identity.

## Standards To Reuse

TPF should define only the pipeline-specific contract and release semantics. The surrounding supply-chain model should reuse common standards and conventions:

1. [OCI Image and Distribution specifications](https://opencontainers.org/) for container image and generic artifact addressing.
2. [SLSA](https://slsa.dev/) and [in-toto](https://in-toto.io/) for build provenance.
3. [SPDX](https://spdx.dev/) or [CycloneDX](https://cyclonedx.org/) for SBOMs.
4. [Sigstore/cosign](https://docs.sigstore.dev/cosign/) for signing.
5. Helm, Kustomize, Terraform, ECS task definitions, Lambda aliases, and cloud-native deployment tools for platform actioning.

CNAB, Open Application Model, Serverless Workflow, and CDEvents are useful references, but they should not replace TPF's typed compiled pipeline contract.

## Relationship To Current Bundle Manifest

`META-INF/pipeline/bundle-manifest.json` is the current v1 identity artifact. It gives the coordinator and worker enough identity to validate portable transition envelopes and pin executions.

The next runtime step should not delete that path. It should introduce release registration beside the existing local executable artifact registration, then map the current bundle manifest into the new contract/release vocabulary.
