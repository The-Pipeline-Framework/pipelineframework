---
title: Builds produce Releases and the TPF CLI deploys them
status: accepted
---

# ADR-0066: Builds Produce Releases and the TPF CLI Deploys Them

## Context

Maven has established lifecycle meanings: `package` creates artefacts, `verify` checks them, `install` stores them
locally, and `deploy` publishes them to an artefact repository. TPF also needs a portable way to verify and deploy an
already-produced Release to local, customer-managed, and service-managed targets without making Maven the deployment
control plane.

The Release Descriptor established by ADR-0064 is already the complete immutable deployment input. Reconstructing
release contents from a Maven project, or introducing a target-specific manifest, would create a second authority.

## Decision

Build integrations produce `pipeline-release.json` after final artefact bytes exist. The Maven integration remains
bound to `verify`; standard `mvn deploy` retains its normal meaning and publishes Maven artefacts. The descriptor is
preserved as an explicit CI artefact rather than being attached to Maven publication by default.

Framework-neutral Release production lives in `pipelineframework-release-producer`. The Maven Mojo is an adapter for
Maven parameters and project layout. Future build integrations use the same producer rather than recreating Release
semantics.

The standalone public `tpf` CLI consumes an existing descriptor. `tpf release verify` resolves and verifies its
complete closure. `tpf deploy <environment>` performs that verification before selecting a Deployment Target. Neither
command builds artefacts or rewrites the descriptor.

Resolver profiles, credentials, endpoints, readiness checks, and target policy live in deployment configuration.
Promotion means deploying identical descriptor bytes to another named environment. The public Cloud target is only a
client of the documented Cloud API; the private TPF Cloud implementation retains its own domain, provisioning, and
Coordinator responsibilities.

```mermaid
flowchart LR
    B[Build tool] -->|final artefacts| R[pipeline-release.json]
    B -->|mvn deploy| M[Maven repository]
    R --> V[tpf release verify]
    V --> D[tpf deploy environment]
    D --> L[Local target]
    D --> C[Public Cloud API]
    D --> I[IaC or custom target]
```

### CLI distribution

The first distribution was the public non-root Java container at `ghcr.io/the-pipeline-framework/tpf`. Native
installation now uses the plain-Java CLI's own Java 25 Native Build Tools/Mandrel configuration and reviewed
reachability metadata. JReleaser packages platform-labelled `BINARY` archives for macOS ARM64 and Linux x64/ARM64,
with Homebrew as the recommended native installation. Windows, Intel macOS and Alpine are outside this distribution.
Native releases are installable only after all platform conformance and installation gates pass.

Native conformance covers JSON/YAML, file/Maven/OCI resolution, credential helpers, device login and refresh,
non-interactive CI authentication, TLS trust and rejection, and exact-byte Cloud registration. Compilation or
Quarkus native CI alone does not establish native CLI conformance. macOS archives are initially unsigned.

Trusted version-tag workflows publish tested stable archives using the repository `GITHUB_TOKEN`. Trusted main
`-SNAPSHOT` builds also publish a moving `latest` prerelease after the same owner, compatibility and native
conformance gates pass. Snapshot archives preserve their version and commit metadata; stale main runs are rejected.
The `latest` archive channel uses only `GITHUB_TOKEN` and does not wait for Homebrew provisioning. Homebrew remains
a stable-release channel; version-tag releases remain immutable. A dedicated GitHub App
restricted to the Homebrew tap supplies short-lived formula-publication tokens. Build jobs receive no publication
credentials. Package publication remains separate from Maven verification or deployment lifecycle goals.

The secondary container uses Java 25, version/full-commit tags and a reported digest. Its public GHCR publication
uses repository `GITHUB_TOKEN` with package-write permission and requires anonymous pulling and published-image
conformance. Container mounts expose host files and persist resolver caches and human credentials; native installation
uses host paths directly. Promotion preserves descriptor bytes and uses Maven/OCI references.

The public Cloud adapter requires available private Cloud and identity APIs. Conformance uses controlled protocol
fixtures and does not establish live Cloud availability. Maven remains the Release producer; no Maven deployment
goal or deployment-target selection is introduced.

## Rationale

This boundary keeps normal build-tool behaviour unsurprising, makes deployment usable from any CI system, and gives
future Gradle and IaC integrations the same Release and resolver contracts. A target receives verified immutable
inputs without learning Maven module layout or compiler output conventions.

## Consequences

- Maven has no TPF environment, credential, or deployment-target model.
- There is no `tpf:deploy` Maven goal and no repurposing of Maven's `deploy` phase.
- The CLI and reusable Java libraries are published from `pipelineframework-cli` and participate in the public
  Compatibility Set.
- Local and remote targets report stages they do not perform as `NOT_REQUESTED` or `EXTERNALLY_MANAGED`; they cannot
  report externally owned work as completed.
- TPF Cloud remains a separate private service. Only its public machine API contract and the thin OSS client cross
  the repository boundary.
- Terraform, OpenTofu, Pulumi, and custom CI remain parallel consumers of the same descriptor and environment inputs.

See [CLI installation](../deploy/cli-installation), [Maven Release production](../deploy/release-descriptors) and
[verification and deployment](../deploy/deployment-cli) for the supported hand-off and separate local, publication,
human Cloud and CI Cloud examples.
