---
title: Cross-repository system tests use immutable candidate overlays
status: accepted
---

# ADR-0062: Cross-repository system tests use immutable candidate overlays

## Context

TPF's Contracts, compiler, runtime, Connectors, Blocks, Expansions, examples, reference implementations and real
applications now have independent repository and release ownership. Owner-local verification proves each checkout,
but it cannot prove that a changed artifact remains compatible with all downstream consumers. Reconstructing a
source monorepo for tests would erase the released-contract boundary established by the split.

Cross-repository validation also executes source from pull requests. Publication credentials, package-read
credentials and commit-status authority must not be available to that code.

## Decision

The `pipelineframework` coordination repository owns one centrally orchestrated product-test run over released
artifacts. Every run resolves one last-known-green baseline OCI manifest to an immutable digest, then overlays one
immutable candidate per changed repository. Maven candidates use a unique version derived from the pull request or
`main` commit. Container candidates are identified only by digest. A coordinated change is an explicitly submitted
set of those same candidate overlays; it is not a composite source reactor and it does not wait in a debounce
window for unrelated work.

The coordination repository owns suite policy. A publisher may request additional suites but cannot remove a
centrally required suite. Selected suites are grouped into a small number of product-shaped shards: compiler and
semantic compatibility, runtime deployment, ecosystem compatibility, examples/reference systems and
applications/ordinary HA. A resolved candidate set is hydrated once; the credential-free result is reused by each
selected shard. Test commands remain owned by the repository that owns the behaviour and are executed at the exact
source SHA recorded in the resolved set.

Changes limited to centrally allowlisted non-semantic paths do not start product shards. Mixed or unknown paths
fall back to the component's complete pull-request mapping. Ordinary HA lanes that were already sufficient after
merge remain post-merge checks; scale, native, cloud and live-provider lanes remain nightly or release evidence.

Privileged intake jobs validate event identity, the current pull-request head, both the unprivileged build run and
the trusted default-branch publisher run, manifest checksums and the repository/coordinate allowlist. They hydrate
an isolated Maven repository and remove all package
credentials before tests run. Test jobs receive no package, dispatch or commit-status credential. A final trusted
job reports the fixed `tpf/system-tests` status to every candidate SHA.

Baseline promotion is serialized. A green `main` candidate may replace the `main` baseline tag only while its SHA is
still the source repository's default-branch head and the tag still resolves to the digest used by its test run.
This prevents a slower, superseded build from moving a component backwards. A compare-and-swap miss starts a new
run against the winning baseline instead of promoting stale evidence.

## Rationale

The candidate overlay is the smallest unit that proves the repository boundary: downstream source consumes an
immutable published contract rather than the producer's checkout. Exact SHAs, Maven versions, image digests and
manifest digests make failures reproducible. Separating privileged materialisation from unprivileged execution
allows pull-request validation without giving arbitrary test code cross-repository authority.

## Consequences

- Owner-local tests remain the first pull-request gate; the central train complements rather than replaces them.
- The fixed `tpf/system-tests` result remains the cross-repository quality gate. A split repository is not considered
  quality-equivalent until its required central result succeeds.
- Pull requests run the product shards required by semantic impact. Established post-merge HA remains post-merge,
  while scale, native, cloud and live-provider suites remain explicit nightly or release evidence.
- Explicit compatibility sets coalesce related repository changes immediately; singleton candidates are never
  delayed merely to create a batching opportunity.
- Candidate packages and manifests require retention cleanup, but they are never public release identities.
- GitHub App installation tokens are repository-scoped. Package access uses the coordination repository's explicit
  GitHub Packages grants and is consumed only during trusted materialisation.
- `central-publishing` remains the only Maven profile. Candidate identity and suite selection do not create another
  reactor, profile or source universe.
- A future test runner or registry may replace GitHub Actions or GHCR without changing the candidate, baseline and
  suite-policy contracts.
