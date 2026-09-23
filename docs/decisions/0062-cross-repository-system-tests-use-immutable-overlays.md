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

The `pipelineframework` coordination repository owns an asynchronous system-test train over released artifacts.
Every run resolves one last-known-green baseline OCI manifest to an immutable digest, then overlays one immutable
candidate per changed repository. Maven candidates use a unique version derived from the pull request or `main`
commit. Container candidates are identified only by digest. A coordinated change is a set of those same candidate
overlays; it is not a composite source reactor.

The coordination repository owns suite policy. A publisher may request additional suites but cannot remove a
centrally required suite. Test commands remain owned by the repository that owns the behaviour and are executed at
the exact source SHA recorded in the resolved set.

Privileged intake jobs validate event identity, the current pull-request head, workflow provenance, manifest
checksums and the repository/coordinate allowlist. They hydrate an isolated Maven repository and remove all package
credentials before tests run. Test jobs receive no package, dispatch or commit-status credential. A final trusted
job reports the fixed `tpf/system-tests` status to every candidate SHA.

Baseline promotion is serialized. A green `main` candidate may replace the `main` baseline tag only when the tag
still resolves to the digest used by its test run. A compare-and-swap miss starts a new run against the winning
baseline instead of promoting stale evidence.

## Rationale

The candidate overlay is the smallest unit that proves the repository boundary: downstream source consumes an
immutable published contract rather than the producer's checkout. Exact SHAs, Maven versions, image digests and
manifest digests make failures reproducible. Separating privileged materialisation from unprivileged execution
allows pull-request validation without giving arbitrary test code cross-repository authority.

## Consequences

- Owner-local tests remain the first pull-request gate; the central train complements rather than replaces them.
- Ordinary E2E and non-scale HA suites can block pull requests, while scale, native, cloud and live-provider suites
  remain explicit nightly or release evidence.
- Candidate packages and manifests require retention cleanup, but they are never public release identities.
- GitHub App installation tokens are repository-scoped. Package access uses the coordination repository's explicit
  GitHub Packages grants and is consumed only during trusted materialisation.
- `central-publishing` remains the only Maven profile. Candidate identity and suite selection do not create another
  reactor, profile or source universe.
- A future test runner or registry may replace GitHub Actions or GHCR without changing the candidate, baseline and
  suite-policy contracts.
