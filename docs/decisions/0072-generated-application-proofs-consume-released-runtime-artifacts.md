---
title: Generated application proofs consume released runtime artifacts
status: draft
---

# ADR-0072: Generated application proofs consume released runtime artifacts

## Context

The AWS Durable application proof and two generated Spring application smoke
modules lived in the runtime reactor. Their placement let same-reactor artifacts
satisfy application dependencies and mixed proof fixture changes with production
runtime changes. ADR-0059 kept runtime integration tests with runtime but did not
distinguish adapter implementation tests from generated application consumers.

## Decision

`pipelineframework-examples` owns `aws-durable-proof`, `spring-smoke-tests` and
`spring-blocking-smoke-tests` as non-published application compatibility proofs.
Its single canonical reactor consumes released runtime, adapter, host and compiler
artifacts through the tested component set. It does not use sibling source paths
or copy runtime implementations or production templates.

`pipelineframework-runtime` retains production adapters, stores, mechanical AWS
hosts and their unit/provider/deployment integration tests. This refines the smoke
test boundary in [ADR-0059](./0059-learning-examples-consume-released-artifacts.md);
it does not move runtime semantics or change the supported application contract.

## Consequences

- The examples gate executes generated Spring applications, blocking offload
  coverage and deployment-free Durable proof controls against released artifacts.
- Durable conformance reads the production template from the deployment artifact.
  Runtime/compiler/BOM revisions remain distinct from the examples producer
  revision; a clean examples checkout cannot attest runtime build provenance.
- The protected AWS workflow moves with its proof. The new owner requires its own
  environment, role and OIDC trust configuration; moving source does not copy
  credentials, authorize deployment or establish production conformance.
- Coordinated changes use exact candidate-set compatibility across both owners.
  The move preserves tests and acceptance obligations; local proof success does
  not certify AWS production support or general Spring parity.
