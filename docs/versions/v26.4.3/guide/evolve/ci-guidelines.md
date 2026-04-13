---
search: false
---

# Builds and Continuous Integration (CI)

The project uses three independent workflows:

1. **build.yml** — PR/non‑main builds
    - Fast build
    - Unit tests only
    - No Jib, no native, no integration tests

2. **full-tests.yml** — push to `main`
    - Full clean build
    - Jib Docker images
    - Integration tests
    - Native builds (matrix)

3. **publish.yml** — `v*` tags
    - Release build
    - Deploys to Maven Central
    - No tests (already validated in main)

## Search Cloud Example Workflows

Maintainer-only notes for the Search cloud examples belong here rather than in the user-facing build guides.

### Azure Functions Preview Workflow

- Workflow: `.github/workflows/e2e-search-azure-functions.yml`
- Repository secrets:
  - `AZURE_CLIENT_ID`
  - `AZURE_TENANT_ID`
  - `AZURE_SUBSCRIPTION_ID`
- GitHub OIDC subjects to trust for the current workflow shape:
  - `repo:The-Pipeline-Framework/pipelineframework:ref:refs/heads/main`
  - `repo:The-Pipeline-Framework/pipelineframework:ref:refs/heads/feat/function-cloud` while the stacked feature branch remains the base for this work; remove or replace it when that branch is merged or retired.
- Add `repo:The-Pipeline-Framework/pipelineframework:pull_request` only if the Azure workflow is later moved into PR-triggered CI.
- Prefer dispatching `CI — E2E Test Matrix` so the reusable Azure workflow can consume the already-built framework artifacts.
- If dispatching `e2e-search-azure-functions.yml` directly, set `download_artifacts=false` unless a matching Maven artifact tarball already exists from an earlier run.
