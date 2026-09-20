# Builds and Continuous Integration (CI)

The primary framework, release, and docs checks are split across these workflows:

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
    - Promotes the exact released commit to the `release-docs` branch after the release workflow succeeds
    - No tests (already validated in main)

4. **docs.yml** — documentation pull requests
    - Validates the docs build and generated replay assets
    - Does not publish the public docs site

Cloudflare Pages keeps its Git integration enabled. Pushes to `main` publish the staging site at
`https://pipelineframework.pages.dev`, and pull-request branches keep their temporary preview
deployments. The public `https://pipelineframework.org` domain targets the
`release-docs.pipelineframework.pages.dev` branch alias, so it advances only after a successful
Maven Central release.

## Search Cloud Example Workflows

Maintainer-only notes for the Search cloud examples belong here rather than in the user-facing build guides.

### Azure Functions Preview Workflow

- Repository: `The-Pipeline-Framework/pipelineframework-reference-implementations`
- Workflow: `.github/workflows/e2e-search-azure-functions.yml`
- Repository secrets:
  - `AZURE_CLIENT_ID`
  - `AZURE_TENANT_ID`
  - `AZURE_SUBSCRIPTION_ID`
- GitHub OIDC subjects to trust for the current workflow shape:
  - `repo:The-Pipeline-Framework/pipelineframework-reference-implementations:ref:refs/heads/main`
- Add `repo:The-Pipeline-Framework/pipelineframework-reference-implementations:pull_request` only if the Azure workflow is later moved into PR-triggered CI.
- Dispatch `e2e-search-azure-functions.yml` directly. It consumes released TPF artifacts and does not require a monorepo artifact tarball.
