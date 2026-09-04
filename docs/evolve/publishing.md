# Publishing

## Public reactor

`framework/pom.xml` is the Maven reactor for the supported TPF distribution.
Its explicit public coordinate set, including artifact packaging, lives in
`framework/public-artifacts.json`. The representation-provider fixture and
structural connector/plugin POMs retain local coordinates but are deliberately
excluded from Maven Central.

The quality lane runs `clean verify`; publication then uses one separate
`clean deploy -Pcentral-publishing` invocation rooted at `framework/pom.xml`.
Use `publication-verification` with a temporary file repository to inspect the
exact deployed set before Central publication.

Publishing TPF has five related but separate responsibilities:

1. publish immutable Java framework releases to Maven Central,
2. publish the current framework `-SNAPSHOT` from `main` to Sonatype Central's snapshots repository,
3. snapshot the docs site for the released version,
4. promote the exact released commit to the public docs site,
5. publish exact-version release and current-snapshot author knowledge used by `mcp.pipelineframework.org`.

Use this page as the release front door. The older full procedure remains available as [Publishing Reference](/evolve/publishing-reference).

## Release Path

| Need | Page |
| --- | --- |
| Cut and publish framework artifacts | [Framework Release Process](/evolve/framework-release-process) |
| Consume or republish the current framework snapshot | [Publishing Reference — Nightly Snapshot Publishing](/evolve/publishing-reference#nightly-snapshot-publishing) |
| Validate docs snapshots and route rewrites | [Docs Snapshot Process](/evolve/docs-snapshot-process) |
| Publish or recover versioned author MCP knowledge | [TPF Author MCP Operations](/evolve/author-mcp-operations) |
| Troubleshoot Maven Central details | [Publishing Reference](/evolve/publishing-reference) |

## Docs deployment

Cloudflare Pages builds `main` automatically at `https://pipelineframework.pages.dev`. This is the
staging site and may contain changes that have merged but are not yet available from Maven Central.
Cloudflare's pull-request preview deployments remain enabled and are independent of the public docs
promotion.

The tagged release workflow promotes its exact release commit to the mutable `release-docs` branch
only after Maven Central publication, GitHub release creation, and the other release steps succeed.
Cloudflare builds that branch as a preview deployment, and the `https://pipelineframework.org`
custom domain targets the `release-docs.pipelineframework.pages.dev` branch alias. Consequently, a
merge to `main` can update staging without advancing the public docs site.

The Cloudflare project must keep `main` as its production branch and continue building all preview
branches. Its proxied DNS record for `pipelineframework.org` must target
`release-docs.pipelineframework.pages.dev`, not `pipelineframework.pages.dev`.

## Author MCP knowledge

Author MCP publication is part of the tagged release and nightly snapshot workflows, not a separate publishing procedure. After Maven Central deployment and GitHub release creation succeed, `publish.yml` dispatches the named `tpf-mcp-bridge` publication workflow with the exact version, tag, and full release commit. After a snapshot successfully reaches Maven Central, `publish-snapshots.yml` dispatches the same workflow with the exact `-SNAPSHOT` version and checked-out `main` commit. The bridge retrieves the immutable Repowise export stored for that commit, checks out the exact source, applies the author-only scope filter, publishes the D1/R2 bundle, verifies it, and activates either the immutable release or the current snapshot alias.

One-time repository setup requires the `TPF_MCP_DISPATCH_TOKEN` Actions secret in `pipelineframework`. Use a narrowly scoped fine-grained token or GitHub App token that can dispatch workflows in `The-Pipeline-Framework/tpf-mcp-bridge` with Actions write permission; it does not need bridge Contents write permission. This secret is release infrastructure, not a per-release maintainer task.

The live `.repowise` database is not committed: it contains mutable local index state, locks, and machine-specific data. In each configured clone, MCP export and delivery are attached to one stable, clean `main` checkout; their shared Git-hook blocks ignore activity from sibling feature worktrees. They cover locally created commits and newly merged or pulled commits in that checkout. Each healthy, non-stale export is written first to a durable local outbox and then uploaded to private Cloudflare R2 under a commit-addressed immutable key. A network or Cloudflare failure leaves the export queued for bounded retries and later Git activity; it never discards the only copy. Any authorized maintainer with a healthy index for that exact commit can supply the immutable input. This work happens as the knowledge base is refreshed, outside the release critical path; maintainers do not prepare or upload a separate bundle while cutting a release.

The downstream MCP publication waits briefly for a delayed delivery but never substitutes another commit's knowledge. If the exact commit-addressed export remains missing or its checksum/provenance does not match, only the MCP knowledge publication fails with an actionable diagnostic; the workflow does not re-index with model credentials or silently publish stale content. After connectivity returns and the queued upload succeeds, rerun the MCP workflow without repeating Maven Central or GitHub publication.

This applies equally to nightly snapshots and `v*` tag releases. The tag workflow has
already published Maven artifacts and created the GitHub release before dispatching its
Author MCP job, so repair and rerun only that downstream job. See
[TPF Author MCP Operations](/evolve/author-mcp-operations) for the end-to-end ownership,
hook, worktree, verification, and recovery runbook.

## Guardrails

- Do not push release commits or tags until local validation passes.
- Keep Maven Central publishing tied to immutable tags.
- Keep the repository's active `Immutable release tags` ruleset on `refs/tags/v*`; it blocks updates and deletion after tag creation.
- Keep snapshot publication restricted to `main`; it is mutable and must never create a release tag or GitHub release.
- Keep the public docs domain on the `release-docs` branch alias; `main` is the docs staging source.
- Keep `release-docs` promotion inside the tagged release workflow and tied to its exact release commit.
- Keep alternate topology POMs, standalone POMs, and docs snapshots aligned with the release.
- Keep author MCP knowledge tied to the exact release tag commit or snapshot `main` commit; do not add Repowise indexing or model credentials to GitHub Actions.
