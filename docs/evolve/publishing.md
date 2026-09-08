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

Publishing TPF has four related but separate responsibilities:

1. publish immutable Java framework releases to Maven Central,
2. publish the current framework `-SNAPSHOT` from `main` to Sonatype Central's snapshots repository,
3. snapshot the docs site for the released version,
4. promote the exact released commit to the public docs site,
5. publish versioned docs and release artifacts.

Use this page as the release front door. The older full procedure remains available as [Publishing Reference](/evolve/publishing-reference).

## Release Path

| Need | Page |
| --- | --- |
| Cut and publish framework artifacts | [Framework Release Process](/evolve/framework-release-process) |
| Consume or republish the current framework snapshot | [Publishing Reference — Nightly Snapshot Publishing](/evolve/publishing-reference#nightly-snapshot-publishing) |
| Validate docs snapshots and route rewrites | [Docs Snapshot Process](/evolve/docs-snapshot-process) |
| Publish or recover versioned docs artifacts | [Docs Snapshot Process](/evolve/docs-snapshot-process) |
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

## Guardrails

- Do not push release commits or tags until local validation passes.
- Keep Maven Central publishing tied to immutable tags.
- Keep the repository's active `Immutable release tags` ruleset on `refs/tags/v*`; it blocks updates and deletion after tag creation.
- Keep snapshot publication restricted to `main`; it is mutable and must never create a release tag or GitHub release.
- Keep the public docs domain on the `release-docs` branch alias; `main` is the docs staging source.
- Keep `release-docs` promotion inside the tagged release workflow and tied to its exact release commit.
- Keep alternate topology POMs, standalone POMs, and docs snapshots aligned with the release.
- Keep release promotion and docs snapshot responsibilities aligned with each exact release tag or snapshot commit; do not add non-release-only side effects to publish workflows.
