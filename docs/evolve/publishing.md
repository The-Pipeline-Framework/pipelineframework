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

Publishing TPF has three related but separate responsibilities:

1. publish immutable Java framework releases to Maven Central,
2. publish the current framework `-SNAPSHOT` from `main` to Sonatype Central's snapshots repository,
3. snapshot the docs site for the released version.

Use this page as the release front door. The older full procedure remains available as [Publishing Reference](/evolve/publishing-reference).

## Release Path

| Need | Page |
| --- | --- |
| Cut and publish framework artifacts | [Framework Release Process](/evolve/framework-release-process) |
| Consume or republish the current framework snapshot | [Publishing Reference — Nightly Snapshot Publishing](/evolve/publishing-reference#nightly-snapshot-publishing) |
| Validate docs snapshots and route rewrites | [Docs Snapshot Process](/evolve/docs-snapshot-process) |
| Troubleshoot Maven Central details | [Publishing Reference](/evolve/publishing-reference) |

## Guardrails

- Do not push release commits or tags until local validation passes.
- Keep Maven Central publishing tied to immutable tags.
- Keep snapshot publication restricted to `main`; it is mutable and must never create a release tag or GitHub release.
- Keep alternate topology POMs, standalone POMs, and docs snapshots aligned with the release.
