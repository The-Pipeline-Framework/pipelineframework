# Versions

Use the current documentation for the latest release, or open a frozen snapshot for an older release.

## Current Release

- [v26.9.3](/) - Current released documentation

## Recent Release Notes

The v26.9.x releases do not have frozen documentation snapshots. Their release notes remain available:

- [v26.9.3](https://github.com/The-Pipeline-Framework/pipelineframework/releases/tag/v26.9.3)
- [v26.9.2](https://github.com/The-Pipeline-Framework/pipelineframework/releases/tag/v26.9.2)
- [v26.9.1](https://github.com/The-Pipeline-Framework/pipelineframework/releases/tag/v26.9.1)

## Frozen Documentation Snapshots

- [v26.8.1](/versions/v26.8.1/) - Snapshot of the v26.8.1 docs
- [v26.7.1](/versions/v26.7.1/) - Snapshot of the v26.7.1 docs
- [v26.6.2](/versions/v26.6.2/) - Snapshot of the v26.6.2 docs
- [v26.6.1](/versions/v26.6.1/) - Snapshot of the v26.6.1 docs
- [v26.5.2](/versions/v26.5.2/) - Snapshot of the v26.5.2 docs
- [v26.5.1](/versions/v26.5.1/) - Snapshot of the v26.5.1 docs
- [v26.4.4](/versions/v26.4.4/) - Snapshot of the v26.4.4 docs
- [v26.2](/versions/v26.2/) - Snapshot of the v26.2 docs
- [v0.9.2](/versions/v0.9.2/) - Snapshot of the v0.9.2 docs
- [v0.9.0](/versions/v0.9.0/) - Snapshot of the v0.9.0 docs

## About Versioning

Pin the exact TPF release across framework, compiler, Connector, Block, and plugin dependencies.
Do not infer compatibility guarantees from a version-number segment alone; use the release notes and
matching version snapshot for the contract shipped by that release.

```mermaid
flowchart LR
    R[Released artefacts] --> N[Release notes]
    R --> S[Frozen docs snapshot]
    M[Current main documentation] --> F[Next released snapshot]
```

## Documentation Snapshot Policy

This site points the current release documentation to the root. When cutting a release, create its
frozen snapshot and update the current version entry:

```bash
cd docs
npm run snapshot -- --version vX.Y[.Z]
```
