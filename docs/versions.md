# Versions

The Pipeline Framework documentation is available for the following versions:

## Latest Version

- [v26.5.1](/) - Current documentation

## Previous Versions

- [v26.5.1](/versions/v26.5.1/) - Snapshot of the v26.5.1 docs
- [v26.4.4](/versions/v26.4.4/) - Snapshot of the v26.4.4 docs
- [v26.4.3](/versions/v26.4.3/) - Snapshot of the v26.4.3 docs
- [v26.2](/versions/v26.2/) - Snapshot of the v26.2 docs
- [v0.9.2](/versions/v0.9.2/) - Snapshot of the v0.9.2 docs
- [v0.9.0](/versions/v0.9.0/) - Snapshot of the v0.9.0 docs

## About Versioning

The Pipeline Framework follows semantic versioning:
- **Major versions** (`x.0.0`) may introduce breaking changes.
- **Minor versions** (`x.y.0`) add features and improvements while preserving compatibility guarantees for the same major version.
- **Patch versions** (`x.y.z`) include fixes and incremental improvements.

## Documentation Snapshot Policy

This site keeps snapshots for released docs versions and points the latest docs to the root.
When cutting a new release, create a docs snapshot and update the version list:

```bash
cd docs
npm run snapshot -- --version vX.Y[.Z]
```
