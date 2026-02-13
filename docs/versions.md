# Versions

The Pipeline Framework documentation is available for the following versions:

## Latest Version

- [v26.2](/) - Current documentation

## Previous Versions

- [v0.9.2](/versions/v0.9.2/) - Snapshot of the v0.9.2 docs
- [v0.9.0](/versions/v0.9.0/) - Snapshot of the v0.9.0 docs

## About Versioning

The Pipeline Framework follows semantic versioning:
- **Major versions** (`x.0.0`) may introduce breaking changes.
- **Minor versions** (`x.y.0`) add features while maintaining compatibility expectations for that major stream.
- **Patch versions** (`x.y.z`) include fixes and incremental improvements.

## Documentation Snapshot Policy

This site keeps snapshots for major/minor releases and points the latest docs to the root.
When cutting a new release, create a docs snapshot and update the version list:

```bash
cd docs
npm run snapshot -- --version v26.3
```
