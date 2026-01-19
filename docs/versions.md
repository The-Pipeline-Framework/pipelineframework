# Versions

The Pipeline Framework documentation is available for the following versions:

## Latest Version

- [v0.9.2](/) - Latest stable release

## Previous Versions

- [v0.9.2](/versions/v0.9.2/) - Snapshot of the v0.9.2 docs
- [v0.9.0](/versions/v0.9.0/) - Snapshot of the v0.9.0 docs

## About Versioning

The Pipeline Framework follows semantic versioning:
- **Major versions** (0.x.0) may introduce breaking changes
- **Minor versions** (0.9.x) add new features while maintaining backward compatibility
- **Patch versions** (0.9.1) include bug fixes and minor improvements

For production use, we recommend using the latest stable version (v0.9.2).

## Documentation Snapshot Policy

This site keeps snapshots for major/minor releases and points the latest docs to the root.
When cutting a new release, create a docs snapshot and update the version list:

```bash
cd docs
npm run snapshot -- --version v0.9.3
```
