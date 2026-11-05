# Product release selections

Each `<product-version>.json` selects independently versioned stable components. The schema is
[`product-release.schema.json`](../system-tests/schemas/product-release.schema.json); executable ownership,
previous-release, availability and test-evidence checks live in `system-tests/scripts/product-release.py`.

Follow the [release process](../docs/evolve/framework-release-process.md). Only components marked `new` are
prepared and published. Marking a component `reuse` preserves its preceding published version and source SHA.
Dependency POMs and generated/publication resources count as release inputs, not just Java source files.

`26.10.1.json` is the initial historical component selection. Its harness pins record the revisions available
when the manifest was introduced. It is not retrospective full-train evidence and has no `validationRunId`.
Future product manifests name their previous product release and must acquire successful selected-release
full-train evidence before BOM publication. Published selections are immutable alongside their release tags.

These manifests describe framework/product publication, not authored pipeline Release Descriptors.
