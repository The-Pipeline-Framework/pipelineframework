---
search: false
---

# Framework Release Process

This page captures the framework artifact release flow.

## Prepare Locally

Run Maven release preparation without pushing:

```bash
./mvnw release:prepare \
  -DpushChanges=false \
  -DreleaseVersion=26.5.2 \
  -DdevelopmentVersion=26.6.1-SNAPSHOT \
  -Dtag=v26.5.2 \
  -Darguments="-DskipTests"
```

## Validate Before Pushing

Run the release gate appropriate to the slice. At minimum:

- version drift checks,
- framework verification,
- CSV topology checks,
- docs build.

Also verify release-coupled standalone POMs, including alternate CSV topology POMs, checkout examples, and `ai-sdk`.

## Publish

After validation:

1. push the prepared release and next-development commits,
2. push the immutable release tag,
3. let GitHub Actions publish signed artifacts to Maven Central,
4. verify the artifacts on Sonatype Central,
5. create or verify the GitHub release.

For detailed Central plugin, GPG, and troubleshooting notes, see [Publishing Reference](/versions/v26.7.1/evolve/publishing-reference).
