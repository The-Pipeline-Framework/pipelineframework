# Builds and Continuous Integration

This repository coordinates released TPF components. It publishes the tested component BOM and runs the
cross-repository compatibility tests; implementation builds belong to their standalone repositories.

## Workflows

1. **build.yml** — pull-request validation
   - builds the canonical two-module coordination reactor;
   - runs the transport compatibility unit tests;
   - verifies a fresh checkout against released snapshot artifacts.
2. **full-tests.yml** — `main` validation
   - installs the coordination parent and BOM;
   - runs the complete compatibility-test module.
3. **publish-snapshots.yml** — snapshot publication
   - verifies every BOM-managed coordinate against configured snapshot repositories;
   - publishes only artifacts declared public in `framework/public-artifacts.json`.
4. **publish.yml** — tagged Central publication
   - performs the same publication-contract verification;
   - signs and publishes the coordination parent and product BOM.
5. **docs.yml** — documentation build and link validation.

## Local gates

Use the worktree-local Maven repository on every invocation:

```bash
./mvnw verify -Dmaven.repo.local="$PWD/.m2/repository"
./mvnw -f framework/pom.xml verify -Dmaven.repo.local="$PWD/.m2/repository"
node scripts/verify-framework-publication.mjs "$PWD/.m2/repository"
```

`central-publishing` is the only Maven profile. It attaches and signs publishable artifacts; it does not select a
different source universe, module graph, or build topology.
