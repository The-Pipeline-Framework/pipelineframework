# Testing Guidelines

The coordination repository contains one non-public test module:
`framework/transport-completeness-tests`.

Its tests prove that the BOM-selected compiler, runtime, contract, and Connector artifacts interoperate when
consumed as released dependencies. They must not rebuild implementation source from another repository.

## Conventions

- Unit and compatibility tests use the `*Test` suffix and run with Surefire.
- Container, native-image, Quarkus integration, Spring smoke, example, and application tests belong to their
  owning standalone repositories.
- Test selection must not introduce Maven profiles or alternate reactor topologies.
- `central-publishing` is the sole profile and is publication-only.

## Commands

```bash
./mvnw -f framework/pom.xml verify \
  -Dmaven.repo.local="$PWD/.m2/repository"

./mvnw verify \
  -Dmaven.repo.local="$PWD/.m2/repository"
```

The publication verifier additionally resolves every exact coordinate managed by `pipelineframework-bom` from a
fresh local Maven cache. This prevents previously installed artifacts from masking an unpublished component.
