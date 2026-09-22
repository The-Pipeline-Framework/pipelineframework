# Coordination Repository Testing

The `pipelineframework` repository no longer owns compiler, runtime, Connector, Block, Expansion, example, or
application implementation source. Testing here is intentionally narrow: it proves that independently released
artifacts compose into the tested product set published by `pipelineframework-bom`.

## Compatibility module

`framework/transport-completeness-tests` consumes released artifacts selected by the product BOM. Its current
fixtures verify contributed protocol values across serialization/runtime boundaries and exercise compiler transport
generation with real Connector-owned types.

Tests use the `*Test` suffix and run with Maven Surefire. Runtime integration, native-image, container, Spring smoke,
example, and application tests remain with the repositories that own those behaviours.

## Validation

```bash
./mvnw -f framework/pom.xml verify \
  -Dmaven.repo.local="$PWD/.m2/repository"
```

The root reactor provides the equivalent aggregate gate:

```bash
./mvnw verify \
  -Dmaven.repo.local="$PWD/.m2/repository"
```

No test-selection or coverage Maven profile is permitted. `central-publishing` is the sole profile and may only
attach, sign, and deploy publication-eligible artifacts without changing the reactor or source universe.
