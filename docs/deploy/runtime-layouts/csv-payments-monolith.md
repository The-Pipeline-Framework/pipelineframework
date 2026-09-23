# CSV Payments Monolith Walkthrough

This page documents the current monolith topology in the standalone
[`csv-kafka-payments`](https://github.com/The-Pipeline-Framework/csv-kafka-payments) application. Paths and commands
below are relative to that repository's root.

## What exists today

- Root POM used by `build-monolith.sh`: `pom.xml`
- Monolith runtime module: `monolith-svc/pom.xml`
- Runtime mapping scenario: `config/runtime-mapping/monolith.yaml`
- Build script: `build-monolith.sh`

## Source layout clarification

- Service source code remains in service modules.
- `monolith-svc` aggregates those sources during the monolith build and packages the runnable runtime.
- `monolith-svc` also merges generated orchestrator client-step sources and pipeline metadata needed at runtime.

## Packaging details (important)

`monolith-svc` uses the same merge pattern as other csv-payments modules:

- `maven-resources-plugin` merges role outputs from `target/classes-pipeline/*` into `target/classes` before Quarkus packaging.
- Monolith additionally copies orchestrator metadata needed by the harness/runtime boundary into `target/classes/META-INF/pipeline`:
  - `telemetry.json`
  - `replay-topology.json`
  - `orchestrator-clients.properties`
- `order.json` must come from monolith generation for the active transport/layout. It should not be overwritten by orchestrator metadata.

Without these metadata files, monolith startup can fail with errors such as:
`Pipeline order metadata not found. Ensure META-INF/pipeline/order.json is generated at build time.`

## Build the monolith runtime

```bash
./build-monolith.sh -DskipTests
```

What the script does:

- Applies monolith runtime mapping.
- Installs `pom.xml` (`-N install`) so module parent descriptors are resolvable in clean local repositories (including CI jobs).
- Ensures development certificates exist for module-local test/runtime launches.
- Builds the monolith module selection from `pom.xml`.
- Uses local transport for in-process step calls.
- Restores previous runtime mapping file after build.

## Run monolith E2E

```bash
./mvnw -f pom.xml -pl orchestrator-svc \
  -Dcsv.runtime.layout=monolith -Dtest=CsvPaymentsEndToEndIT test
```

Important detail:

- The test class lives in `orchestrator-svc`.
- In monolith mode, it launches `monolith-svc` runnable JAR.
- `orchestrator-svc` is the harness; `monolith-svc` is the runtime under test.

## CI expectation

Monolith support is complete only when CI runs both:

1. Monolith build (`build-monolith.sh`)
2. Monolith E2E (`CsvPaymentsEndToEndIT` with `-Dcsv.runtime.layout=monolith`)

## Common failure signals

- `Expected executable jar at ../monolith-svc/target/quarkus-app/quarkus-run.jar`:
  monolith build not run before test.
- `Pipeline order metadata not found...`:
  monolith packaging did not include orchestrator pipeline metadata in `META-INF/pipeline`.
- Missing tables (for example `relation ... does not exist`):
  launched runtime is not creating schema in the test DB.
- gRPC connectivity errors in monolith mode:
  transport/layout mismatch (using network client steps where local is expected).
