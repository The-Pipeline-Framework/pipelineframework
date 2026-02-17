# CSV Payments Monolith Walkthrough

This page documents the current monolith topology in `examples/csv-payments`.

## What exists today

- Monolith parent POM: `examples/csv-payments/pom.monolith.xml`
- Monolith runtime module: `examples/csv-payments/monolith-svc/pom.xml`
- Runtime mapping scenario: `examples/csv-payments/config/runtime-mapping/monolith.yaml`
- Build script: `examples/csv-payments/build-monolith.sh`

## Source layout clarification

- Service source code remains in service modules.
- `monolith-svc` aggregates those sources during the monolith build and packages the runnable runtime.
- `monolith-svc` also merges generated orchestrator client-step sources and pipeline metadata needed at runtime.

## Packaging details (important)

`monolith-svc` uses the same merge pattern as other csv-payments modules:

- `maven-resources-plugin` merges role outputs from `target/classes-pipeline/*` into `target/classes` before Quarkus packaging.
- Monolith additionally copies orchestrator pipeline metadata into `target/classes/META-INF/pipeline`:
  - `order.json`
  - `telemetry.json`
  - `orchestrator-clients.properties`

Without these metadata files, monolith startup can fail with errors such as:
`Pipeline order metadata not found. Ensure META-INF/pipeline/order.json is generated at build time.`

## Build the monolith runtime

```bash
./examples/csv-payments/build-monolith.sh -DskipTests
```

What the script does:

- Applies monolith runtime mapping.
- Builds `pom.monolith.xml`.
- Uses local transport for in-process step calls.
- Restores previous runtime mapping file after build.

## Run monolith E2E

```bash
./mvnw -f examples/csv-payments/pom.xml -pl orchestrator-svc \
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
