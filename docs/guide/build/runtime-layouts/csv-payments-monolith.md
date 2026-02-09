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
- Missing tables (for example `relation ... does not exist`):
  launched runtime is not creating schema in the test DB.
- gRPC connectivity errors in monolith mode:
  transport/layout mismatch (using network client steps where local is expected).
