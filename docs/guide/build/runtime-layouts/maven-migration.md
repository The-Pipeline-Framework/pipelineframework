# Maven Migration Playbook

This page focuses on practical migration between build topologies.

## Strategy

Move in small, testable increments:

1. Keep current modular build green.
2. Introduce runtime mapping (`validation: auto`).
3. Change Maven modules to match target topology.
4. Tighten mapping with `validation: strict`.

## Modular -> Pipeline Runtime

Target shape:

- `orchestrator-svc` stays separate.
- Pipeline services collapse into one runtime module.
- Plugin side effects can stay grouped by aspect runtime.

Typical Maven changes:

- Create one service runtime module (for grouped pipeline steps).
- Move or aggregate service step sources/deps into that module.
- Remove per-step service modules from parent `<modules>`.
- Keep `orchestrator-svc` module as separate deployable.

In CSV Payments, this is implemented as:

- `examples/csv-payments/pom.pipeline-runtime.xml`
- `examples/csv-payments/pipeline-runtime-svc/pom.xml`
- `examples/csv-payments/build-pipeline-runtime.sh`

Typical runtime mapping changes:

- `layout: pipeline-runtime`
- Default regular steps to the grouped service module.
- Default synthetics to plugin module(s).

## Pipeline Runtime -> Monolith

Target shape:

- One runtime containing orchestrator, steps, and plugins.

Typical Maven changes:

- Create a monolith module containing orchestrator + service + plugin runtime deps.
- Build one runnable artifact from that module.
- Keep old modules only as migration scaffolding/test harness if needed.

Typical runtime mapping changes:

- `layout: monolith`
- `defaults.module` to monolith module.
- `defaults.synthetic.module` to same monolith module.

## Command style

Prefer explicit flags/scripts over profiles for clarity. Example pattern:

```bash
./examples/csv-payments/build-monolith.sh -DskipTests
./examples/csv-payments/build-pipeline-runtime.sh -DskipTests
./mvnw -f examples/csv-payments/pom.xml -pl orchestrator-svc \
  -Dcsv.runtime.layout=monolith -Dtest=CsvPaymentsEndToEndIT test
```

## Validation checklist after each migration step

- Build succeeds from clean checkout.
- Orchestrator E2E still passes.
- Runtime mapping warnings/errors are understood.
- Health checks reflect intended topology.
- CI runs the topology lane you intend to support.
