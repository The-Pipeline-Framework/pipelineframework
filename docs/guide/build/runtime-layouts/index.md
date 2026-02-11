# Runtime Layouts and Build Topologies

This section is the application developer guide for shaping deployments after scaffold generation.
In the standard flow, you design in the Web UI Canvas Designer, download the scaffold,
then choose how many deployables you actually want to run.

## Three terms you must separate

### Runtime layout (logical)

Defined in `pipeline.runtime.yaml`.

- Decides where orchestrator, regular steps, and synthetic and plugin side effects are placed.
- Values include `modular`, `pipeline-runtime`, and `monolith`.
- Drives generated wiring and validation.

### Build topology (physical)

Defined by Maven modules and POM wiring.

- Decides what artifacts are produced (how many JARs/containers).
- Decides what is actually deployable in CI/prod.
- Is not rewritten automatically by runtime mapping.

### Transport mode (call mechanism)

Values include `GRPC`, `REST`, and `LOCAL`.

- Decides how steps are invoked.
- Orthogonal to layout/topology.

### Platform mode (deployment target)

Values include `COMPUTE` and `FUNCTION` (legacy aliases: `STANDARD`, `LAMBDA`).

- Decides whether generation targets standard Quarkus runtimes or AWS Lambda packaging/runtime semantics.
- Constrained by transport and step shapes (currently Function mode requires REST and unary-unary steps).
- Orthogonal to runtime layout/topology.

## Who this guide is for

Application developers using the normal onboarding path:

1. Design in Canvas.
2. Download scaffold.
3. Adjust runtime layout and build topology for your target environment.

## Why people get confused

You can set `layout: monolith` and still not have a real monolith artifact if the
Maven topology is still modular.

- Runtime mapping changed the logical placement.
- Maven still produced modular deployables.

Both layers must be aligned to achieve your intended runtime shape.

## What runtime mapping changes automatically

- Placement rules for regular and synthetic steps.
- Validation behavior ([`auto` = best-effort fallback placement, `strict` = fail on unmapped/mismatched placement](/guide/evolve/runtime-mapping/schema#validation)).
- Generated client/server wiring aligned to placement + transport.

## What runtime mapping does not change automatically

- Parent/module structure in Maven.
- Number of runtime artifacts produced.
- CI lanes needed to build/test a new topology.

::: note
Choosing `monolith` does not remove per-step service modules from the scaffold. `monolith-svc` acts as the runtime packaging module and aggregates service sources at build time.
:::

For Maven topology changes, use the migration playbook.

## Start here

- [POM vs Layout Matrix](/guide/build/runtime-layouts/pom-layout-matrix)
- [Using Runtime Mapping](/guide/build/runtime-layouts/using-runtime-mapping)
- [Maven Migration Playbook](/guide/build/runtime-layouts/maven-migration)
- [CSV Payments Pipeline-Runtime Walkthrough](/guide/build/runtime-layouts/csv-payments-pipeline-runtime)
- [CSV Payments Monolith Walkthrough](/guide/build/runtime-layouts/csv-payments-monolith)
- [Search Lambda Verification Lane](/guide/build/runtime-layouts/search-lambda)
