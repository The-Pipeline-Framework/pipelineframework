# Runtime Layouts and Build Topologies

Runtime mapping and Maven topology solve different problems:

- Runtime mapping (`pipeline.runtime.yaml`) controls **logical placement** of orchestrator, steps, and synthetic/plugin side effects.
- Maven topology controls **physical deployables** (which JAR/container is produced).

Both must be aligned to get the runtime shape you want in production.

## Who this guide is for

Application developers using the normal onboarding path:

1. Design in Canvas.
2. Download scaffold.
3. Adjust runtime layout and build shape for your target environment.

## Start here

- [Using Runtime Mapping](/guide/build/runtime-layouts/using-runtime-mapping)
- [Maven Migration Playbook](/guide/build/runtime-layouts/maven-migration)
- [CSV Payments Monolith Walkthrough](/guide/build/runtime-layouts/csv-payments-monolith)

## One-line rule

If `layout: monolith` is configured but your Maven build is still modular, you get logical monolith placement, not a real monolith artifact.
