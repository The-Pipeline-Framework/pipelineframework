# Deployment and packaging

Read this reference only for runtime placement, transport/platform, build topology,
generated artifacts, application bootstrap/package, local/remote constraints, or
topology tests. Verify the chosen release/runtime against current docs/source/examples.

## Put configuration at the right lifetime

Use `docs/develop/configuration/all-settings.md` as the current search map, not as a
property list to memorize. Classify the setting before choosing YAML, a build option,
`application.properties`, an environment variable, or typed input:

| Lifetime/owner | Examples | Authoring rule |
| --- | --- | --- |
| portable pipeline semantics | canonical types, steps, cardinality, branching, Query/Command/Await intent, connector operation choice | author in `pipeline.yaml`; do not hide in runtime properties |
| build-time generation | transport/platform generation inputs, Java bindings, generated REST/client paths, typed telemetry metadata, provider availability to the processor | changing it requires regeneration/rebuild; runtime config cannot create missing code or metadata |
| runtime framework policy | concurrency, backpressure, retry, circuit admission, cache, persistence/materialization provider choice, background execution, Await adapter plumbing, health | select/tune the framework shell; do not inject into business steps |
| deployment/provider wiring | endpoints, brokers, buckets/tables, credentials/secret references, exporters and backends | keep outside portable contracts and supply through the current runtime/deployment mechanism |
| invocation business data | customer/order/provider request facts that vary per item | carry as typed pipeline input, not connector or application configuration |

The same subject can span lifetimes. For example, an Await step declares its semantic
transport/boundary in YAML while runtime properties enable and tune the concrete broker
adapter. Materialization policy declares that a value is stored out of line; runtime
configuration selects and tunes its repository. Neither changes what the authored
service means.

Prefer framework/global defaults. Add a per-step or exact generated-boundary override
only for a real operational exception. Use the boundary identity emitted by generated
diagnostics; do not guess names or build an application override registry. Check the
current precedence rules in the settings page and the matching loader/renderer tests.

### Telemetry is also split by lifetime

Typed boundary information and the instrumentation available to a deployable are
compiled/generated. A runtime toggle cannot add a signal or dependency that was absent
at build/augmentation time. Runtime/deployment configuration owns enablement, sampling,
exporters, files/backends, and SLO/operational thresholds.

Keep pipeline lifecycle telemetry in generated/framework instrumentation. Add explicit
business metrics only when the application owns a genuine domain measure; do not make
every service recreate step spans, replay events, correlation, or transport metrics.
When telemetry behavior changes, inspect generated telemetry metadata, runtime telemetry
tests, and the current observability docs as well as `all-settings.md`.

## Separate the deployment questions

| Question | TPF surface |
| --- | --- |
| Where do orchestrator roles, steps, and synthetics logically run? | runtime layout / `pipeline.runtime.yaml` |
| Which JARs or containers physically exist? | Maven/build topology |
| How do generated boundaries call each other? | `GRPC`, `REST`, or `LOCAL` transport |
| What target hosts the runtime? | platform/deployment configuration |
| How are values encoded across the call? | generated wire/envelope protocol |

Do not treat `FUNCTION`, provider names, or HTTP envelope protocols as transport values.
Placement changes generated calls; it does not change canonical meaning. Runtime layout
does not rewrite POMs, aggregate source, or choose artifact count.

## Start with one supported unit when it fits

Choose the simplest supported build/deployment shape first. A matching monolith layout
and build topology can package orchestrator, services, and plugins into one runnable
unit. Current repository examples may still use a dedicated `monolith-svc` module and
source aggregation to produce that unit; those mechanics are not authoring doctrine.

Split deployables only for an actual reason: independent scaling, security/network
isolation, failure containment, provider/runtime constraints, team ownership, or a
separately operated control plane/worker. Do not begin with one module per step because
an old scaffold did so. Conversely, setting `layout: monolith` alone does not turn a
modular Maven build into one artifact.

If changing topology, align in this order:

1. preserve the typed pipeline and semantic boundary;
2. choose logical placement for regular and synthetic steps;
3. make the build topology produce the intended units;
4. choose transport/platform for real boundaries;
5. compile and test the packaged topology.

Local versus remote support differs by step kind, cardinality, generated descriptors,
platform, and runtime. Do not encode a remembered support matrix here. Compile the
actual topology and inspect the current renderer/binding tests before adding a manual
HTTP/gRPC client or changing a type to make a remote path work.

## Let the compiler and package own generated boundaries

The application supplies the canonical YAML/types, smallest authored Java, genuine
mappers/provider code, runtime config, dependencies, and a build topology. The compiler
and packaging lifecycle generate and assemble supported adapters, clients/servers,
role sources, transport contracts, descriptors, and `META-INF/pipeline/` metadata.

Inspect generated sources and diagnostics before writing glue. Relevant contract
artifacts currently include ordered step/semantic, branching, telemetry, platform, and
provider metadata; exact filenames and formats are implementation details. Handwritten
copies drift from the compiler model.

For build/bootstrap work, inspect the current smallest representative application and
the pipeline POM lifecycle. Check:

- the framework API/runtime and annotation-processor dependencies needed by that shape;
- provider dependencies on both runtime and processor classpaths when generation needs them;
- generated-source/resource roots and Quarkus/Spring bootstrap for the chosen runtime;
- application/runtime properties and connector secret/connection references;
- package output and the artifact actually started in tests/production.

Do not copy the retired app-generator's parent POM, orchestrator host, certificate,
per-step module, persistence-service, or Docker layout blindly. Current compiler output
and current examples decide what remains necessary.

## Validate proportionally

Unit-test authored transformations and mappers. Run a focused compilation of the real
YAML/build module to exercise discovery, cardinality, mappings, provider capabilities,
and generated artifacts. Add a packaged integration test for the deployment shape and
remote boundary actually used. A monolith should prove local/generated calls; a split
layout should prove its real transport and descriptors. Do not require every application
to reproduce the repository's full topology matrix.

Search order: `docs/deploy/runtime-layouts/`, current configuration docs, pipeline POM
lifecycle, the smallest relevant example POM/YAML/runtime mapping, compiler generation
phases and metadata tests, then runtime-specific smoke/package tests.
