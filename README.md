# The Pipeline Framework

[![Maven Central](https://img.shields.io/maven-central/v/org.pipelineframework/pipelineframework.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22org.pipelineframework%22%20AND%20a:%22pipelineframework%22)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Java 21+](https://img.shields.io/badge/Java-21+-brightgreen.svg)](https://adoptium.net/)
[![Quarkus](https://img.shields.io/badge/Quarkus-3.33.1-orange)](https://quarkus.io)
[![CodeRabbit](https://img.shields.io/coderabbit/prs/github/The-Pipeline-Framework/pipelineframework?label=CodeRabbit&color=purple)](https://coderabbit.ai)

The Pipeline Framework (TPF) is a Java framework on Quarkus for building reliable applications from fast, forward-only chains of business functions. You write focused Java code such as "validate this payment", "enrich this record", "parse this document", or "call this existing operator"; TPF turns that code into a runnable Quarkus application with generated REST, gRPC, local, and function-style entry points.

TPF is for teams that want clean function-style business logic without hand-building the same transport, retry, failure, observability, and deployment glue in every service. The business flow stays portable Java and YAML; TPF generates the repeated Quarkus code, checks the flow at build time, and runs the generated pipeline runtime reliably.

## What TPF Means by Pipeline

In TPF, a pipeline is not a CI/CD or batch pipeline made of coarse, long-running jobs. It is a low-latency, high-throughput flow of small Java functions.

- A **step** is one function in that flow.
- A **typed** step has explicit Java input and output types.
- A **reactive** flow can keep many items moving without blocking one thread per item.
- The flow moves forward through declared functions with explicit boundaries.
- The shape is simpler than an arbitrary graph because order, progress, and handoff points stay visible.
- TPF generates, validates, and runs the Quarkus code around that flow.

## Key Terms in Plain English

- **Generated adapter**: TPF-created REST, gRPC, local, or function-style code that calls your business function.
- **Operator**: an existing Java method or remote endpoint reused as a pipeline step.
- **Mapper**: code that translates between your domain types and transport or external-system types.
- **Durable execution**: accepted work is recorded outside the current JVM or process, so it can survive crashes and restarts.
- **`QUEUE_ASYNC`**: the config value for background execution where TPF stores execution state, dispatches work, retries failed work, recovers after worker crashes, and sends terminal failures to a dead-letter channel.
- **DLQ**: a dead-letter queue or channel for failed executions that need investigation or replay.
- **Idempotency**: stable keys that let retries avoid duplicating business effects.
- **Lineage**: enough tracking information to know where an item came from and which step produced it.
- **Runtime layout**: where TPF logically places the orchestrator, steps, and plugin side effects.
- **Build topology**: the Maven modules, JARs, and containers that physically create deployables.
- **Transport mode**: how generated components call each other: gRPC, REST, or local in-process calls.
- **Platform mode**: whether the app runs as a normal Quarkus service or through function-style entry points.

## How TPF Splits Responsibilities

TPF splits responsibility between your application code and the framework runtime.

- **You define the business flow**: typed functions, input/output contracts, operators, mappers, and domain decisions such as "validate this payment", "reject this bad record", "index this document", or "handoff this checkout checkpoint".
- **TPF generates the repeated code**: REST endpoints, gRPC services, local clients, function-style handlers, client calls, and runtime descriptions that would otherwise become hand-written service glue.
- **TPF runs the generated runtime**: it starts the flow, calls each step, records progress when configured, retries failed work, recovers leased work after crashes, and sends terminal failures to the configured failure channel.
- **You choose the runtime shape**: `modular`, `pipeline-runtime`, or `monolith`, plus the Maven and container topology that physically builds the deployables.
- **TPF validates the flow before startup**: function shape, mapper compatibility, operator references, transport requirements, and generated runtime files are checked during the build.
- **You own production policy**: provider selection, idempotency choices, retry budgets, DLQ handling, observability thresholds, and deployment rollout.

The result is that application code stays focused on business behaviour while the framework handles the repeatable work of moving items through the pipeline reliably.

## Why TPF

- **One flow, multiple ways to call it**: Generate gRPC, REST, local, and function-style entry points from the same ordered function chain.
- **Build-time safety**: Catch mismatched operators, mappers, input/output types, and generated call paths before the application starts.
- **Operator reuse**: Reuse a local Java `Class::method` operator or remote IDL v2 operator without hiding it behind ad-hoc service glue.
- **Layout flexibility**: Run the same pipeline in `modular`, `pipeline-runtime`, or `monolith` layouts as your team and deployment model evolve.
- **Platform flexibility**: Run as a normal Quarkus service or through function-style entry points without changing the business functions.
- **Operational readiness**: Built-in health checks, tracing/metrics/logging hooks, crash-surviving background execution, retries, and dead-letter handling.
- **Plugin extensibility**: Add persistence, caching, telemetry, and other cross-cutting work declaratively instead of repeating side-effect code in every function.

## Core Capabilities

### Model-driven pipeline generation

TPF is YAML-driven first. You declare the function chain and write the business functions; TPF generates the Quarkus code that exposes, calls, validates, and runs them.

- Pipeline order is written at build time to `META-INF/pipeline/order.json`.
- Telemetry descriptions are written at build time to `META-INF/pipeline/telemetry.json`.
- Build-time generation makes the generated call paths visible instead of spreading them through hand-written glue code.
- In a payments flow, this means the framework can connect validation, enrichment, status creation, and rejection handling without each function owning its own transport layer.

### Operators and external delegation

Operators let a pipeline step call reusable code while the pipeline still makes the boundary explicit.

- Local Java operators use `operator: fully.qualified.Class::method` and are resolved at build time.
- Remote IDL v2 operators use generated adapters and explicit runtime targets for code that lives outside the Java build.
- Mapper and transport checks keep delegated operator boundaries explicit instead of relying on implicit conversion.
- For AI enrichment, an existing embedding, vector search, or LLM helper library can become part of the pipeline without rewriting it as a full service first.

### Reducing hidden coupling

Coupling is where function chains often decay: one function starts depending on another function's internal class, HTTP shape, deployment unit, side effect, retry assumption, or failure channel. TPF does not pretend coupling disappears; it makes the important coupling explicit and moves repeatable coupling into generated code or framework runtime behaviour.

- **Data coupling**: explicit input/output types and mappers keep each function from importing another function's private classes.
- **Transport coupling**: generated REST, gRPC, local, and function-style adapters keep business functions independent from how they are called.
- **Deployment coupling**: runtime layout and build topology are separate decisions, so the business flow is not locked to one deployable shape.
- **Cross-cutting coupling**: plugins/aspects keep persistence, cache, telemetry, and logging out of business functions.
- **Lineage and idempotency coupling**: TPF tracks item identity, previous-item references, payload version, and idempotency keys so retries and investigation have stable context.
- **Application state coupling**: examples such as CSV Payments carry prior context forward explicitly using a "Russian dolls" style domain model, instead of reaching sideways into earlier functions or shared mutable state.
- **Failure coupling**: Item Reject Sink handles per-item business rejection, while an execution DLQ handles full-run failures that need operator attention.

### Runtime layouts and deployment evolution

TPF separates where the flow logically runs from how the deployable files are built.

- **Runtime layout** decides the logical placement of orchestrators, business functions, and plugin side effects.
- **Build topology** decides which Maven modules, JARs, and containers are actually produced.
- **Transport mode** (`GRPC`, `REST`, `LOCAL`) decides how generated clients call pipeline functions.
- **Platform mode** (`COMPUTE`, `FUNCTION`) decides whether the generated runtime targets a standard service or a function-style entry point model.

That separation lets teams start with a monolith or grouped runtime, then move toward more distributed layouts when ownership, throughput, or deployment constraints justify it.

### Plugins, aspects, and side effects

Cross-cutting capabilities are configured declaratively through aspects and implemented by plugins.

- You declare concerns such as persistence, cache, telemetry, or logging around the pipeline.
- TPF generates and runs the transport-aware side-effect integration for gRPC, REST, and local execution paths.
- Business functions stay focused on domain transformations instead of repeating infrastructure code.

### Background execution and checkpoint-driven flows

TPF supports normal request/response execution and background execution where callers submit work, receive an execution ID, and let TPF continue the flow after the request returns.

- Background execution can store accepted work outside the current JVM so crashes and restarts do not lose that work.
- Checkpoint-style handoff patterns for cross-pipeline orchestration, as demonstrated by the TPFGo example.
- Lineage and replay-safe state handling across split/merge flows.
- In a checkout flow, one pipeline can publish a stable checkpoint and the next pipeline can accept it through framework-owned handoff endpoints.

### High availability and crash-surviving execution

TPF's crash-surviving execution path is configured with `pipeline.orchestrator.mode=QUEUE_ASYNC`.

- TPF stores execution progress, dispatches work, retries failed transitions, recovers work after worker crashes, and publishes terminal failures.
- Execution state can be backed by providers outside the current process instead of process-local memory.
- Work dispatch can use queue-backed providers for recovery and worker takeover.
- Terminal execution failures can be routed to an execution DLQ, while item-level failures can use Item Reject Sink.
- Your team still chooses the production policy: providers, idempotency rules, retry budgets, observability thresholds, and rollout strategy.

### Function and cloud deployment

TPF supports function-style deployment paths in addition to standard Quarkus service runtimes.

- `FUNCTION` and `COMPUTE` are first-class platform modes.
- Function-oriented flows use the same business functions and generated runtime rules rather than becoming a separate programming model.
- Multi-cloud function support is part of the current platform story, including AWS Lambda, Azure Functions, and Google Cloud Functions guidance.

## Architecture Model

At a high level, TPF works like this:

1. Define the pipeline, types, and business logic.
2. Configure runtime mapping, transport, platform, and optional aspects/plugins.
3. Compile the application so TPF validates the model and generates endpoints, clients, handlers, and runtime files.
4. Run the application so TPF calls the generated code, records progress when configured, retries failures, and routes failed work.
5. Operate the deployables with the production policies and topology your environment requires.

This keeps the domain flow stable while generated endpoints, clients, handlers, and deployable shapes can change around it.

## Getting Started

### Design flow

The fastest way to start is the Canvas designer:

1. Open [app.pipelineframework.org](https://app.pipelineframework.org).
2. Design the pipeline visually.
3. Download the generated application scaffold.
4. Build and run it with Quarkus and Maven.

Canvas is the preferred onboarding path, but TPF also supports template- and YAML-driven flows for automation, CI, and deeper framework usage.

### Core docs

- [Documentation home](https://pipelineframework.org)
- [Getting started](https://pipelineframework.org/guide/getting-started/)
- [Runtime layouts and build topologies](https://pipelineframework.org/guide/build/runtime-layouts/)
- [Using plugins](https://pipelineframework.org/guide/development/using-plugins)
- [Orchestrator runtime](https://pipelineframework.org/guide/development/orchestrator-runtime)
- [Testing](https://pipelineframework.org/guide/development/testing)
- [Observability](https://pipelineframework.org/guide/operations/observability/)
- [Operators](https://pipelineframework.org/guide/development/operators)
- [Operator operations](https://pipelineframework.org/guide/operations/operators)
- [Error handling and DLQ](https://pipelineframework.org/guide/operations/error-handling)
- [TPFGo example](https://pipelineframework.org/guide/development/tpfgo-example)

## Reference Examples

- [`examples/csv-payments`](examples/csv-payments/) shows the topology and runtime-layout story across modular, pipeline-runtime, and monolith builds.
- [`examples/search`](examples/search/) is the richer reference application for crawl/parse/tokenize/index flows, cache/persistence interplay, function-platform verification, and integration hardening.
- [`examples/checkout`](examples/checkout/) contains the TPFGo reference flow for checkpoint-boundary handoff and multi-pipeline orchestration.

## Project Surfaces

- [`framework/runtime`](framework/runtime/) contains runtime APIs, execution, telemetry, and config loading.
- [`framework/deployment`](framework/deployment/) contains annotation processing, validation, and code generation phases.
- [`plugins`](plugins/) contains foundational cross-cutting capabilities such as persistence and cache.
- [`docs`](docs/) contains the VitePress documentation site.
- [`web-ui`](web-ui/) contains the Canvas/web UI.
- [`template-generator-node`](template-generator-node/) contains template-generation and schema support.
- [`ai-sdk`](ai-sdk/) contains the standalone Java SDK used for AI/delegation and transport exercises.

## Build and Validation

Framework verification:

```bash
./mvnw -f framework/pom.xml verify
```

Full repository verification:

```bash
./mvnw verify
```

Docs build:

```bash
npm --prefix docs run build
```

## Security

If you discover a security vulnerability, see our [security policy](SECURITY.md) for responsible disclosure details.

## Contributing

Contributions are welcome across framework code, examples, docs, tooling, and design discussion.

- Read [CONTRIBUTING.md](CONTRIBUTING.md) to get started.
- Use [AGENTS.md](AGENTS.md) for repo-specific engineering guidance when working inside this repository.
