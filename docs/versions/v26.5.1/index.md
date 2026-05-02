---
layout: home

hero:
  name: The Pipeline Framework
  text: Fast Typed Function Chains on Quarkus
  tagline: Write focused Java business functions; TPF generates, validates, and runs the Quarkus code around them
  image:
    src: /logo.png
    alt: The Pipeline Framework
  actions:
    - theme: alt
      text: Quick Start
      link: /guide/getting-started/
    - theme: brand
      text: Design with Canvas
      link: https://app.pipelineframework.org

features:
  - title: Developer Joy
    details: Start quickly, keep function inputs and outputs explicit, and spend less time writing glue code
    link: /value/developer-experience
  - title: Performance
    details: Handle more work with less runtime overhead, using generated code paths and non-blocking execution
    link: /value/runtime-efficiency
  - title: Transport Choice
    details: Generate gRPC, REST, and local clients from one model, and evolve how you expose the pipeline
    link: /value/integration-flexibility
  - title: Start Monolith, Split Later
    details: Start as a monolith, then split into layouts that fit your team boundaries when you are ready
    link: /value/deployment-evolution
  - title: Kube-Native
    details: Container-first delivery with health, observability, retries, and failure handling that stay understandable
    link: /value/operational-confidence
  - title: Plugins, Not Glue
    details: Add persistence, cache, telemetry, and logging without pushing that code into every business function
    link: /value/extensibility-and-platform
search: false
---

<Callout type="tip" title="Visual Pipeline Designer Available">
The Pipeline Framework includes a visual canvas designer at <a href="https://app.pipelineframework.org" target="_blank" rel="noopener noreferrer">https://app.pipelineframework.org</a>. Design the flow visually, click "Download Application", and you get a runnable Quarkus scaffold with generated code for the pipeline shape you designed.
</Callout>

<FeaturedArticles />
<LatestReleases />

## Key Features

The Pipeline Framework helps you build fast, forward-only flows of Java functions. A step is one function in the flow: it receives a typed input, produces a typed output, and lets TPF generate the REST, gRPC, local, or function-style code around it.

### Runtime and Processing Model

- **Reactive by default**: Built on Mutiny so many items can move through the flow without blocking one thread per item.
- **Why it matters**: Better CPU utilization and higher compute density typically mean lower runtime cost for the same workload.
- **Quarkus foundation**: Uses Quarkus build-time generation and runtime patterns for production-grade services.
- **Traceable flow**: Preserve enough context to understand what produced each item and where it came from.
- **Clear step shapes**: Supports one-to-one, one-to-many, many-to-one, many-to-many, and side-effect steps.

### Extensibility with Plugins

- **Plugin-friendly model**: Pipeline plugins add cross-cutting work without coupling business logic to infrastructure code.
- **Why it matters**: You can add cache, telemetry, persistence, and logging with less code churn in core services.
- **Clear extension points**: Use [Using Plugins](/versions/v26.5.1/guide/development/using-plugins) and the [Aspect guide](/versions/v26.5.1/guide/evolve/aspects/semantics) as primary references.

### Integration and Deployment Flexibility

- **Transport coverage**: Automatic generation for gRPC, REST, and local in-process calls.
- **Quarkus Dev Services workflow**: Smooth local development with managed service dependencies (for example, PostgreSQL) in dev/test flows.
- **Why it matters**: Teams can adapt interfaces to consumers and platform constraints without rewriting core processing logic.
- **Deployment layouts**: Works across modular, grouped pipeline-runtime, and monolith layouts.
- **Evolution path**: Start with a monolith for speed, then break into services as scale and ownership boundaries grow.

### Operations

- **Built-in observability**: Metrics, tracing, and structured logging are generated into the runtime shape.
- **Resilience features**: Health checks, retries, crash-surviving background execution, and dead-letter handling.
- **Container-first packaging**: Current delivery flow supports image builds with Jib for predictable CI/CD integration.
- **Native compilation path**: Quarkus native builds support low cold-start and efficient resource usage where needed.
- **Why it matters**: Teams get faster incident diagnosis, more predictable deployments, and lower operational overhead at scale.

### Developer Experience

- **Shared `common` module**: Central place for entity and DTO definitions used across pipeline components.
- **Type safety across the pipeline**: Compile-time checks catch mismatched step input/output types early.
- **Why it matters**: Teams get safer refactors, fewer mapping mistakes, and faster confidence when changing domain models.
- **Testability gains**: Functional boundaries and generated transport layers make behaviour easier to isolate and test.
- **Fast onboarding and testing**: Design visually with Canvas and validate behaviour via [Testing with Testcontainers](/versions/v26.5.1/guide/development/testing).
