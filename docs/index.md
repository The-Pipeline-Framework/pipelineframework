---
layout: home

hero:
  name: The Pipeline Framework
  text: Reactive Pipeline Processing
  tagline: Build pipeline-style applications on Quarkus, with generated adapters and a shared type-safe core
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
    details: Start quickly, keep types in one place, and spend less time wiring things together
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
    details: Container-first delivery and cloud-friendly runtime foundations, with operational patterns that scale
    link: /value/operational-confidence
  - title: Plugins, Not Glue
    details: Add cross-cutting capabilities via plugins, while keeping domain code focused on behavior
    link: /value/extensibility-and-platform
---

<Callout type="tip" title="Visual Pipeline Designer Available">
The Pipeline Framework includes a visual canvas designer at <a href="https://app.pipelineframework.org" target="_blank">https://app.pipelineframework.org</a> that allows you to create and configure your pipelines using an intuitive drag-and-drop interface. Simply design your pipeline visually, click "Download Application", and you'll get a complete ZIP file with all the generated source code - no command-line tools needed!
</Callout>

<LatestReleases />

## Key Features

The Pipeline Framework is a powerful tool for building reactive pipeline processing systems. It simplifies the development of distributed systems by providing a consistent way to create, configure, and deploy pipeline steps.

### Runtime and Processing Model

- **Reactive by default**: Built on Mutiny for non-blocking throughput and backpressure-aware execution.
- **Why it matters**: Better CPU utilization and higher compute density typically mean lower runtime cost for the same workload.
- **Quarkus runtime foundation**: Aligns with Quarkus runtime and build-time patterns for fast, production-grade services.
- **Immutable event flow**: No in-place updates during execution; append/preserve semantics improve traceability and safety.
- **Rich step semantics**: Supports OneToOne, OneToMany, ManyToOne, ManyToMany, and SideEffect patterns.

### Extensibility with Plugins

- **Plugin-friendly model**: Pipeline plugins add cross-cutting capabilities without coupling business logic to infrastructure code.
- **Why it matters**: You can deliver platform concerns (cache, telemetry, orchestration behaviors) with less code churn in core services.
- **Clear extension points**: Use [Using Plugins](/guide/development/using-plugins) and [Aspect Semantics](/guide/evolve/aspects/semantics) as primary references.

### Integration and Deployment Flexibility

- **Transport coverage**: Automatic generation for gRPC, REST, and local/in-process clients.
- **Quarkus Dev Services workflow**: Smooth local development with managed service dependencies (for example, PostgreSQL) in dev/test flows.
- **Why it matters**: Teams can adapt interfaces to consumers and platform constraints without rewriting core processing logic.
- **Deployment layouts**: Works across modular, pipeline-runtime, and monolith build layouts.
- **Evolution path**: Start with a monolith for speed, then break into services as scale and ownership boundaries grow.

### Operations

- **Built-in observability**: Metrics, tracing, and structured logging are first-class.
- **Resilience features**: Health checks, robust error handling, and dead-letter queue support.
- **Container-first packaging**: Current delivery flow supports image builds with Jib for predictable CI/CD integration.
- **Native compilation path**: Quarkus native builds support low cold-start and efficient resource usage where needed.
- **Why it matters**: Teams get faster incident diagnosis, more predictable deployments, and lower operational overhead at scale.

### Developer Experience

- **Shared `common` module**: Central place for entity and DTO definitions used across pipeline components.
- **Type safety across the pipeline**: Compile-time checks catch contract drift early as steps and transports evolve.
- **Why it matters**: Teams get safer refactors, fewer runtime mapping errors, and faster confidence when changing domain models.
- **Testability gains**: Functional boundaries and generated transport layers make behavior easier to isolate and test.
- **Fast onboarding and testing**: Design visually with Canvas and validate behavior via [Testing with Testcontainers](/guide/development/testing).
