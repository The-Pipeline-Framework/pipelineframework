---
layout: home

hero:
  name: The Pipeline Framework
  text: Reactive Pipeline Processing
  tagline: Build scalable, resilient pipeline applications with Quarkus and Mutiny
  image:
    src: /logo.png
    alt: The Pipeline Framework
  actions:
    - theme: brand
      text: Quick Start
      link: /guide/getting-started/quick-start
    - theme: alt
      text: View on GitHub
      link: https://github.com/The-Pipeline-Framework/pipelineframework
    - theme: alt
      text: Design with Canvas
      link: https://app.pipelineframework.org

features:
  - title: Reactive by Design
    details: Built on Mutiny for non-blocking, high-performance applications
  - title: Immutable Architecture
    details: No database updates during pipeline execution - only appends/preserves
  - title: Multiple Processing Patterns
    details: OneToOne, OneToMany, ManyToOne, ManyToMany, SideEffect and blocking variants
  - title: gRPC & REST Flexibility
    details: Automatic adapter generation for fast gRPC or easy REST integration
  - title: Annotation Driven
    details: Simple annotations generate complex infrastructure automatically
  - title: Visual Design
    details: Use the Canvas designer to create and configure pipelines visually
  - title: Health Monitoring
    details: Built-in health check capabilities
  - title: Observability First
    details: Built-in metrics, tracing, and logging support
  - title: Resilient by Default
    details: Comprehensive error handling with dead letter queues
search: false
---

<Callout type="tip" title="Visual Pipeline Designer Available">
The Pipeline Framework includes a visual canvas designer at <a href="https://app.pipelineframework.org" target="_blank">https://app.pipelineframework.org</a> that allows you to create and configure your pipelines using an intuitive drag-and-drop interface. Simply design your pipeline visually, click "Download Application", and you'll get a complete ZIP file with all the generated source code - no command-line tools needed!
</Callout>

## Introduction

The Pipeline Framework is a powerful tool for building reactive pipeline processing systems. It simplifies the development of distributed systems by providing a consistent way to create, configure, and deploy pipeline steps.

## Key Features

- **Reactive Programming**: Built on top of Mutiny for non-blocking operations
- **Immutable Architecture**: No database updates during pipeline execution - only appends/preserves, ensuring data integrity
- **Annotation-Based Configuration**: Simplifies adapter generation with `@PipelineStep`
- **gRPC & REST Flexibility**: Automatic adapter generation for fast gRPC or easy REST integration
- **Multiple Processing Patterns**: OneToOne, OneToMany, ManyToOne, ManyToMany, SideEffect and blocking variants
- **Health Monitoring**: Built-in health check capabilities
- **Multiple Persistence Models**: Choose from reactive or virtual thread-based persistence
- **Modular Design**: Clear separation between runtime and deployment components
- **Auto-Generation**: Generates necessary infrastructure at build time
- **Observability**: Built-in metrics, tracing, and logging support
- **Error Handling**: Comprehensive error handling with DLQ support
- **Backpressure Management**: Reactive processing with configurable backpressure strategies

## Getting Started

New to The Pipeline Framework? Start with our [Quick Start](/versions/v26.2/guide/getting-started/quick-start) guide to learn the basics using the visual Canvas designer.

## Guides

To get started with The Pipeline Framework, explore these guides:

### Build Fast
- [Quick Start](/versions/v26.2/guide/getting-started/quick-start): Create a pipeline with the Canvas
- [Business Value](/versions/v26.2/guide/getting-started/business-value): Speed, ROI, and portability
- [Canvas Designer Guide](/versions/v26.2/guide/getting-started/canvas-guide): Deep dive into the visual designer

### Design
- [Application Structure](/versions/v26.2/guide/design/application-structure): Structuring pipeline applications
- [Common Module Structure](/versions/v26.2/guide/design/common-module-structure): Shared components and type mappings
- [Expansion and Reduction](/versions/v26.2/guide/design/expansion-and-reduction): Cardinality explained for imperative developers

### Build
- [Pipeline Compilation](/versions/v26.2/guide/build/pipeline-compilation): How build-time generation works
- [Configuration Reference](/versions/v26.2/guide/build/configuration/): Build-time and runtime settings
- [Dependency Management](/versions/v26.2/guide/build/dependency-management): Manage build-time and runtime deps
- [Best Practices](/versions/v26.2/guide/operations/best-practices): Operational and design guidance

### Develop
- [@PipelineStep Annotation](/versions/v26.2/guide/development/pipeline-step): Annotation contract and parameters
- [Code a Step](/versions/v26.2/guide/development/code-a-step): Implement a step and its mappers
- [Using Plugins](/versions/v26.2/guide/development/using-plugins): Apply plugins to pipelines
- [Mappers and DTOs](/versions/v26.2/guide/development/mappers-and-dtos): Type conversions and mappings
- [Dependency Management](/versions/v26.2/guide/build/dependency-management): Manage build-time and runtime deps
- [Upgrade Guide](/versions/v26.2/guide/development/upgrade): Version changes and migrations
- [Orchestrator Runtime](/versions/v26.2/guide/development/orchestrator-runtime): Coordinate pipeline execution

### Observe
- [Observability Overview](/versions/v26.2/guide/operations/observability/): Metrics, tracing, logs, and security notes
- [Metrics](/versions/v26.2/guide/operations/observability/metrics): Instrumentation and dashboards
- [Tracing](/versions/v26.2/guide/operations/observability/tracing): Distributed tracing and context propagation
- [Logging](/versions/v26.2/guide/operations/observability/logging): Structured logging and levels
- [Health Checks](/versions/v26.2/guide/operations/observability/health-checks): Liveness and readiness
- [Alerting](/versions/v26.2/guide/operations/observability/alerting): Alerts and noise reduction
- [Security Notes](/versions/v26.2/guide/operations/observability/security): Protect telemetry data
- [Error Handling & DLQ](/versions/v26.2/guide/operations/error-handling): Managing errors and dead letter queues

### Extend
- [Writing a Plugin](/versions/v26.2/guide/plugins/writing-a-plugin): Build your own aspects
- [Orchestrator Extensions](/versions/v26.2/guide/development/extension/orchestrator-runtime): Customize orchestration flows
- [Reactive Service Extensions](/versions/v26.2/guide/development/extension/reactive-services): Wrap or adapt process() behavior
- [Client Step Extensions](/versions/v26.2/guide/development/extension/client-steps): Customize client-side calls
- [REST Resource Extensions](/versions/v26.2/guide/development/extension/rest-resources): Extend generated REST resources

### Evolve
- [Functional Architecture](/versions/v26.2/guide/evolve/architecture): Architectural decisions and trade-offs
- [Annotation Processor Architecture](/versions/v26.2/guide/evolve/annotation-processor-architecture): Build-time IR, bindings, and renderers
- [Plugins Architecture](/versions/v26.2/guide/evolve/plugins-architecture): Cross-cutting behavior model
- [Aspect Semantics](/versions/v26.2/guide/evolve/aspects/semantics): Aspect expansion rules
- [Aspect Ordering](/versions/v26.2/guide/evolve/aspects/ordering): Ordering guarantees and constraints
- [Aspect Warnings](/versions/v26.2/guide/evolve/aspects/warnings): Known limitations and caveats
- [Reference Implementation](/versions/v26.2/guide/evolve/reference-implementation): End-to-end example and rationale
- [Template Generator (Reference)](/versions/v26.2/guide/evolve/template-generator): Automation/CI usage
- [Publishing](/versions/v26.2/guide/evolve/publishing): Release and publishing workflow
- [CI Guidelines](/versions/v26.2/guide/evolve/ci-guidelines): Build validation and automation
- [Testing Guidelines](/versions/v26.2/guide/evolve/testing-guidelines): Coverage and test strategy
- [Gotchas & Pitfalls](/versions/v26.2/guide/evolve/gotchas-pitfalls): Known sharp edges
- [Proto Descriptor Integration](/versions/v26.2/guide/evolve/protobuf-integration-descriptor-res): Descriptor generation and troubleshooting

This approach reduces boilerplate code and ensures consistency across your pipeline steps.
