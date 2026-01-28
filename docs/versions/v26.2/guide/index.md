---
search: false
---

# Introduction

The Pipeline Framework is a powerful tool for building reactive pipeline processing systems. It simplifies the development of distributed systems by providing a consistent way to create, configure, and deploy pipeline steps.

<Callout type="tip" title="Visual Pipeline Designer">
The Pipeline Framework includes a visual canvas designer at <a href="https://app.pipelineframework.org" target="_blank">https://app.pipelineframework.org</a> that allows you to create and configure your pipelines using an intuitive drag-and-drop interface. Simply design your pipeline visually, click "Download Application", and you'll get a complete ZIP file with all the generated source code - no command-line tools needed!
</Callout>

## Key Features

- **Reactive Programming**: Built on top of Mutiny for non-blocking operations
- **Immutable Architecture (by default)**: Persistence is append-first and configurable; updates are explicit and opt-in
- **Visual Design Canvas**: Create and configure pipelines with the visual designer at [https://app.pipelineframework.org](https://app.pipelineframework.org)
- **Annotation-Based Configuration**: Simplifies adapter generation with `@PipelineStep`
- **gRPC & REST Flexibility**: Automatic adapter generation for fast gRPC or easy REST integration
- **Multiple Processing Patterns**: OneToOne, OneToMany, ManyToOne, ManyToMany, SideEffect and blocking variants
- **Health Monitoring**: Built-in health check capabilities
- **Multiple Persistence Models**: Choose from reactive or virtual thread-based persistence
- **Modular Design**: Clear separation between runtime and deployment components
- **Test Integration**: Built-in support for unit and integration tests with Testcontainers

## Framework Overview

For complete documentation of the framework architecture, implementation details, and reference implementations, see the complete documentation files in the main repository:

- [Reference Implementation](/versions/v26.2/guide/evolve/reference-implementation) - Complete implementation guide with examples
- [Canvas Designer Guide](/versions/v26.2/guide/getting-started/canvas-guide) - Complete Canvas usage guide
- [Java-Centered Types](/versions/v26.2/guide/development/java-centered-types) - Comprehensive Java-first approach with automatic protobuf mapping

## How It Works

The framework lets you define pipeline steps as simple classes annotated with `@PipelineStep`. At build time, it generates the adapters and infrastructure, keeping services clean and focused on business logic.

### Build Fast
- [Quick Start](/versions/v26.2/guide/getting-started/quick-start): Create a pipeline via the Canvas and run it locally
- [Business Value](/versions/v26.2/guide/getting-started/business-value): Speed, ROI, and portability
- [Canvas Designer Guide](/versions/v26.2/guide/getting-started/canvas-guide): Complete guide to the visual designer

### Design
- [Application Structure](/versions/v26.2/guide/design/application-structure): Modular layout and service boundaries
- [Common Module Structure](/versions/v26.2/guide/design/common-module-structure): Shared domain types and mappers
- [Expansion and Reduction](/versions/v26.2/guide/design/expansion-and-reduction): Cardinality explained for imperative developers

### Build
- [Pipeline Compilation](/versions/v26.2/guide/build/pipeline-compilation): Build-time generation flow
- [Configuration Reference](/versions/v26.2/guide/build/configuration/): Build-time and runtime settings
- [Dependency Management](/versions/v26.2/guide/build/dependency-management): Manage build-time and runtime deps
- [Best Practices](/versions/v26.2/guide/operations/best-practices): Operational and design guidance

### Develop
- [@PipelineStep Annotation](/versions/v26.2/guide/development/pipeline-step): Annotation contract and parameters
- [Code a Step](/versions/v26.2/guide/development/code-a-step): Implement a step and its mappers
- [Using Plugins](/versions/v26.2/guide/development/using-plugins): Apply plugins to pipelines
- [Mappers and DTOs](/versions/v26.2/guide/development/mappers-and-dtos): Type conversions across layers
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
- [Error Handling & DLQ](/versions/v26.2/guide/operations/error-handling): Failure handling patterns
- [Caching](/versions/v26.2/guide/plugins/caching/): Cache policies, backends, and invalidation

### Extend
- [Writing a Plugin](/versions/v26.2/guide/plugins/writing-a-plugin): Create plugins and aspects
- [Orchestrator Extensions](/versions/v26.2/guide/development/extension/orchestrator-runtime): Customize orchestration flows
- [Reactive Service Extensions](/versions/v26.2/guide/development/extension/reactive-services): Wrap or adapt process() behavior
- [Client Step Extensions](/versions/v26.2/guide/development/extension/client-steps): Customize client-side calls
- [REST Resource Extensions](/versions/v26.2/guide/development/extension/rest-resources): Extend generated REST resources

### Evolve
- [Functional Architecture](/versions/v26.2/guide/evolve/architecture): Core concepts and architectural patterns
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

This approach reduces boilerplate and keeps pipeline code consistent and portable.
