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
- **Immutable Architecture**: No database updates during pipeline execution - only appends/preserves, ensuring data integrity
- **Visual Design Canvas**: Create and configure pipelines with the visual designer at <a href="https://app.pipelineframework.org" target="_blank">https://app.pipelineframework.org</a>
- **Annotation-Based Configuration**: Simplifies adapter generation with `@PipelineStep`
- **gRPC & REST Flexibility**: Automatic adapter generation for fast gRPC or easy REST integration
- **Multiple Processing Patterns**: OneToOne, OneToMany, ManyToOne, ManyToMany, SideEffect and blocking variants
- **Health Monitoring**: Built-in health check capabilities
- **Multiple Persistence Models**: Choose from reactive or virtual thread-based persistence
- **Modular Design**: Clear separation between runtime and deployment components
- **Test Integration**: Built-in support for unit and integration tests with Testcontainers

## Framework Overview

For complete documentation of the framework architecture, implementation details, and reference implementations, see the complete documentation files in the main repository:

- [Reference Implementation](/versions/v0.9.2/REFERENCE_IMPLEMENTATION.html) - Complete implementation guide with examples
- [YAML Configuration Schema](/versions/v0.9.2/YAML_SCHEMA.html) - Complete YAML schema documentation
- [Canvas Designer Guide](/versions/v0.9.2/CANVAS_GUIDE.html) - Complete Canvas usage guide
- [Java-Centered Types](/versions/v0.9.2/JAVA_CENTERED_TYPES.html) - Comprehensive Java-first approach with automatic protobuf mapping

## How It Works

The framework allows you to define pipeline steps as simple classes annotated with `@PipelineStep`. The framework automatically generates the necessary adapters at build time, eliminating the need for manual configuration.

### Getting Started
- [Quick Start](/versions/v0.9.2/guide/quick-start): Get started quickly with the visual Canvas designer
- [Canvas Designer Guide](/versions/v0.9.2/CANVAS_GUIDE.html): Complete guide to using the visual designer
- [Using the Template Generator](/versions/v0.9.2/guide/using-template-generator): Advanced usage of the template generator

### Application Development
- [Application Structure](/versions/v0.9.2/guide/application-structure): Structuring pipeline applications
- [Backend Services](/versions/v0.9.2/guide/backend-services): Creating backend services that implement pipeline steps
- [Orchestrator Services](/versions/v0.9.2/guide/orchestrator-services): Building orchestrator services that coordinate pipelines

### Advanced Topics
- [Pipeline Compilation](/versions/v0.9.2/guide/pipeline-compilation): Understanding how the annotation processor works
- [Error Handling & DLQ](/versions/v0.9.2/guide/error-handling): Managing errors and dead letter queues
- [Observability](/versions/v0.9.2/guide/observability): Monitoring and observing pipeline applications

### Reference
- [Architecture](/versions/v0.9.2/reference/architecture): Deep dive into the framework architecture

This approach reduces boilerplate code and ensures consistency across your pipeline steps.