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
      link: /guide/quick-start
    - theme: alt
      text: View on GitHub
      link: https://github.com/mbarcia/pipelineframework
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

New to The Pipeline Framework? Start with our [Quick Start](/versions/v0.9.2/guide/quick-start) guide to learn the basics using the visual Canvas designer.

## Guides

To get started with The Pipeline Framework, explore these guides:

### Getting Started
- [Getting Started](/versions/v0.9.2/guide/getting-started.html): Setting up the framework in your project
- [Creating Pipeline Steps](/versions/v0.9.2/guide/creating-steps.html): Building your first pipeline steps

### Application Development
- [Application Structure](/versions/v0.9.2/guide/application-structure.html): Structuring pipeline applications
- [Backend Services](/versions/v0.9.2/guide/backend-services.html): Creating backend services that implement pipeline steps
- [Orchestrator Services](/versions/v0.9.2/guide/orchestrator-services.html): Building orchestrator services that coordinate pipelines

### Advanced Topics
- [Pipeline Compilation](/versions/v0.9.2/guide/pipeline-compilation.html): Understanding how the annotation processor works
- [Error Handling & DLQ](/versions/v0.9.2/guide/error-handling.html): Managing errors and dead letter queues
- [Observability](/versions/v0.9.2/guide/observability.html): Monitoring and observing pipeline applications

### Reference
- [Architecture](/versions/v0.9.2/reference/architecture.html): Deep dive into the framework architecture
- [Framework Overview](/versions/v0.9.2/FRAMEWORK_OVERVIEW.html): Complete architecture and comparison to original spec
- [Reference Implementation](/versions/v0.9.2/REFERENCE_IMPLEMENTATION.html): Complete implementation guide with examples
- [YAML Configuration Schema](/versions/v0.9.2/YAML_SCHEMA.html): Complete YAML schema documentation
- [Canvas Designer Guide](/versions/v0.9.2/CANVAS_GUIDE.html): Complete Canvas usage guide
- [Java-Centered Types](/versions/v0.9.2/JAVA_CENTERED_TYPES.html): Java-first approach with protobuf mapping
- [Publishing to Maven Central](/versions/v0.9.2/PUBLISHING.html): Guide to releasing and publishing the framework

This approach reduces boilerplate code and ensures consistency across your pipeline steps.