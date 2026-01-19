---
search: false
---

# The Pipeline Framework Guide

Welcome to The Pipeline Framework guide! This guide will help you understand how to use the framework to build reactive pipeline processing systems that work for your specific use case.

<Callout type="tip" title="Immutable Architecture">
The Pipeline Framework follows an immutable architecture where no database updates occur during pipeline execution - only appends/preserves. This ensures complete data integrity and provides a complete audit trail of all transformations.
</Callout>

<Callout type="tip" title="Rich Processing Patterns">
The framework supports multiple processing patterns including OneToOne, OneToMany, ManyToOne, ManyToMany, SideEffect and blocking variants. Each step type is optimized for specific data transformation scenarios.
</Callout>

## How to Use This Guide by Role

### For Developers
Start here to create your first pipeline applications:

- [Quick Start](/versions/v0.9.2/guide/quick-start): Get up and running in minutes with the visual Canvas designer
- [Creating Pipeline Steps](/versions/v0.9.2/guide/creating-steps): Build your first pipeline step from scratch
- [Application Structure](/versions/v0.9.2/guide/application-structure): Understand how to organize your code
- [Backend Services](/versions/v0.9.2/guide/backend-services): Implement the business logic for your steps
- [Mappers and DTOs](/versions/v0.9.2/guide/mappers-and-dtos): Handle data transformation between object types
- [Configuration](/versions/v0.9.2/guide/configuration): Manage your pipeline configuration settings
- [Error Handling & DLQ](/versions/v0.9.2/guide/error-handling): Implement error handling and dead letter queue functionality
- [Observability](/versions/v0.9.2/guide/observability): Monitor and observe your pipeline applications
- [Best Practices](/versions/v0.9.2/guide/best-practices): Follow recommended approaches for pipeline applications
- [Orchestrator Services](/versions/v0.9.2/guide/orchestrator-services): Control the flow of your pipeline

### For QA Engineers
Learn how to test pipeline applications effectively:

- [Testing Strategies](/versions/v0.9.2/guide/best-practices#testing): Testing strategies for pipeline applications
- [Observability](/versions/v0.9.2/guide/observability): How to monitor and verify pipeline behavior
- [Error Handling & DLQ](/versions/v0.9.2/guide/error-handling): Testing error scenarios and recovery

### For Product Owners
Understand the business value and capabilities:

- [Architecture](/versions/v0.9.2/reference/architecture): High-level overview of the system design
- [Reference Implementation](/versions/v0.9.2/REFERENCE_IMPLEMENTATION): See a real-world example
- [Canvas Guide](/versions/v0.9.2/CANVAS_GUIDE): Learn about the visual design tool for non-technical stakeholders

### For Architects
Deep-dive into design patterns and enterprise considerations:

- [Architecture](/versions/v0.9.2/reference/architecture): System design patterns and principles
- [Observability](/versions/v0.9.2/guide/observability): Enterprise monitoring and tracing strategies
- [Error Handling & DLQ](/versions/v0.9.2/guide/error-handling): Resilience and fault-tolerance patterns
- [Configuration](/versions/v0.9.2/guide/configuration): Environment-specific settings and deployment strategies
- [Best Practices](/versions/v0.9.2/guide/best-practices): Design and implementation best practices

### For CTOs
Get a strategic overview for technical leadership:

- [Architecture](/versions/v0.9.2/reference/architecture): System capabilities and technology stack
- [YAML Schema](/versions/v0.9.2/YAML_SCHEMA): Infrastructure-as-code configuration options
- [Best Practices](/versions/v0.9.2/guide/best-practices): Organizational implementation guidelines
- [Observability](/versions/v0.9.2/guide/observability): Enterprise monitoring strategies
- [Configuration](/versions/v0.9.2/guide/configuration): Enterprise deployment strategies

## Prerequisites

Before diving into The Pipeline Framework, make sure you have:

- Java 21 or higher
- Maven 3.8+
- A Quarkus-based project (the framework is designed to work with Quarkus)
- Basic understanding of reactive programming concepts

Happy pipelining!