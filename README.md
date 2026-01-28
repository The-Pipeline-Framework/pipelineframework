# The Pipeline Framework

[![Maven Central](https://img.shields.io/maven-central/v/org.pipelineframework/pipelineframework.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22org.pipelineframework%22%20AND%20a:%22pipelineframework%22)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Java 21+](https://img.shields.io/badge/Java-21+-brightgreen.svg)](https://adoptium.net/)
[![Quarkus](https://img.shields.io/badge/Quarkus-3.30.5-orange)](https://quarkus.io)
[![CodeRabbit](https://img.shields.io/coderabbit/prs/github/The-Pipeline-Framework/pipelineframework?label=CodeRabbit&color=purple
)](https://coderabbit.ai)

The Pipeline Framework is a powerful framework for building reactive pipeline processing systems. It simplifies the development of distributed systems by providing a consistent way to create, configure, and deploy pipeline steps with automatic gRPC and REST adapter generation.

## 🚀 Quick Start

### Visual Designer
The fastest way to get started is using our visual canvas designer:
- Visit [https://app.pipelineframework.org](https://app.pipelineframework.org)
- Design your pipeline visually
- Download the complete source code for your pipeline application

## 📋 Featuring capabilities

### Product Owners
- **Microservices Architecture**: Each pipeline step can be deployed and scaled independently
- **Data Integrity**: Immutability ensures that all transformations are preserved, providing robust data integrity
- **Audit Trail**: Every input transformation is permanently recorded, enabling complete auditability
- **Reliability**: Built-in dead letter queue for error handling ensures resilient processing

### Architects
- **Immutability by Design**: No database updates during pipeline execution - only appends/preserves
- **Kubernetes & Serverless Ready**: Native builds enable quick start-up times for Kubernetes and serverless platforms (AWS Lambda, Google Cloud Run)
- **Cost Efficiency**: Reduced resource consumption with native builds and reactive processing
- **Scalability**: Each pipeline step can be scaled independently based on demand
- **gRPC & REST Flexibility**: High-performance gRPC for throughput-intensive tasks, REST for ease of integration
- **Multiple Persistence Options**: Choose from reactive (Panache) or traditional/blocking persistence drivers, for multiple database vendors
- **Rich Step Types**: Support for OneToOne, OneToMany, ManyToOne, ManyToMany, and SideEffect processing patterns

### Developers
- **Monorepo Support**: Complete pipeline can be debugged step-by-step in a single repository
- **Compile-Time Safety**: All entities in common package provide type safety across steps
- **Reactive Processing**: Mutiny's event/worker thread model for efficient concurrency
- **Build-Time Generation**: Automatic gRPC and REST adapter generation at build time
- **gRPC & REST Flexibility**: Fast gRPC for high-throughput scenarios, REST for ease of development and integration
- **TLS Ready**: Reference implementation includes TLS configuration out of the box
- **Multiple Processing Patterns**: OneToOne, OneToMany, ManyToOne, ManyToMany, SideEffect and blocking variants
- **Health Monitoring**: Built-in health check capabilities for infrastructure monitoring

### QA Engineers
- **Unit Testing Excellence**: Each step deliberately kept small, making unit tests easy to write and fast to run
- **Deterministic Behavior**: Immutability ensures consistent test results across runs
- **AI-Assisted Development**: Modular structure is ideal for AI coding assistants
- **Focused Testing**: Each step's limited scope enables comprehensive testing
- **Multiple Execution Modes**: Test with different processing patterns (OneToOne, OneToMany, etc.) to validate behavior
- **Health Check Testing**: Built-in health monitoring provides additional testable endpoints

### CTOs
- **Cost Reduction**: Native builds and reactive processing reduce infrastructure costs
- **Developer Productivity**: Rapid development with visual designer and auto-generation
- **Operational Excellence**: Built-in observability, health checks, and error handling
- **Technology Modernization**: Reactive, cloud-native architecture with industry standards
- **Risk Mitigation**: Dead letter queue and multiple processing patterns provide resilience

## ✨ Capabilities summary

- Reactive, immutable architecture with strong compile-time safety  
- Automatic adapter generation for REST & gRPC  
- Modular monorepo with clear separation of runtime vs. deployment  
- Native-ready builds for ultra‑fast startup and cloud efficiency  
- Built-in health monitoring, observability, metrics, and tracing  
- Visual pipeline designer for rapid development  
- Multiple execution patterns (OneToOne, OneToMany, ManyToOne, ManyToMany, SideEffect, blocking)  
- Comprehensive error handling with dead-letter queue support  
- Multiple persistence models (reactive or virtual‑thread‑based)  
- Strong testing support: deterministic, focused, and easy to automate  

## 📚 Documentation

- **[Full Documentation](https://pipelineframework.org)** - Complete documentation site
- **[Quick Start Guide](https://pipelineframework.org/guide/getting-started/quick-start)** - Step-by-step tutorial
- **[Canvas Designer Guide](https://pipelineframework.org/guide/getting-started/canvas-guide)** - Visual design tool
- **[API Reference](https://pipelineframework.org/guide/development/pipeline-step)** - Annotation documentation

## 🏗️ Architecture

The framework follows a modular architecture:

```text
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Orchestrator  │───▶│ Backend Service  │───▶│ Backend Service │
│   (Coordinates) │    │     (Step 1)     │    │     (Step 2)    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                        │                       │
         ▼                        ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│    Input Data   │    │   Processing     │    │   Output Data   │
│                 │    │   Pipeline       │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

Each pipeline step is implemented as an independent service that can be scaled and deployed separately.

## 🛠️ Example Implementation

Here's a simple pipeline step implementation:

```java
@PipelineStep(
    inputType = PaymentRecord.class,
    outputType = PaymentStatus.class,
    stepType = StepOneToOne.class,
    inboundMapper = PaymentRecordMapper.class,
    outboundMapper = PaymentStatusMapper.class
)
@ApplicationScoped
public class ProcessPaymentService implements ReactiveService<PaymentRecord, PaymentStatus> {

    @Override
    public Uni<PaymentStatus> process(PaymentRecord input) {
        // Business logic here
        PaymentStatus status = new PaymentStatus();
        // Process the payment record
        return Uni.createFrom().item(status);
    }
}
```

## 🔌 Plugin System

The framework now includes a powerful plugin system that enables external components to participate in pipelines as side effect steps with typed gRPC endpoints. Plugins are implemented using simple interfaces without dependencies on DTOs, mappers, or protos.

```java
@ApplicationScoped
public class LoggingPlugin implements ReactiveSideEffectService<String> {
    private static final Logger LOG = Logger.getLogger(LoggingPlugin.class);

    @Override
    public Uni<String> process(String message) {
        LOG.info("Processing message: " + message);
        return Uni.createFrom().voidItem();
    }
}
```

For more information about the plugin system, see the [plugins' documentation](docs/plugins/index.md).

## Continuous Integration
See our [CI documentation](CI.md)

## 🧪 Testing
See our [Testing documentation](TESTING.md)

## 🛡️ Security

If you discover a security vulnerability, please see our [security policy](SECURITY.md) for details on how to report it responsibly.

## 🤝 Contributing

Contributions of all kinds are welcome — code, documentation, ideas, translations and questions.

Please read our [contribution guidelines](CONTRIBUTING.md) for details on how to get started.
