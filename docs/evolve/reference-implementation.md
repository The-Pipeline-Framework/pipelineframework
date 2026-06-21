# The Pipeline Framework: Reference Implementation

## Overview

This document provides a comprehensive reference implementation of The Pipeline Framework, demonstrating how to create pipeline applications using both YAML configuration files and the visual Canvas tool.

## Quick Start with YAML Configuration

### 1. Generate Sample Configuration

First, generate a sample configuration file:

```bash
pipeline-template-generator --generate-config
```

This creates `sample-pipeline-config.yaml` with a complete example configuration.

Alternatively, if you're using the JAR file:

```bash
java -jar template-generator-1.0.0.jar --generate-config
```

### 2. Customize Configuration

Edit the generated YAML file to define your pipeline:

```yaml
version: 2
appName: "My Pipeline App"
basePackage: "com.example.mypipeline"
transport: "GRPC"
messages:
  CustomerInput:
    fields:
      - number: 1
        name: "id"
        type: "uuid"
      - number: 2
        name: "name"
        type: "string"
      - number: 3
        name: "email"
        type: "string"
  CustomerOutput:
    fields:
      - number: 1
        name: "id"
        type: "uuid"
      - number: 2
        name: "name"
        type: "string"
      - number: 3
        name: "status"
        type: "string"
      - number: 4
        name: "processedAt"
        type: "timestamp"
  ValidationOutput:
    fields:
      - number: 1
        name: "id"
        type: "uuid"
      - number: 2
        name: "isValid"
        type: "bool"
steps:
  - name: "Process Customer"
    cardinality: "ONE_TO_ONE"
    inputTypeName: "CustomerInput"
    outputTypeName: "CustomerOutput"
  - name: "Validate Order"
    cardinality: "ONE_TO_ONE"
    inputTypeName: "CustomerOutput"
    outputTypeName: "ValidationOutput"
```

### 3. Generate Application

Generate the complete application from your configuration:

```bash
java -jar template-generator-1.0.0.jar --config my-pipeline-config.yaml --output ./my-pipeline-app
```

## Pipeline Step Types and Cardinalities

### ONE_TO_ONE (1-1)

Transforms single input to single output:

```java
@PipelineStep(
    inputType = CustomerInput.class,
    outputType = CustomerOutput.class,
    stepType = StepOneToOne.class
)
public class ProcessCustomerStep implements ReactiveService<CustomerInput, CustomerOutput> {
    @Override
    public Uni<CustomerOutput> process(CustomerInput input) {
        // Implementation here
    }
}
```

### EXPANSION (1-Many)

Transforms single input to multiple outputs:

```java
@PipelineStep(
    inputType = CustomerOutput.class,
    outputType = OrderInput.class,
    stepType = StepOneToMany.class
)
public class GenerateOrdersStep implements ReactiveStreamingService<CustomerOutput, OrderInput> {
    @Override
    public Multi<OrderInput> process(CustomerOutput input) {
        // Implementation here
    }
}
```

### REDUCTION (Many-1)

Aggregates multiple inputs to single output:

```java
@PipelineStep(
    inputType = OrderInput.class,
    outputType = SummaryOutput.class,
    stepType = StepManyToOne.class
)
public class AggregateOrdersStep implements ReactiveStreamingClientService<OrderInput, SummaryOutput> {
    @Override
    public Uni<SummaryOutput> process(Multi<OrderInput> input) {
        // Implementation here
    }
}
```

### SIDE_EFFECT (1-1 with same input/output)

Performs side effects without changing data:

```java
@PipelineStep(
    inputType = SummaryOutput.class,
    outputType = SummaryOutput.class,  // Same as input type
    stepType = StepOneToOne.class
)
public class LogSummaryStep implements ReactiveService<SummaryOutput, SummaryOutput> {
    @Override
    public Uni<SummaryOutput> process(SummaryOutput input) {
        // Side effect implementation, returns same input
    }
}
```

## Quick Start with Visual Canvas

### 1. Visit the Canvas Designer

Go to https://app.pipelineframework.org to use the visual designer.

### 2. Create Steps Visually

- Drag and drop new steps onto the canvas
- Select cardinality for each step
- Define input/output types and fields
- Connect steps to establish pipeline flow

### 3. Download Configuration

- Generate YAML configuration from the visual design
- Download the configuration file
- Use it with the template generator:

```bash
java -jar template-generator-1.0.0.jar -c downloaded-config.yaml -o ./my-pipeline-app
```

## Generated Project Structure

The template generator creates a complete Maven multi-module project:

```text
my-pipeline-app/
├── pom.xml                       # Parent POM with all modules
├── common/                       # Shared components
│   ├── pom.xml
│   ├── src/main/proto/           # gRPC proto definitions
│   ├── src/main/java/com/example/mypipeline/common/domain/    # Entity classes
│   ├── src/main/java/com/example/mypipeline/common/dto/       # DTO classes
│   └── src/main/java/com/example/mypipeline/common/mapper/    # Mapper classes
├── process-customer-svc/         # First pipeline step service
│   ├── pom.xml
│   ├── src/main/java/...        # Service implementation
│   └── Dockerfile
├── validate-order-svc/           # Second pipeline step service
│   ├── pom.xml
│   ├── src/main/java/...        # Service implementation
│   └── Dockerfile
├── orchestrator-svc/             # Orchestrator service
│   ├── pom.xml
│   ├── README.md
│   ├── src/main/java/...        # Service implementation
│   ├── src/main/resources/...   # Configuration files
│   ├── src/test/...             # Test files
│   ├── multi-file-e2e-test-input/ # E2E test input files
│   └── multi-file-e2e-test-output/ # E2E test output files
├── mvnw                          # Maven wrapper (Unix)
├── mvnw.cmd                      # Maven wrapper (Windows)
├── .mvn/wrapper/                 # Maven wrapper files
└── README.md                     # Project documentation
```

## Implementation Workflow

### 1. Define Pipeline Configuration

- Use YAML file or Canvas designer to define steps
- Specify cardinalities and data types
- Define field mappings for each step

### 2. Generate Application

- Run template generator to create full project
- Generated code includes all necessary components
- Observability stack is included by default

### 3. Implement Business Logic

- Fill in the `process()` methods in generated service classes
- Use reactive patterns with Uni/Multi as appropriate
- Add domain-specific logic and validations

### 4. Test and Deploy

- Use generated Docker Compose for local testing
- Deploy individual services as needed
- Monitor with integrated observability tools

## Field Type Mapping

Current templates use semantic v2 types. The compiler derives Java bindings and protobuf wire types from the semantic declaration:

| Semantic Type | Default Java Binding | Default Protobuf Binding |
|---------------|----------------------|--------------------------|
| `string` | `String` | `string` |
| `bool` | `Boolean` | `bool` |
| `int32` | `Integer` | `int32` |
| `int64` | `Long` | `int64` |
| `float32` | `Float` | `float` |
| `float64` | `Double` | `double` |
| `uuid` | `UUID` | `string` |
| `decimal` | `BigDecimal` | `string` |
| `currency` | `Currency` | `string` |
| `timestamp` | `Instant` | `string` |
| `datetime` | `LocalDateTime` | `string` |
| `date` | `LocalDate` | `string` |
| `duration` | `Duration` | `string` |
| `uri` | `URI` | `string` |
| `path` | `Path` | `string` |

### Automatic Conversion

- MapStruct handles automatic conversion between Java and protobuf types for built-in types
- Built-in conversions: primitives, wrappers, UUID, BigDecimal/BigInteger, Java 8 time types, URI/URL/File/Path
- Custom converters in `CommonConverters` class manage specialized mappings: Currency, AtomicInteger, AtomicLong, `List<String>`
- Null-safe conversions for all supported types

## Inter-Step Dependencies

The framework automatically manages type dependencies between steps:

1. **Automatic Imports**: Generated proto files import types from previous steps
2. **Type Propagation**: Output types of one step become input types of the next
3. **Validation**: Configuration validation ensures type consistency
4. **Synchronization**: Field synchronization across connected steps

## Best Practices

1. **Start Small**: Begin with simple 1-1 steps to understand the flow
2. **Use Canvas**: For complex pipelines, use the visual designer first
3. **Test Incrementally**: Implement and test each step separately
4. **Monitor**: Use the integrated observability tools to monitor performance
5. **Document**: Add documentation for complex business logic
