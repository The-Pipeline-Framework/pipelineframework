---
search: false
---

# Using the Template Generator

This guide explains how to use the Pipeline Framework's template generator to quickly create complete pipeline applications from YAML configuration files.

<Callout type="tip" title="Visual Alternative">
Instead of using the template generator, you can use the visual Canvas designer at <a href="https://app.pipelineframework.org" target="_blank">https://app.pipelineframework.org</a> to create and configure your pipeline applications. The Canvas provides an intuitive drag-and-drop interface for defining pipeline steps and their connections, then generates the appropriate configuration files.
</Callout>

## Overview

The template generator is a command-line tool that creates complete Maven multi-module pipeline applications from YAML configuration files. It automates the entire process of:

- Generating parent POM with all modules properly configured
- Creating common module with domain entities, DTOs, and mappers
- Generating individual service modules for each pipeline step
- Creating orchestrator module with CLI application and configuration
- Creating configuration for test environments
- Setting up proper Maven build configurations
- Configuring service communication via gRPC

## Prerequisites

Before using the template generator, ensure you have:

- Java 21+ installed on your system
- Maven 3.8+ installed on your system
- Access to the template generator JAR file

## Getting Started

### 1. Download the Template Generator

Download the latest template generator JAR file from the releases page or build it from source.

### 2. Generate a Sample Configuration

Generate a sample YAML configuration file to understand the structure:

```bash
java -jar template-generator-1.0.0.jar --generate-config
```

This creates `sample-pipeline-config.yaml` with a complete example configuration.

### 3. Customize Your Pipeline Configuration

Edit the generated YAML file to define your pipeline. The configuration includes:

- Application name and base package
- List of pipeline steps with their properties
- Input/output type definitions for each step
- Field definitions with rich Java type system

Example configuration:

```yaml
---
appName: "Payment Processing Pipeline"
basePackage: "com.example.payments"
steps:
- name: "Process Customer"
  cardinality: "ONE_TO_ONE"
  inputTypeName: "CustomerInput"
  inputFields:
  - name: "id"
    type: "UUID"
    protoType: "string"
  - name: "name"
    type: "String"
    protoType: "string"
  outputTypeName: "CustomerOutput"
  outputFields:
  - name: "id"
    type: "UUID"
    protoType: "string"
  - name: "name"
    type: "String"
    protoType: "string"
  - name: "status"
    type: "String"
    protoType: "string"
- name: "Validate Order"
  cardinality: "ONE_TO_ONE"
  inputTypeName: "CustomerOutput"
  outputTypeName: "ValidationOutput"
  outputFields:
  - name: "id"
    type: "UUID"
    protoType: "string"
  - name: "isValid"
    type: "Boolean"
    protoType: "bool"
```

### 4. Generate Your Complete Application

Generate the complete application from your configuration:

```bash
java -jar template-generator-1.0.0.jar --config my-pipeline-config.yaml --output ./my-pipeline-app
```

## Generated Application Structure

The template generator creates a complete Maven multi-module project:

```text
my-pipeline-app/
├── pom.xml                           # Parent POM
├── common/                           # Shared components
│   ├── pom.xml
│   └── src/main/java/
│       └── com/example/app/common/
│           ├── domain/              # Domain entities
│           ├── dto/                 # Data Transfer Objects
│           └── mapper/              # MapStruct mappers
├── process-customer-svc/            # First pipeline step service
│   ├── pom.xml
│   └── src/main/java/
│       └── com/example/app/processcustomer/service/
├── validate-order-svc/             # Second pipeline step service
│   ├── pom.xml
│   └── src/main/java/
│       └── com/example/app/validateorder/service/
├── orchestrator-svc/               # Pipeline orchestrator
│   ├── pom.xml
│   └── src/main/java/
│       └── com/example/app/orchestrator/
├── src/test/resources/              # Test configuration
│   └── application-test.properties  # Test-specific configuration
├── src/test/java/                   # Integration tests
│   └── **/*IT.java                  # Integration test classes
└── mvnw                             # Maven wrapper
```

## Running the Generated Application

Navigate to your generated application directory and build it:

```bash
cd my-pipeline-app
./mvnw clean compile
```

Run the application in development mode:

```bash
./mvnw quarkus:dev
```

Or package and run in production mode:

```bash
./mvnw clean package
java -jar target/my-app-runner.jar
```

## Customizing Generated Applications

While the template generator creates a complete application, you can customize it for your specific needs:

### 1. Modify Service Implementations

Each generated service includes a placeholder `apply()` method that you need to implement:

```java
// process-customer-svc/src/main/java/com/example/app/processcustomer/service/ProcessProcessCustomerService.java
@PipelineStep(
   order = 1,
   inputType = CustomerInput.class,
   outputType = CustomerOutput.class,
   stepType = StepOneToOne.class,
   grpcStub = MutinyProcessCustomerServiceGrpc.MutinyProcessCustomerServiceStub.class,
   grpcImpl = MutinyProcessCustomerServiceGrpc.ProcessCustomerServiceImplBase.class,
   inboundMapper = CustomerInputMapper.class,
   outboundMapper = CustomerOutputMapper.class,
   grpcClient = "process-customer"
)
public class ProcessProcessCustomerService implements StepOneToOne<CustomerInput, CustomerOutput> {
    @Override
    public Uni<CustomerOutput> applyOneToOne(Uni<CustomerInput> request) {
        // TODO: Implement your business logic here
        return request.map(customerInput -> {
            CustomerOutput output = new CustomerOutput();
            output.setId(customerInput.getId());
            output.setName(customerInput.getName().toUpperCase());
            output.setStatus("PROCESSED");
            output.setProcessedAt(LocalDateTime.now().toString());
            return output;
        });
    }
}
```

### 2. Customize Orchestrator Input Provisioning

Modify the orchestrator application's input provisioning logic:

```java
// orchestrator-svc/src/main/java/com/example/app/orchestrator/service/ProcessFolderService.java
@ApplicationScoped
public class ProcessFolderService {

    public Stream<InputType> process(String inputPath) throws URISyntaxException {
        // TODO: Implement your input provisioning logic
        // This could read from files, databases, message queues, etc.
    }
}
```

### 3. Adjust Configuration

Customize the generated application properties:

```properties
# orchestrator-svc/src/main/resources/application.properties
# Pipeline Configuration
pipeline.runtime.retry-limit=5
pipeline.runtime.retry-wait-ms=1000
```

## Advanced Features

### YAML Configuration Modes

The template generator supports different modes of operation:

1. **Interactive Mode**: Step-by-step CLI wizard to collect pipeline specifications
2. **YAML File Mode**: Generate applications from predefined YAML configuration files
3. **YAML Generation Mode**: Create sample configuration files for reference

### Rich Java Type System

The template generator uses a Java DTO-centered approach with automatic protobuf conversion:

- Rich Java types: String, Integer, Long, Double, Boolean, UUID, BigDecimal, Currency, Path, `List<String>`, LocalDateTime, etc.
- Automatic mapping to appropriate protobuf equivalents
- Built-in conversions for primitives, wrappers, UUID, BigDecimal/BigInteger, Java 8 time types, URI/URL/File/Path
- Custom converters for Currency, AtomicInteger, AtomicLong, `List<String>`

### Automatic Import Management

The generator intelligently manages imports based on the Java types used in your pipeline:

- Automatic generation of necessary import statements
- Proper grouping of Java standard library, third-party, and custom imports
- Resolution of type dependencies between pipeline steps

### MapStruct Integration

The template generator automatically generates MapStruct mappers with:

- Domain ↔ DTO conversions
- DTO ↔ gRPC conversions
- Intelligent built-in and custom converters
- Null-safe conversions for all supported types

## Troubleshooting

### Common Issues

1. **Compilation Errors**: Ensure all required dependencies are included in your POM files
2. **Missing Imports**: Check that all used Java types are properly imported
3. **Configuration Validation**: Validate your YAML configuration against the schema

### Validation

The template generator validates your configuration against a comprehensive JSON schema that ensures:

- Required fields are present
- Type compatibility between steps
- Valid Java and Protobuf type mappings
- Proper cardinality definitions

## Best Practices

### Configuration Organization

- Use descriptive names for steps and fields
- Organize related steps together in logical groups
- Document complex business logic in comments

### Generated Code Maintenance

- Preserve the generated structure and annotations
- Implement business logic in the designated `apply()` methods
- Add custom dependencies to the appropriate module POMs

### Version Control

- Commit the generated application to version control
- Exclude build artifacts and temporary files
- Document any customizations made after generation

## Next Steps

After generating and customizing your application:

- [Backend Services](/versions/v0.9.2/guide/backend-services): Learn more about implementing pipeline steps
- [Orchestrator Services](/versions/v0.9.2/guide/orchestrator-services): Understand orchestrator service configuration
- [Error Handling & DLQ](/versions/v0.9.2/guide/error-handling): Implement robust error handling
- [Observability](/versions/v0.9.2/guide/observability): Monitor and observe your pipeline applications
- [Testing](/versions/v0.9.2/guide/testing): Write tests for your pipeline components