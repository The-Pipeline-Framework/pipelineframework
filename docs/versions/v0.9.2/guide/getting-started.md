---
search: false
---

# Getting Started

This guide walks you through the process of quickly setting up and using The Pipeline Framework to create your first pipeline application using the template generator - the fastest and easiest way to get started!

<Callout type="tip" title="Immutable Architecture">
The Pipeline Framework follows an immutable architecture where no database updates occur during pipeline execution - only appends/preserves. This ensures complete data integrity and provides a complete audit trail of all transformations.
</Callout>

<Callout type="tip" title="Prerequisites">
Before you begin, ensure you have IntelliJ IDEA or VS Code running on Java 21+ and Maven 3.8+ installed on your system.
</Callout>

<Callout type="tip" title="Visual Pipeline Designer Alternative">
Instead of using the template generator, you can use the visual Canvas designer at <a href="https://app.pipelineframework.org" target="_blank">https://app.pipelineframework.org</a> to create and configure your pipeline applications. The Canvas provides an intuitive drag-and-drop interface for defining pipeline steps and their connections.
</Callout>

## Quick Start with Template Generator

The fastest way to get started with The Pipeline Framework is by using the template generator, which creates a complete, ready-to-run pipeline application in seconds.

### 1. Install the Template Generator

Install the latest template generator from npm:

```bash
npm install -g pipeline-template-generator
```

### 2. Generate a Sample Configuration

Generate a sample YAML configuration file to understand the structure:

```bash
pipeline-template-generator --generate-config
```

This creates `sample-pipeline-config.yaml` with a complete example configuration that you can customize.

### 3. Customize Your Pipeline Configuration

Edit the generated YAML file to define your pipeline. Here's an example configuration:

```yaml
---
appName: "My Pipeline App"
basePackage: "com.example.mypipeline"
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
  - name: "email"
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
  - name: "processedAt"
    type: "String"
    protoType: "string"
- name: "Validate Order"
  cardinality: "ONE_TO_ONE"
  inputTypeName: "CustomerOutput"  # Automatically uses output of previous step
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
pipeline-template-generator --config my-pipeline-config.yaml --output ./my-pipeline-app
```

### 5. Build and Run Your Application

Navigate to your generated application directory and build it:

```bash
cd my-pipeline-app
./mvnw clean compile
```

Your application is now ready to run! The template generator creates a complete Maven multi-module project with all necessary components, including:

- Parent POM with all modules properly configured
- Common module with domain entities, DTOs, and mappers
- Individual service modules for each pipeline step
- Orchestrator module with CLI application and configuration
- Test configuration files for different environments
- Integration tests with Testcontainers
- Maven configuration for builds and testing

## Running Your Generated Application

Run your application using Quarkus Dev Mode in your IDE:

```bash
# For command-line development (if needed):
./mvnw quarkus:dev

# For fastest development feedback, use Quarkus Dev Mode in your IDE

# Production mode
./mvnw clean package
java -jar target/my-app-runner.jar

# Running Integration Tests (in service modules)
./mvnw verify -Pit -Denable.container.build=true

# Native mode
./mvnw clean package -Pnative
./target/my-app-runner
```

## Next Steps

Congratulations! You've successfully created your first pipeline application. Now explore these advanced topics:

- [Application Structure](/versions/v0.9.2/guide/application-structure): Learn how the generated application is structured
- [Backend Services](/versions/v0.9.2/guide/backend-services): Understand the backend services that implement your pipeline steps
- [Orchestrator Services](/versions/v0.9.2/guide/orchestrator-services): Learn about the orchestrator service that coordinates your pipeline
- [Error Handling & DLQ](/versions/v0.9.2/guide/error-handling): Implement robust error handling in your pipeline
- [Observability](/versions/v0.9.2/guide/observability): Monitor and observe your pipeline applications
- [Manual Configuration Reference](#manual-configuration-reference): For advanced users who want to manually configure pipeline steps

## Manual Configuration Reference

<Callout type="info" title="Advanced Users Only">
The following sections describe manual configuration approaches for advanced users who need fine-grained control over their pipeline implementations. For most use cases, we recommend using the template generator or visual Canvas designer.
</Callout>

### Adding the Framework to Your Project

Add the following dependency to your `pom.xml`. Both runtime and deployment components are bundled in a single dependency:

```xml
<dependency>
  <groupId>org.pipelineframework</groupId>
  <artifactId>pipelineframework</artifactId>
  <version>LATEST_VERSION</version>
</dependency>
```

### Optional Dependencies for Persistence

If you plan to use the `autoPersist = true` feature for automatic entity persistence, you'll also need to include database dependencies:

```xml
<dependency>
  <groupId>io.quarkus</groupId>
  <artifactId>quarkus-reactive-pg-client</artifactId>
</dependency>
<dependency>
  <groupId>io.quarkus</groupId>
  <artifactId>quarkus-hibernate-reactive-panache</artifactId>
</dependency>
```

If you don't need persistence functionality, you can omit these dependencies and set `autoPersist = false` on all your pipeline steps.

### Basic Configuration

#### 1. Define Your Protocol Buffer (Proto) Definitions

Create `.proto` files to define your data structures and service contracts. These definitions establish the communication protocol between pipeline steps:

```protobuf
syntax = "proto3";

option java_package = "com.example.app.grpc";

message CustomerInput {
  string id = 1;
  string name = 2;
  string email = 3;
}

message CustomerOutput {
  string id = 1;
  string name = 2;
  string status = 3;
  string processedAt = 4;
}

service ProcessCustomerService {
  rpc remoteProcess(CustomerInput) returns (CustomerOutput);
}
```

#### 2. Create Your Service Class

Create a class that implements one of the step interfaces. Use the `inputGrpcType` and `outputGrpcType` parameters to explicitly specify the gRPC types that should be used in the generated client step interface:

```java
@PipelineStep(
   order = 1,
   inputType = CustomerInput.class,
   outputType = CustomerOutput.class,
   inputGrpcType = CustomerInput.class,
   outputGrpcType = CustomerOutput.class,
   stepType = StepOneToOne.class,
   grpcStub = MutinyProcessCustomerServiceGrpc.MutinyProcessCustomerServiceStub.class,
   grpcImpl = MutinyProcessCustomerServiceGrpc.ProcessCustomerServiceImplBase.class,
   inboundMapper = CustomerInputMapper.class,
   outboundMapper = CustomerOutputMapper.class,
   grpcClient = "process-customer"
)
@ApplicationScoped
public class ProcessCustomerService implements StepOneToOne<CustomerInput, CustomerOutput> {
    
    @Override
    public Uni<CustomerOutput> applyOneToOne(CustomerInput customerInput) {
        // Your implementation here
        return Uni.createFrom().item(/* processed customer output */);
    }
}
```

#### 3. Create Your Mapper Classes

Create mapper classes for converting between gRPC, DTO, and domain types using MapStruct. Mappers implement the `Mapper` interface with three generic types (gRPC, DTO, Domain):

```java
@Mapper(
    componentModel = "jakarta",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN
)
public interface CsvPaymentsInputFileMapper extends Mapper<CsvPaymentsInputFile, CsvPaymentsInputFileDto, CsvPaymentsInputFile> {

    CsvPaymentsInputFileMapper INSTANCE = Mappers.getMapper(CsvPaymentsInputFileMapper.class);

    // Domain ↔ DTO
    @Override
    CsvPaymentsInputFileDto toDto(CsvPaymentsInputFile domain);

    @Override
    CsvPaymentsInputFile fromDto(CsvPaymentsInputFileDto dto);

    // DTO ↔ gRPC
    @Override
    @Mapping(target = "id", qualifiedByName = "uuidToString")
    @Mapping(target = "filepath", qualifiedByName = "pathToString")
    @Mapping(target = "csvFolderPath", qualifiedByName = "pathToString")
    CsvPaymentsInputFile toGrpc(CsvPaymentsInputFileDto dto);

    @Override
    @Mapping(target = "id", qualifiedByName = "stringToUUID")
    @Mapping(target = "filepath", qualifiedByName = "stringToPath")
    @Mapping(target = "csvFolderPath", qualifiedByName = "stringToPath")
    CsvPaymentsInputFileDto fromGrpc(CsvPaymentsInputFile grpc);
}
```

The MapStruct annotation processor automatically generates the implementation classes. You only need to define the interface methods with appropriate `@Mapping` annotations for complex transformations.

### Build Process

When you build your application, the Pipeline Framework's annotation processor:

1. Discovers all `@PipelineStep` annotated services
2. Generates gRPC and REST adapters for each service
3. Creates a complete pipeline application that orchestrates all steps
4. Registers all generated components with the dependency injection container

### Running Your Application

Run your application using the standard Quarkus commands:

```bash
# Development mode
mvn quarkus:dev

# Production mode
mvn clean package
java -jar target/my-app-runner.jar

# Native mode
mvn clean package -Pnative
./target/my-app-runner
```

### Testing Your Application

The framework provides testing utilities to help you test your pipeline steps:

```java
@QuarkusTest
class ProcessCustomerServiceTest {

    @InjectMock ProcessCustomerService processCustomerService;
    
    @Test
    void testProcessCustomer() {
        CustomerInput testInput = createTestCustomerInput();
        
        Uni<CustomerOutput> result = processCustomerService.applyOneToOne(testInput);
        
        UniAssertSubscriber<CustomerOutput> subscriber = 
            result.subscribe().withSubscriber(UniAssertSubscriber.create());
            
        CustomerOutput output = subscriber.awaitItem().getItem();
        assertNotNull(output);
        assertEquals("PROCESSED", output.getStatus());
    }
}
```