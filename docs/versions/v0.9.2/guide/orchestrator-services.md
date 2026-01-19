---
search: false
---

# Creating Orchestrator Services

This guide explains how orchestrator services work in The Pipeline Framework and how they are automatically generated when you use the template generator to create pipeline applications.

<Callout type="tip" title="Visual Orchestrator Configuration">
Use the Canvas designer at <a href="https://app.pipelineframework.org" target="_blank">https://app.pipelineframework.org</a> to visually configure your orchestrator services. The Canvas allows you to define the complete pipeline flow, including input sources, step connections, and output handlers, without writing complex orchestration code.
</Callout>

## Overview

Orchestrator services are responsible for:
1. Initiating the pipeline execution
2. Providing input data to the pipeline
3. Coordinating the flow between pipeline steps
4. Handling the final output of the pipeline

When you use the template generator to create a pipeline application, it automatically generates a complete orchestrator service with:
- A CLI application class that implements `QuarkusApplication`
- Proper configuration in `application.properties`
- Test configuration for integration testing
- Integration with the framework's pipeline execution engine

The Pipeline Framework automatically generates the core pipeline execution logic when backend services are annotated with `@PipelineStep`, leaving orchestrator services to focus on input provisioning and output handling.

## Generated Orchestrator Service Structure

When the template generator creates an application, it generates an orchestrator service with the following structure:

```text
orchestrator-svc/
├── pom.xml                           # Service POM with framework dependencies
├── src/main/java/
│   └── com/example/app/orchestrator/
│       └── OrchestratorApplication.java  # Main CLI application class with input provisioning stub
└── src/main/resources/
    └── application.properties            # Service configuration
```

### OrchestratorApplication.java

The generated OrchestratorApplication.java is a Quarkus application that uses Picocli for command-line argument parsing. It's designed to work with multiple input configuration methods:

#### Input Configuration Options

The application supports multiple ways to specify the input:

1. **Command-line argument**:
   ```bash
   java -jar app.jar -i /path/to/input
   ```

2. **Environment variable**:
   ```bash
   PIPELINE_INPUT=/path/to/input java -jar app.jar
   ```

3. **Quarkus configuration property** (especially useful for dev mode):
   ```properties
   # In application.properties or application-dev.properties
   quarkus.pipeline.input=/path/to/input
   ```
   
4. **System property**:
   ```bash
   java -Dquarkus.pipeline.input=/path/to/input -jar app.jar
   ```

The application checks for these options in the following priority:
1. Command-line argument (`-i` or `--input`) - highest priority
2. Environment variable (`PIPELINE_INPUT`) - used when command-line argument is not provided

For dev mode (quarkus:dev) with environment variables, make sure the environment variable is properly set in your run configuration.

The generated orchestrator application implements `QuarkusApplication` and includes a `getInputMulti()` method stub that needs to be implemented by the user. This method is responsible for provisioning input data to the pipeline:

```java
// orchestrator-svc/src/main/java/com/example/app/orchestrator/OrchestratorApplication.java
import io.quarkus.runtime.QuarkusApplication;
import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import org.pipelineframework.PipelineExecutionService;
import com.example.app.common.domain.CustomerInput;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "orchestrator", mixinStandardHelpOptions = true, version = "1.0.0",
         description = "Sample Pipeline App Orchestrator Service")
@Dependent
public class OrchestratorApplication implements QuarkusApplication {

    @Option(names = {"-i", "--input"}, description = "Input value for the pipeline", required = true)
    String input;

    @Inject
    PipelineExecutionService pipelineExecutionService;

    public static void main(String[] args) {
        io.quarkus.runtime.Quarkus.run(OrchestratorApplication.class, args);
    }

    @Override
    public int run(String... args) {
        CommandLine cmd = new CommandLine(this);
        cmd.parseArgs(args);

        if (input != null) {
            executePipelineWithInput(input);
            return 0; // Success exit code
        } else {
            System.err.println("Input parameter is required");
            return 1; // Error exit code
        }
    }

    // Execute the pipeline when arguments are properly parsed
    private void executePipelineWithInput(String input) {
        Multi<CustomerInput> inputMulti = getInputMulti(input);

        // Execute the pipeline with the processed input using injected service
        pipelineExecutionService.executePipeline(inputMulti)
            .collect().asList()
            .await().indefinitely();

        System.out.println("Pipeline execution completed");
    }

    // This method needs to be implemented by the user after template generation
    // based on their specific input type and requirements
    private static Multi<CustomerInput> getInputMulti(String input) {
        // TODO: User needs to implement this method after template generation
        // Create and return appropriate Multi based on the input and first step requirements
        // For example:
        // CustomerInput inputItem = new CustomerInput();
        // inputItem.setField(input);
        // return Multi.createFrom().item(inputItem);
        
        throw new UnsupportedOperationException("Method getInputMulti needs to be implemented by user after template generation");
    }
}
```

**Important**: After template generation, you must implement the `getInputMulti()` method to define how your application provisions input data to the pipeline. This method should parse your input parameters and create the appropriate `Multi` stream of objects required by your first pipeline step.

### Application Properties

The generated orchestrator includes comprehensive configuration:

```properties
# orchestrator-svc/src/main/resources/application.properties
quarkus.package.main-class=com.example.app.orchestrator.OrchestratorApplication

# Pipeline Configuration
pipeline.runtime.retry-limit=10
pipeline.runtime.retry-wait-ms=500
pipeline.runtime.debug=false
pipeline.runtime.recover-on-failure=false
pipeline.runtime.run-with-virtual-threads=false
pipeline.runtime.auto-persist=true
pipeline.runtime.max-backoff=30000
pipeline.runtime.jitter=false

# gRPC client configurations for each service
quarkus.grpc.clients.processCustomer.host=process-customer-svc
quarkus.grpc.clients.processCustomer.port=8444
quarkus.grpc.clients.processCustomer.plain-text=false
quarkus.grpc.clients.processCustomer.use-quarkus-grpc-client=true
quarkus.grpc.clients.processCustomer.tls.enabled=true

# ... additional service configurations
```

## Customizing Generated Orchestrator Services

While the template generator creates a complete orchestrator service, you can customize it for your specific needs:

### 1. Implement Input Provisioning

The template generates a `getInputMulti()` method stub that you must implement to provision inputs to your pipeline. This method converts your command-line input parameters into the appropriate `Multi` stream:

```java
// orchestrator-svc/src/main/java/com/example/app/orchestrator/OrchestratorApplication.java
@Dependent
public class OrchestratorApplication implements QuarkusApplication {

    // ... other code ...

    // After template generation, implement this method to provision inputs:
    private static Multi<CustomerInput> getInputMulti(String input) {
        // Example implementation:
        // 1. Parse input string for file paths, database queries, or other input sources
        // 2. Convert to the appropriate domain objects for your pipeline
        // 3. Return a Multi stream of these objects
        
        // For example, if your input is a CSV file path:
        List<CustomerInput> inputList = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(input))) {
            // Process CSV and create CustomerInput objects
            // inputList.add(...);
        } catch (IOException e) {
            throw new RuntimeException("Error reading input file", e);
        }
        
        return Multi.createFrom().iterable(inputList);
    }
}
```

### 2. Customize OrchestratorApplication

Add custom logic to the generated `OrchestratorApplication`. You can modify command-line options or add additional processing:

```java
@Command(name = "orchestrator", mixinStandardHelpOptions = true, version = "1.0.0",
         description = "My Custom Pipeline App Orchestrator Service")
@Dependent
public class OrchestratorApplication implements QuarkusApplication {

    @Option(names = {"-i", "--input"}, description = "Input value for the pipeline", required = true)
    String input;
    
    // Add custom command-line options
    @Option(names = {"--custom-option"},
            description = "A custom option for this orchestrator")
    String customOption;

    @Inject
    PipelineExecutionService pipelineExecutionService;
    
    @Inject
    CustomProcessingService customProcessingService; // Add your custom services as needed

    public static void main(String[] args) {
        io.quarkus.runtime.Quarkus.run(OrchestratorApplication.class, args);
    }

    @Override
    public int run(String... args) {
        CommandLine cmd = new CommandLine(this);
        cmd.parseArgs(args);

        if (input != null) {
            // Custom pre-processing logic
            customProcessingService.preProcess();
            
            executePipelineWithInput(input);
            
            // Custom post-processing logic
            customProcessingService.postProcess();
            
            return 0; // Success exit code
        } else {
            System.err.println("Input parameter is required");
            return 1; // Error exit code
        }
    }

    // Execute the pipeline when arguments are properly parsed
    private void executePipelineWithInput(String input) {
        Multi<CustomerInput> inputMulti = getInputMulti(input);

        // Execute the pipeline with the processed input using injected service
        pipelineExecutionService.executePipeline(inputMulti)
            .collect().asList()
            .await().indefinitely();

        System.out.println("Pipeline execution completed");
    }

    // Implement this method to provision inputs:
    private static Multi<CustomerInput> getInputMulti(String input) {
        // Your implementation to convert input to Multi<CustomerInput>
        throw new UnsupportedOperationException("Method needs to be implemented");
    }
}
```

## Manual Orchestrator Service Creation

<Callout type="info" title="Advanced Users Only">
The following section describes how to manually create orchestrator services for advanced users who need capabilities beyond what the template generator provides. For most use cases, we recommend using the template generator.
</Callout>

### 1. Implement Orchestrator Application

Manual creation is useful when you need:

- **Custom base classes or alternative frameworks** - Extending from custom base classes instead of using the generated structure
- **Different CLI frameworks** - Using alternative command-line parsing libraries instead of Picocli
- **Custom integration requirements** - Integration with existing systems that require different initialization patterns
- **Fine-grained control** - Full control over the application lifecycle and pipeline execution flow

The manual approach allows you to create a custom orchestrator by implementing the QuarkusApplication interface with direct integration to the pipeline execution framework:

```java
// orchestrator-svc/src/main/java/com/example/app/orchestrator/CustomOrchestratorApplication.java
@Command(name = "custom-orchestrator", mixinStandardHelpOptions = true, version = "1.0.0",
         description = "Custom orchestrator with specialized requirements")
@Dependent
public class CustomOrchestratorApplication implements QuarkusApplication {

    @Option(names = {"-i", "--input"}, description = "Input source for the pipeline", required = true)
    String inputSource;

    @Inject
    PipelineExecutionService pipelineExecutionService;
    
    // Add custom dependencies as needed
    @Inject
    CustomMetricsService metricsService;

    public static void main(String[] args) {
        io.quarkus.runtime.Quarkus.run(CustomOrchestratorApplication.class, args);
    }

    @Override
    public int run(String... args) {
        // Custom initialization logic can be added here before pipeline execution
        metricsService.initialize();
        
        CommandLine cmd = new CommandLine(this);
        cmd.parseArgs(args);

        if (inputSource != null) {
            // Custom pre-processing logic
            metricsService.startTimer();
            
            executePipelineWithInput(inputSource);
            
            // Custom post-processing logic
            metricsService.reportMetrics();
            
            return 0; // Success exit code
        } else {
            System.err.println("Input source parameter is required");
            return 1; // Error exit code
        }
    }

    // Execute the pipeline when arguments are properly parsed
    private void executePipelineWithInput(String inputSource) {
        Multi<CustomerInput> inputMulti = getInputMulti(inputSource);

        // Execute the pipeline with the processed input using injected service
        pipelineExecutionService.executePipeline(inputMulti)
            .collect().asList()
            .await().indefinitely();

        System.out.println("Pipeline execution completed");
    }

    // Implementation to convert your input into Multi stream
    private static Multi<CustomerInput> getInputMulti(String inputSource) {
        // Custom input processing logic specific to your requirements
        // For example, reading from different sources: database, message queues, files, etc.
        throw new UnsupportedOperationException("Method getInputMulti needs to be implemented with custom logic");
    }
}
```

**When to use manual creation vs. template generator:**

- **Use the template generator** when you want a complete, standardized pipeline application with all the default functionality and observability features
- **Use manual creation** when you need custom behaviors that are not supported by the template generator, such as custom dependency injection, non-standard initialization, or third-party integration requirements