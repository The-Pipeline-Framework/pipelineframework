---
search: false
---

# Local Pipeline Steps

Local pipeline steps are pipeline steps that run within the same application process instead of as separate remote services. This approach is useful for services that don't need to run in a distributed environment and can be executed directly within the orchestrator process.

## Overview

Local steps differ from remote steps in that they:

- Run in the same JVM process as the orchestrator
- Don't require gRPC communication overhead
- Access services directly through method calls
- Provide better performance for operations that don't need distribution
- Are ideal for file system operations, local data processing, and other local tasks

## Defining Local Steps

To create a local step, annotate your service with `@PipelineStep` and set the `local` parameter to `true`:

```java
@PipelineStep(
    order = 1,
    inputType = String.class,
    outputType = InputCsvFileProcessingSvc.CsvPaymentsInputFile.class,
    stepType = StepOneToMany.class,
    local = true  // This marks the step as local
)
@ApplicationScoped
public class ProcessFolderService {
    
    public Stream<CsvPaymentsInputFile> process(String csvFolderPath) throws URISyntaxException {
        // Local business logic here
        // This method will be called directly by the generated step wrapper
    }
}
```

## When to Use Local Steps

Local steps are ideal for:

- **File system operations**: Reading from local directories, writing output files
- **Data validation**: Validating input data before passing to remote services
- **Transformation tasks**: Converting between data formats within the same process
- **Performance-critical operations**: When you want to avoid network latency
- **Development and testing**: Simplifying local development without remote services

## Comparison with Remote Steps

| Aspect | Local Steps | Remote Steps |
|--------|-------------|--------------|
| Communication | Direct method calls | gRPC/REST communication |
| Performance | Higher (no network overhead) | Lower (network overhead) |
| Scalability | Limited by single process | Can scale independently |
| Deployment | Deployed with orchestrator | Deployed separately |
| Fault Isolation | No isolation | Service-level isolation |

## Generated Infrastructure

When you use `local = true`, the framework generates:

- A step wrapper class that injects your service directly
- A step implementation that calls your service method directly
- Proper mapping between Mutiny types and your service methods
- All the same configuration and error handling as remote steps

The orchestrator treats local and remote steps identically from a pipeline perspective, maintaining consistency in the processing flow.