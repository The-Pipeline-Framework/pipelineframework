---
search: false
---

# Handling File Operations with Generated REST Resources

This guide explains how to handle file operations such as downloads and uploads when using the auto-generated REST resources in The Pipeline Framework.

## Overview

The Pipeline Framework automatically generates REST adapters when you use the `restEnabled = true` parameter in your `@PipelineStep` annotation. However, the generated resources focus on standard DTO processing and may not handle specialized operations like file downloads directly.

## Generated REST Resources vs. Manual Resources

### Generated REST Resources
When you use:
```java
@PipelineStep(
    inputType = InputType.class,
    outputType = OutputFileType.class,
    restEnabled = true
)
public class ProcessFileReactiveService implements ReactiveStreamingClientService<InputType, OutputFileType> {
    // ...
}
```

The framework generates a resource like:
```java
@Path("/api/v1/process-file-reactive")
public class ProcessFileResource {
    // Standard process method for DTOs
    @POST
    @Path("/process")
    public Uni<OutputFileTypeDto> process(Multi<InputTypeDto> inputDtos) {
        // Implementation using mappers and service
    }
    
    // ServerExceptionMapper for error handling
    @ServerExceptionMapper
    public RestResponse handleRuntimeException(RuntimeException ex) {
        return RestResponse.status(Response.Status.BAD_REQUEST, "Error processing request: " + ex.getMessage());
    }
}
```

### Manual Resources for File Operations
For specialized operations like file downloads, you might need to complement the generated resource with custom endpoints:

```java
@Path("/api/v1/process-file-reactive")
public class ProcessFileRestResource {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessFileRestResource.class);
    
    @Inject
    ProcessFileReactiveService domainService;
    
    @POST
    @Path("/process-download")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Uni<Response> processAndDownload(List<InputTypeDto> request) {
        // Convert DTOs to domain objects
        Multi<InputType> domainInput = Multi.createFrom().iterable(request)
            .onItem().transform(inputTypeMapper::fromDto);
        
        // Process data using the same service
        Uni<OutputFileType> domainResult = domainService.process(domainInput);
        
        return domainResult
            .onItem().transformToUni(file -> createFileDownloadResponse(file));
    }
    
    private Uni<Response> createFileDownloadResponse(OutputFileType file) {
        java.nio.file.Path filePath = file.getFilepath();
        String fileName = filePath.getFileName().toString();
        
        StreamingOutput output = outputStream -> {
            try {
                Files.copy(filePath, outputStream);
            } finally {
                // Clean up the temporary file after streaming
                try {
                    Files.deleteIfExists(filePath);
                } catch (IOException e) {
                    LOG.warn("Failed to delete temporary file: {}", filePath, e);
                }
            }
        };
        
        return Uni.createFrom().item(
            Response.ok(output)
                .header("Content-Disposition", "attachment; filename=\"" + fileName + "\"")
                .header("Content-Type", "text/csv")
                .build());
    }
}
```

## Recommended Approaches

### Option 1: Hybrid Approach (Recommended)
Keep both the generated resource for standard operations and create custom resources for file operations:

1. **Enable `restEnabled = true`** in your `@PipelineStep` annotation for standard DTO processing
2. **Create a separate resource class** for specialized file operations
3. **Re-use the same service instance** to avoid duplication of business logic

```java
// In your service class - generate standard REST endpoints
@PipelineStep(
    inputType = InputType.class,
    outputType = OutputFileType.class,
    inboundMapper = InputTypeMapper.class,
    outboundMapper = OutputFileTypeMapper.class,
    restEnabled = true  // This will generate the standard resource
)
public class ProcessFileReactiveService implements ReactiveStreamingClientService<InputType, OutputFileType> {
    // Your service business logic
}

// Separate resource for file download functionality
@Path("/api/v1/custom-file-processing")
public class CustomFileProcessingResource {
    
    @Inject
    ProcessFileReactiveService domainService;
    
    @POST
    @Path("/download")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Uni<Response> downloadFile(List<PaymentOutputDto> request) {
        // Use the same domain service for processing
        // Add file download specific logic
    }
}
```

### Option 2: Custom Annotation Configuration
For common patterns, you can implement custom handling by keeping the generated functionality but extending it:

```java
@ApplicationScoped
public class FileDownloadService {
    
    @Inject
    ProcessFileReactiveService domainService;

    // Generic file download endpoint that can work with multiple service types
    public <T, S> Uni<Response> createDownloadResponse(
        Multi<T> inputs, 
        Function<Multi<T>, Uni<S>> processingFunction,
        Function<S, Response> fileResponseFunction) {
        
        return processingFunction.apply(inputs)
            .onItem().transform(fileResponseFunction);
    }
}
```

## Key Considerations

### 1. Separation of Concerns
- Generated resources handle standard DTO → Domain → Service processing
- Custom resources handle specialized operations like file I/O, complex streaming, etc.

### 2. Consistent Error Handling
- Generated resources include `@ServerExceptionMapper` for runtime exceptions
- Ensure custom resources use similar error handling patterns

### 3. Resource Management
- Custom file operations should properly manage resources (files, streams, etc.)
- Consider file cleanup, permissions, and potential disk space issues

### 4. Testing Strategy
- Generated resources come with basic functionality that's automatically tested
- Custom resources for file operations will need additional test coverage

## Best Practices

1. **Start with Generated Resources**: Use the auto-generated REST endpoints for standard operations
2. **Add Custom Resources for Specialized Needs**: Implement custom resources when you need file handling, streaming, or other specialized operations
3. **Share Service Logic**: Re-use the same service instances to avoid duplicating business logic
4. **Maintain Consistency**: Follow similar patterns for error handling and response formats
5. **Document Differences**: Clearly document which endpoints are auto-generated vs. custom

## Example Implementation

For a complete implementation, see the reference implementation where manual resources handle file download operations while the framework generates standard DTO processing endpoints.

This approach provides the best of both worlds: automatic generation for standard operations and custom handling for specialized requirements.