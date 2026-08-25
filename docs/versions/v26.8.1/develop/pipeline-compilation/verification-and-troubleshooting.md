---
search: false
---

# Verification and Troubleshooting

## Troubleshooting

### Common Issues

#### 1. Missing Dependencies

Ensure the required dependency is present. Both runtime and deployment components are bundled in a single dependency:

```xml
<dependency>
    <groupId>org.pipelineframework</groupId>
    <artifactId>pipelineframework</artifactId>
</dependency>
```

#### 2. Annotation Processing Not Running

Verify the processor is on the classpath:

```bash
# Check that the pipelineframework dependency is included
mvn dependency:tree | grep pipelineframework
```

#### 3. Generated Classes Not Found

Check the generated sources directory:

```bash
# List generated classes
find target/generated-sources -name "*.java" | grep -i pipeline
```

### Debugging Tips

#### Enable Detailed Logging

```properties
# application.properties
quarkus.log.category."org.pipelineframework".level=DEBUG
quarkus.log.category."org.pipelineframework.processor".level=TRACE
```

#### Verify Generated Classes

```bash
# Check that step classes were generated
find target/classes -name "*Step.class" | head -5
# Check that gRPC service classes were generated
find target/classes -name "*GrpcService.class" | head -5
```

#### Clean and Rebuild

```bash
# Clean build to force regeneration
mvn clean compile
```

## Best Practices

### Development Workflow

1. **Author Internal Step Contracts in YAML**: Define `service`, `cardinality`, `input`, `output`, and optional `inboundMapper` / `outboundMapper` in `pipeline.yaml`.
2. **Build Project**: Run `mvn compile` to trigger generation
3. **Verify Generation**: Check that generated classes are created and the service interface matches the YAML cardinality
4. **Test Integration**: Run integration tests to verify the pipeline works
5. **Deploy**: Deploy the complete application with generated components

### Maintenance

1. **Keep YAML and Service Signatures in Sync**: Update `pipeline.yaml` whenever an internal service interface changes
2. **Review Generated Code**: Periodically review generated code for correctness
3. **Monitor Build Logs**: Watch for generation warnings or errors
4. **Test Changes**: Thoroughly test after making changes to YAML-defined steps or service implementations

### Performance Considerations

1. **Minimize Regeneration**: Only rebuild when annotations change
2. **Optimize Mappers**: Ensure mappers are efficient
3. **Configure Retries**: Set appropriate retry limits and wait times
4. **Monitor Resource Usage**: Watch memory and CPU usage of generated components

The Pipeline Framework's annotation processing provides a powerful way to automatically generate pipeline infrastructure while maintaining type safety and reducing boilerplate code. By understanding how this process works, you can leverage its full potential while troubleshooting any issues that may arise.
