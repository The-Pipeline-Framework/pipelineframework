# Build Integration

## Build Process Integration

### Maven Configuration

The pipeline framework integrates with the Maven build process. Both runtime and deployment components are bundled in a single dependency:

```xml
<!-- pom.xml dependencies -->
<dependency>
    <groupId>org.pipelineframework</groupId>
    <artifactId>pipelineframework</artifactId>
</dependency>
```

### Annotation Processor Execution

The annotation processor runs during the `compile` phase:

```bash
# During mvn compile
[INFO] --- quarkus:3.28.0.CR1:generate-code (default) @ service-module ---
[INFO] [org.pipelineframework.processor.PipelineStepProcessor] Loading pipeline.yaml
[INFO] [org.pipelineframework.processor.PipelineStepProcessor] Resolving YAML-declared services and operators
[INFO] [org.pipelineframework.processor.PipelineStepProcessor] Generated ProcessPaymentServiceGrpcService
[INFO] [org.pipelineframework.processor.PipelineStepProcessor] Generated ProcessPaymentGrpcClientStep
[INFO] [org.pipelineframework.processor.PipelineStepProcessor] Generated SendPaymentServiceGrpcService
[INFO] [org.pipelineframework.processor.PipelineStepProcessor] Generated SendPaymentGrpcClientStep
[INFO] [org.pipelineframework.processor.PipelineStepProcessor] Generated ProcessAckPaymentServiceGrpcService
[INFO] [org.pipelineframework.processor.PipelineStepProcessor] Generated ProcessAckPaymentGrpcClientStep
[INFO] [org.pipelineframework.processor.PipelineStepProcessor] Generated step implementations and service adapters
```

Configure TPF-specific annotation processors on Maven's `default-compile` execution, not on the
compiler plugin as a whole. Plugin-level processor paths are inherited by `default-testCompile`,
which can run pipeline generation a second time against the incomplete test-compilation model.

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-compiler-plugin</artifactId>
    <executions>
        <execution>
            <id>default-compile</id>
            <configuration>
                <annotationProcessorPaths>
                    <path>
                        <groupId>org.pipelineframework</groupId>
                        <artifactId>pipelineframework-deployment</artifactId>
                        <version>${pipelineframework.version}</version>
                    </path>
                    <!-- Add connector/provider processors used by this application here. -->
                </annotationProcessorPaths>
                <compilerArgs>
                    <arg>-Apipeline.config=${project.basedir}/src/main/resources/pipeline.yaml</arg>
                </compilerArgs>
            </configuration>
        </execution>
    </executions>
</plugin>
```

Test-only annotation processors can still be configured separately. TPF pipeline compilation is a
main-source build step and should not be repeated during `testCompile`.

### Required gRPC Descriptor Set Generation

The annotation processor resolves gRPC bindings from a protobuf descriptor set. Configure your build to emit a
descriptor set (for example via Quarkus gRPC codegen) or pass `protobuf.descriptor.file`/`protobuf.descriptor.path`
to the annotation processor if you have a custom descriptor location.

## Customization Points

### Extending Generated Classes

While generated classes are typically not modified directly, you can extend them:

```java
// Custom extension of generated step
@ApplicationScoped
public class CustomProcessPaymentGrpcClientStep extends ProcessPaymentGrpcClientStep {

    @Override
    public Uni<PaymentStatus> applyOneToOne(PaymentRecord input) {
        // Add custom logic before/after calling super
        return super.applyOneToOne(input)
            .onItem().invoke(status -> {
                // Custom post-processing
                logPaymentStatus(status);
            });
    }

    private void logPaymentStatus(PaymentStatus status) {
        // Custom logging logic
    }
}
```

### Customizing Generation

Use configuration and transport settings instead of transport-specific annotation fields:

- Set `transport: GRPC` or `transport: REST` in `pipeline.yaml`.
- Override REST paths with `pipeline.rest.path.<ServiceName>` in `application.properties`.
