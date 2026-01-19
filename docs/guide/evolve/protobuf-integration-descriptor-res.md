# Protobuf Integration and Descriptor-Driven Resolution

The Pipeline Framework now supports descriptor-driven resolution for gRPC types, which provides accurate and reliable type resolution based on compiled protobuf descriptors. This replaces the previous fallback mechanism with a robust solution that relies on actual protobuf compilation outputs.

## Setup Requirements

To use the descriptor-driven resolution, you need to ensure that:

1. Your protobuf files are compiled during the build process before annotation processing
2. The generated FileDescriptorSet files (typically with `.desc` or `.dsc` extensions) are available in the build directory
3. The annotation processor can locate these descriptor files
4. Quarkus (or your protobuf build) is configured to generate descriptor sets

The build will fail for gRPC services if descriptor sets are not generated. Use the descriptor-set configuration
supported by your Quarkus/protobuf build, or supply `protobuf.descriptor.path`/`protobuf.descriptor.file` to the
annotation processor.

## Template-Driven Proto Generation

When `pipeline-config.yaml` is present, you can generate step and orchestrator `.proto` files at build time
before compilation. The default template output wires the `PipelineProtoGenerator` into the `common` module's
`generate-sources` phase so protobuf definitions stay in sync with the pipeline template used by Canvas.
Generated protos land under `target/generated-sources/proto`, and Quarkus gRPC codegen is configured to
include that directory.

This keeps descriptor sets consistent with the pipeline model while preserving descriptor-driven resolution
inside the annotation processor.

## Default Descriptor Locations

If no annotation processor options are provided, the framework searches for:
- `target/generated-sources/grpc/descriptor_set.dsc` in the current module
- The same path in a sibling `common` module
- The same path in other sibling modules under the multimodule root

## Configuration Options

You can customize the descriptor file location using annotation processor options (these override defaults):

### Maven Configuration
```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-compiler-plugin</artifactId>
    <configuration>
        <annotationProcessorPaths>
            <!-- your annotation processors -->
        </annotationProcessorPaths>
        <compilerArgs>
            <arg>-Aprotobuf.descriptor.path=/path/to/descriptors</arg>
            <!-- OR for a specific file -->
            <arg>-Aprotobuf.descriptor.file=/path/to/specific.descriptor</arg>
        </compilerArgs>
    </configuration>
</plugin>
```

## Proto ↔ Pipeline Compatibility Contract

### 1. Overview: Who Owns What

The Pipeline Framework maintains a clear ownership model between your pipeline definition and gRPC `.proto` files:

- **Pipeline model is authoritative**: Your `@PipelineStep` annotations define the canonical contract
- **`.proto` is a derived artifact**: Generated from your pipeline model but can be manually edited
- **Manual edits are allowed, but constrained**: You can modify `.proto` files, but they must remain compatible with the pipeline model

This ensures that your business logic remains the source of truth while allowing flexibility in your gRPC contracts.

### 2. What TPF Validates at Build Time

At build time, The Pipeline Framework validates these aspects of your `.proto` files:

- **Service names**: Must start with `Process`
- **RPC method names**: Must be named `remoteProcess`
- **Streaming semantics**: Must align with the step type
- **Multiple methods**: Additional methods are allowed but trigger warnings

Any mismatches will cause build failures, preventing runtime issues.

### 3. Service & RPC Conventions

The Pipeline Framework follows strict naming conventions for gRPC services:

- **Service Name**: Must start with `Process` (the rest is derived from your service class name)
- **Method Name**: Exactly `remoteProcess` - this is the only method the framework recognizes
- **Single Method**: Exactly one method is expected in each service (the `remoteProcess` method)
- **Multiple Methods**: While technically allowed, having additional methods beyond `remoteProcess` is strongly discouraged and will trigger warnings

For example, if your pipeline step is named `ValidatePayment`, the expected service name is `ProcessValidatePaymentService` with a method named `remoteProcess`.

### 4. Streaming Semantics Mapping

The framework maps pipeline step types to specific gRPC method signatures:

| Pipeline Step Type | gRPC Method Shape                                             |
|--------------------|---------------------------------------------------------------|
| OneToOne           | `rpc remoteProcess(Request) returns (Response)`               |
| OneToMany          | `rpc remoteProcess(Request) returns (stream Response)`        |
| ManyToOne          | `rpc remoteProcess(stream Request) returns (Response)`        |
| ManyToMany         | `rpc remoteProcess(stream Request) returns (stream Response)` |
| SideEffect         | `rpc remoteProcess(Request) returns (Request)`                |

This mapping ensures that the streaming behavior of your gRPC service matches the processing semantics of your pipeline step. This alignment is critical for performance and correctness.

### 5. Message Compatibility Rules

Message-level compatibility checks (field names/types, SIDE_EFFECT input/output identity) are not currently enforced by the framework. The pipeline relies on the compiled descriptors for gRPC type resolution, but it does not validate domain-to-proto field compatibility at build time.

### 6. What Happens When Things Don't Match

The framework provides clear error messages when descriptor files are missing or when there are mismatches between the protobuf definitions and the `@PipelineStep` annotations, helping you quickly identify and resolve integration issues.

When your `.proto` files don't match your pipeline model, you'll see build-time errors:

- **Build-time errors**: Mismatches cause compilation to fail with descriptive error messages
- **Warnings**: Additional methods in services trigger warnings but allow compilation
- **Typical failure scenarios**: Incorrect service names, wrong method names, mismatched streaming semantics, incompatible message types

Example error message:
```text
[ERROR] Pipeline step 'ValidatePayment' expects service 'ProcessValidatePaymentService' with method 'remoteProcess',
but found service 'ValidatePaymentService' with method 'validate'. 
Update your .proto file to match the expected naming convention.
```
### 7. Troubleshooting

If you encounter errors about missing descriptor files:
1. Verify that protobuf compilation is happening before annotation processing
2. Check that the descriptor files are generated in the expected location
3. If using custom paths, ensure the `protobuf.descriptor.path` or `protobuf.descriptor.file` options are correctly set
4. Make sure the service names in your protobuf files start with `Process` and match the service class name
5. If you get dependency resolution errors (like "X is not defined"), ensure all imported .proto files are included in the descriptor set

### 8. Best Practices

Follow these guidelines to maintain compatibility:

- **Don't rename remoteProcess**: Keep the method name exactly as `remoteProcess`
- **Don't change streaming shape casually**: Changing between streaming and unary requires updating both your code and proto definitions
- **Prefer regenerating proto via tooling**: Use the framework's generation tools when possible to ensure compatibility
- **Treat .proto edits as contract changes**: Any changes to `.proto` files should be reviewed carefully as they affect the interface between services

### 9. What TPF Does Not Do

The Pipeline Framework explicitly does not:

- **No proto regeneration in the annotation processor**: The processor never rewrites `.proto` files. If you opt into
  template-driven generation (via `PipelineProtoGenerator` in `generate-sources`), that happens before compilation.
- **No runtime validation**: All validation occurs at build time, not at runtime
- **No reflection-based discovery**: The framework uses compiled descriptors, not runtime inspection of annotations

This deterministic approach ensures that your pipeline will behave consistently and predictably.
