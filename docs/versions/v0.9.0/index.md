# The Pipeline Framework v0.9.2

A framework for building reactive pipeline processing systems.

## Annotation-Based Automatic Adapter Generation

This framework now supports annotation-based automatic generation of gRPC and REST adapters, which simplifies the development of pipeline steps by eliminating the disconnect between step configuration and adapter configuration.

### New Annotations

#### @PipelineStep

The `@PipelineStep` annotation is used to mark a class as a pipeline step. This annotation enables automatic generation of gRPC and REST adapters.

```java
@PipelineStep(
   inputType = MyInput.class,
   outputType = MyOutput.class,
   stepType = StepOneToOne.class,
   backendType = MyAdapter.class,
   inboundMapper = MyMapper.class,
   outboundMapper = MyMapper.class
)
```

#### Mapper Classes

Mapper classes implement the conversion interfaces using MapStruct to handle conversions between different representations of the same entity: domain, DTO, and gRPC formats.

```java
@Mapper(
    componentModel = "jakarta",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN
)
public interface MyMapper extends Mapper<MyGrpcType, MyDtoType, MyDomainType> {

    MyMapper INSTANCE = Mappers.getMapper(MyMapper.class);

    // Domain ↔ DTO
    @Override
    MyDtoType toDto(MyDomainType domain);

    @Override
    MyDomainType fromDto(MyDtoType dto);

    // DTO ↔ gRPC
    @Override
    MyGrpcType toGrpc(MyDtoType dto);

    @Override
    MyDtoType fromGrpc(MyGrpcType grpc);
}
```

The MapStruct annotation processor automatically generates the implementation classes. You only need to define the interface methods with appropriate `@Mapping` annotations for complex transformations.

### Benefits

1. **Reduced Boilerplate**: Developers no longer need to manually configure adapters
2. **Consistent Configuration**: Configuration is defined in one place (the step implementation)
3. **Improved Developer Experience**: Simpler, more intuitive API
4. **Reduced Errors**: Less manual configuration reduces the chance of errors
5. **Better Maintainability**: Configuration changes only need to be made in one place
6. **Architectural Integrity**: Maintains proper separation of concerns between orchestrator and service modules

### Module Structure

- `runtime`: Contains the annotations, mapper interfaces, and generic adapter classes
- `deployment`: Contains the Quarkus build processor that scans for annotations and generates adapter beans

### Usage

Developers only need to:

1. Annotate their service class with `@PipelineStep`
2. Create their mapper classes that implement the appropriate mapper interfaces
3. Create MapStruct-based mapper interfaces that extend the `Mapper<Grpc, Dto, Domain>` interface
4. Implement the service interface (`StepOneToOne`, etc.)

The framework automatically generates and registers the adapter beans at build time.
