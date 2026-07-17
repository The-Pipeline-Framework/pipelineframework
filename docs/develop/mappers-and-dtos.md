# Mappers and DTOs

This guide explains how to work with mappers and DTOs in The Pipeline Framework, following the patterns demonstrated in the CSV Payments reference implementation.

## Overview

The Pipeline Framework uses MapStruct-based mappers to convert between different object types during pipeline processing:

- **Domain Objects**: The main business objects used within your services
- **DTOs**: Data Transfer Objects for internal service processing
- **gRPC Objects**: Generated gRPC message types for communication

## MapStruct-Based Mappers

There are two different concepts named "Mapper" used together:

- **MapStruct Mapper**: the `@org.mapstruct.Mapper` annotation used to generate mapping code.
- **TPF Mapper Interface**: `org.pipelineframework.mapper.Mapper<Domain, External>`, which standardizes `fromExternal` and `toExternal` at one generated boundary.

For an internal service step, configure an `inboundMapper` for the service input boundary and an `outboundMapper` for the output boundary. They can use different external DTO or generated protobuf types.

```java
@Mapper(
    componentModel = "jakarta",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN
)
public interface PaymentRecordMapper extends Mapper<PaymentRecord, PaymentRecordDto> {

    PaymentRecordMapper INSTANCE = Mappers.getMapper(PaymentRecordMapper.class);

    @Override
    PaymentRecord fromExternal(PaymentRecordDto external);

    @Override
    PaymentRecordDto toExternal(PaymentRecord domain);
}
```

Use a second mapper with the same two-type interface when the step output has a different domain or external type. The two mapper declarations are explicit in YAML so TPF can validate both generated boundaries.

Logical type names in pipeline YAML determine the generated contract; Java bindings and application-owned mappers connect it to DTO and domain types. See the [Pipeline Template DSL](/develop/pipeline-template-dsl) for the type model and defaults.

## Working with DTOs (Optional)

In most cases, you do **not** need to work with DTOs inside your `ReactiveService`. The generated REST/gRPC adapters perform DTO ↔ domain conversion at the transport boundary, so your business logic stays domain-only.

Only reach for DTOs inside a service when there is a clear, local benefit (e.g., interacting with a legacy library or a DTO-first API). The CSV Payments example previously used DTOs in one service to set an ID before persistence; this is no longer necessary now that domain entities generate IDs on creation via `BaseEntity`.

If you still need to work with DTOs internally, use the mapper to convert between domain and DTO objects:

```java
@PipelineStep(
    // ... annotation configuration
)
@ApplicationScoped
public class ProcessPaymentService implements ReactiveService<PaymentRecord, PaymentStatus> {

    @Inject
    PaymentRecordMapper paymentRecordMapper;
    @Inject
    PaymentStatusMapper paymentStatusMapper;

    @Override
    public Uni<PaymentStatus> process(PaymentRecord paymentRecord) {
        // Convert domain to DTO for internal processing
        PaymentRecordDto dto = paymentRecordMapper.toDto(paymentRecord);

        // Process the DTO
        PaymentStatusDto statusDto = processPaymentDto(dto);

        // Convert DTO back to domain
        return Uni.createFrom().item(paymentStatusMapper.fromDto(statusDto));
    }

    private PaymentStatusDto processPaymentDto(PaymentRecordDto dto) {
        // Process the DTO and return a status DTO
        return PaymentStatusDto.builder()
            .paymentRecordId(dto.getId())
            .status("PROCESSED")
            .message("Payment record processed successfully")
            .build();
    }
}
```

## Common Converters (Optional Scaffold)

The template generator includes a `CommonConverters` class as a convenience starting point. It is **optional** and intended to be customized per project. In the CSV Payments example it includes conversions like `UUID`, `BigDecimal`, `Long`, `Currency`, and `Path`. In other examples (like Search) it includes list and atomic conversions. Treat this as scaffolding, not a framework requirement.

## Best Practices

1. **Use Boundary Mappers**: Create `Mapper<Domain, External>` implementations for each configured inbound or outbound boundary
2. **Leverage Common Converters**: If helpful, use (or adapt) a `CommonConverters` class for standard type mappings
3. **Clear Error Handling**: Add appropriate error handling for mapping failures
4. **Performance Considerations**: Be mindful of expensive mapping operations and cache where appropriate
5. **Validation**: Consider validating DTOs after mapping to ensure data integrity
