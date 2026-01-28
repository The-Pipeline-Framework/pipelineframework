---
search: false
---

# Mappers and DTOs

This guide explains how to work with mappers and DTOs in The Pipeline Framework, following the patterns demonstrated in the CSV Payments reference implementation.

## Overview

The Pipeline Framework uses MapStruct-based mappers to convert between different object types during pipeline processing:

- **Domain Objects**: The main business objects used within your services
- **DTOs**: Data Transfer Objects for internal service processing  
- **gRPC Objects**: Generated gRPC message types for communication

## MapStruct-Based Mappers

Create unified mappers using MapStruct that handle all conversions:

```java
@Mapper(
    componentModel = "jakarta",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN
)
public interface PaymentRecordMapper extends Mapper<PaymentRecordGrpc, PaymentRecordDto, PaymentRecord> {

    PaymentRecordMapper INSTANCE = Mappers.getMapper(PaymentRecordMapper.class);

    // Domain ↔ DTO
    @Override
    PaymentRecordDto toDto(PaymentRecord domain);

    @Override
    PaymentRecord fromDto(PaymentRecordDto dto);

    // DTO ↔ gRPC
    @Override
    @Mapping(target = "id", qualifiedByName = "uuidToString")
    @Mapping(target = "amount", qualifiedByName = "bigDecimalToString")
    @Mapping(target = "currency", qualifiedByName = "currencyToString")
    PaymentRecordGrpc toGrpc(PaymentRecordDto dto);

    @Override
    @Mapping(target = "id", qualifiedByName = "stringToUUID")
    @Mapping(target = "amount", qualifiedByName = "stringToBigDecimal")
    @Mapping(target = "currency", qualifiedByName = "stringToCurrency")
    PaymentRecordDto fromGrpc(PaymentRecordGrpc grpc);
}
```

The same unified mapper interface handles both inbound and outbound conversions, so no separate outbound mapper is needed when using the MapStruct approach.

## Working with DTOs

When your service needs to work with DTOs internally, use the main mapper to convert between domain and DTO objects:

```java
@PipelineStep(
    // ... annotation configuration
)
@ApplicationScoped
public class ProcessPaymentService implements ReactiveService<PaymentRecord, PaymentStatus> {

    @Inject
    PaymentRecordMapper paymentRecordMapper;  // Main mapper for DTO conversions

    @Override
    public Uni<PaymentStatus> process(PaymentRecord paymentRecord) {
        // Convert domain to DTO for internal processing
        PaymentRecordDto dto = paymentRecordMapper.toDto(paymentRecord);

        // Process the DTO
        PaymentStatusDto statusDto = processPaymentDto(dto);

        // Convert DTO back to domain
        return Uni.createFrom().item(paymentRecordMapper.fromDto(statusDto));
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

## Common Converters

The framework provides common converters for standard types that MapStruct cannot handle automatically:

- `UUID` ↔ `String` conversion
- `BigDecimal` ↔ `String` conversion  
- `Currency` ↔ `String` conversion
- `LocalDateTime` ↔ `String` conversion
- `List<String>` ↔ `String` conversion with delimiters

## Best Practices

1. **Use Unified Interface**: Create mappers that implement the unified `Mapper<Grpc, Dto, Domain>` interface
2. **Leverage Common Converters**: Use the provided `CommonConverters` for standard type mappings
3. **Clear Error Handling**: Add appropriate error handling for mapping failures
4. **Performance Considerations**: Be mindful of expensive mapping operations and cache where appropriate
5. **Validation**: Consider validating DTOs after mapping to ensure data integrity