package org.pipelineframework.checkout.order_request_process.service;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.UUID;
import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.checkout.common.dto.OrderRequestDto;
import org.pipelineframework.checkout.createorder.grpc.OrderRequestProcessSvc;
import org.pipelineframework.mapper.Mapper;

@ApplicationScoped
public class OrderRequestGrpcMapper implements Mapper<OrderRequestProcessSvc.OrderRequest, OrderRequestDto, OrderRequestDto> {

    /**
     * Converts a gRPC OrderRequest into an OrderRequestDto.
     *
     * @param grpc the gRPC OrderRequest to convert; must not be null
     * @return an OrderRequestDto with requestId and customerId converted to UUID (null if the source value is null or blank) and items normalized to an empty string when the source is null
     * @throws IllegalArgumentException if {@code grpc} is null
     */
    @Override
    public OrderRequestDto fromGrpc(OrderRequestProcessSvc.OrderRequest grpc) {
        if (grpc == null) {
            throw new IllegalArgumentException("grpc must not be null");
        }
        return new OrderRequestDto(
            asUuid(grpc.getRequestId()),
            asUuid(grpc.getCustomerId()),
            defaultString(grpc.getItems()));
    }

    /**
     * Convert an OrderRequestDto into its gRPC OrderRequest representation.
     *
     * @param dto the source DTO; must not be null and must have non-null `requestId` and `customerId`.
     *            The DTO's `items` may be null and will be converted to an empty string.
     * @return a populated OrderRequestProcessSvc.OrderRequest with `requestId` and `customerId`
     *         serialized to strings and `items` defaulted to an empty string when absent.
     * @throws IllegalArgumentException if `dto` is null or if `dto.requestId()` or `dto.customerId()` is null
     */
    @Override
    public OrderRequestProcessSvc.OrderRequest toGrpc(OrderRequestDto dto) {
        if (dto == null) {
            throw new IllegalArgumentException("dto must not be null");
        }
        if (dto.requestId() == null) {
            throw new IllegalArgumentException("dto.requestId must not be null");
        }
        if (dto.customerId() == null) {
            throw new IllegalArgumentException("dto.customerId must not be null");
        }
        return OrderRequestProcessSvc.OrderRequest.newBuilder()
            .setRequestId(asString(dto.requestId()))
            .setCustomerId(asString(dto.customerId()))
            .setItems(defaultString(dto.items()))
            .build();
    }

    /**
     * Return the provided DTO unchanged.
     *
     * @param dto the OrderRequestDto to return without modification
     * @return the same OrderRequestDto instance passed as {@code dto}
     */
    @Override
    public OrderRequestDto fromDto(OrderRequestDto dto) {
        return dto;
    }

    /**
     * Return the provided domain-level OrderRequestDto unchanged.
     *
     * @param domain the domain OrderRequestDto to return
     * @return the same {@code OrderRequestDto} instance passed in as {@code domain}
     */
    @Override
    public OrderRequestDto toDto(OrderRequestDto domain) {
        return domain;
    }

    /**
     * Parse a string into a UUID, returning null for null or blank input.
     *
     * @param value the string to parse; may be null or blank
     * @return the parsed UUID, or null if {@code value} is null or blank
     * @throws IllegalArgumentException if {@code value} is non-blank but not a valid UUID string
     */
    public static UUID asUuid(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid UUID format for value: " + value, e);
        }
    }

    /**
     * Convert a UUID to its canonical string form, or produce an empty string when the UUID is null.
     *
     * @param value the UUID to convert; may be null
     * @return the UUID as a string, or an empty string if {@code value} is null
     */
    public static String asString(UUID value) {
        return value == null ? "" : value.toString();
    }

    /**
     * Convert an Instant to its string representation.
     *
     * @param value the Instant to convert; may be null
     * @return the Instant as an ISO-8601 string, or an empty string if {@code value} is null
     */
    public static String asString(Instant value) {
        return value == null ? "" : value.toString();
    }

    /**
     * Converts an ISO-8601 timestamp string to an Instant, or returns null for null/blank input.
     *
     * @param value the ISO-8601 formatted timestamp string, may be null or blank
     * @return the parsed Instant, or null if {@code value} is null or blank
     * @throws IllegalArgumentException if {@code value} is non-blank but not a valid Instant representation
     */
    public static Instant asInstant(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid Instant format for value: " + value, e);
        }
    }

    /**
     * Convert a possibly-null string into a non-null string.
     *
     * @param value the input string that may be null
     * @return `value` if it is not null, otherwise an empty string
     */
    static String defaultString(String value) {
        return value == null ? "" : value;
    }
}