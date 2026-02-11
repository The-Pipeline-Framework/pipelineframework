package org.pipelineframework.checkout.order_dispatch.service;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.UUID;
import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.checkout.common.dto.ReadyOrderDto;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDispatchSvc;
import org.pipelineframework.mapper.Mapper;

@ApplicationScoped
public class ReadyOrderGrpcMapper implements Mapper<OrderDispatchSvc.ReadyOrder, ReadyOrderDto, ReadyOrderDto> {

    /**
     * Convert a gRPC OrderDispatchSvc.ReadyOrder into a ReadyOrderDto.
     *
     * @param grpc the gRPC ReadyOrder to convert; must contain non-empty, parseable `orderId`, `customerId`, and `readyAt`
     * @return a ReadyOrderDto populated from the gRPC message
     * @throws IllegalArgumentException if {@code grpc} is null, if any required field is missing, or if UUID/Instant parsing fails
     */
    @Override
    public ReadyOrderDto fromGrpc(OrderDispatchSvc.ReadyOrder grpc) {
        if (grpc == null) {
            throw new IllegalArgumentException("grpc must not be null");
        }
        var orderId = asUuid(grpc.getOrderId(), "orderId");
        var customerId = asUuid(grpc.getCustomerId(), "customerId");
        var readyAt = asInstant(grpc.getReadyAt(), "readyAt");
        if (orderId == null || customerId == null || readyAt == null) {
            throw new IllegalArgumentException("grpc must include orderId, customerId and readyAt");
        }
        return new ReadyOrderDto(
            orderId,
            customerId,
            readyAt);
    }

    /**
     * Convert a ReadyOrderDto into its gRPC representation OrderDispatchSvc.ReadyOrder.
     *
     * @param dto the DTO to convert; must not be null
     * @return the corresponding OrderDispatchSvc.ReadyOrder
     * @throws IllegalArgumentException if {@code dto} is null
     */
    @Override
    public OrderDispatchSvc.ReadyOrder toGrpc(ReadyOrderDto dto) {
        if (dto == null) {
            throw new IllegalArgumentException("dto must not be null");
        }
        return OrderDispatchSvc.ReadyOrder.newBuilder()
            .setOrderId(asString(dto.orderId()))
            .setCustomerId(asString(dto.customerId()))
            .setReadyAt(asString(dto.readyAt()))
            .build();
    }

    /**
     * Return the input DTO unchanged.
     *
     * @return the same ReadyOrderDto instance that was passed in
     */
    @Override
    public ReadyOrderDto fromDto(ReadyOrderDto dto) {
        // Deliberate no-op identity mapping: DTO and domain are the same type.
        return dto;
    }

    /**
     * Identity mapping that returns the input ReadyOrderDto unchanged.
     *
     * @param domain the ReadyOrderDto to return
     * @return the same ReadyOrderDto instance passed in
     */
    @Override
    public ReadyOrderDto toDto(ReadyOrderDto domain) {
        // Deliberate no-op identity mapping: DTO and domain are the same type.
        return domain;
    }

    /**
     * Converts a non-blank string to a UUID, or returns null for null/blank input.
     *
     * @param value the string representation of a UUID to parse
     * @param fieldName the field name to include in the exception message if parsing fails
     * @return the parsed UUID, or null if {@code value} is null or blank
     * @throws IllegalArgumentException if {@code value} is not a valid UUID; message includes {@code fieldName} and the invalid value
     */
    public static UUID asUuid(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid UUID for " + fieldName + ": " + value, e);
        }
    }

    /**
     * Converts a UUID to its string representation.
     *
     * @param value the UUID to convert; may be {@code null}
     * @return the UUID's string form, or an empty string if {@code value} is {@code null}
     */
    public static String asString(UUID value) {
        return value == null ? "" : value.toString();
    }

    /**
     * Convert an Instant to its ISO-8601 string representation.
     *
     * @param value the instant to convert
     * @return the ISO-8601 string for the instant, or an empty string if {@code value} is null
     */
    public static String asString(Instant value) {
        return value == null ? "" : value.toString();
    }

    /**
     * Parse an ISO-8601 instant string into an Instant, or return null for null/blank input.
     *
     * @param value the string to parse
     * @param fieldName name of the field used in error messages when parsing fails
     * @return the parsed Instant, or null if {@code value} is null or blank
     * @throws IllegalArgumentException if {@code value} is non-blank but cannot be parsed as an Instant
     */
    public static Instant asInstant(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid Instant for " + fieldName + ": " + value, e);
        }
    }
}