package org.pipelineframework.checkout.deliverorder.common.mapper;

import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.UUID;
import org.pipelineframework.checkout.deliverorder.common.domain.ReadyOrder;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDispatchSvc;
import org.pipelineframework.mapper.Mapper;

@ApplicationScoped
public class ReadyOrderMapper implements Mapper<OrderDispatchSvc.ReadyOrder, ReadyOrder, ReadyOrder> {
    /**
     * Converts a gRPC OrderDispatchSvc.ReadyOrder into a domain ReadyOrder.
     *
     * The gRPC object must contain valid `orderId` and `customerId` UUID strings and a valid
     * ISO-8601 `readyAt` timestamp; these are validated and parsed during conversion.
     *
     * @param grpc the gRPC ReadyOrder to convert
     * @return a ReadyOrder domain object constructed from the gRPC message
     * @throws IllegalArgumentException if {@code grpc} is null
     * @throws ReadyOrderMapperException if any of `orderId`, `customerId`, or `readyAt` are missing or invalid
     */
    @Override
    public ReadyOrder fromGrpc(OrderDispatchSvc.ReadyOrder grpc) {
        if (grpc == null) {
            throw new IllegalArgumentException("grpc ReadyOrder is null");
        }
        UUID orderId = uuid(grpc.getOrderId(), "orderId");
        UUID customerId = uuid(grpc.getCustomerId(), "customerId");
        Instant readyAt = instant(grpc.getReadyAt(), "readyAt");
        if (orderId == null || customerId == null || readyAt == null) {
            throw new ReadyOrderMapperException("grpc ReadyOrder must include orderId, customerId and readyAt");
        }
        return new ReadyOrder(
            orderId,
            customerId,
            readyAt);
    }

    /**
     * Convert a domain ReadyOrder into its gRPC OrderDispatchSvc.ReadyOrder representation.
     *
     * @param dto the domain ReadyOrder to convert; must not be null
     * @throws IllegalArgumentException if {@code dto} is null
     * @return a gRPC OrderDispatchSvc.ReadyOrder with orderId, customerId, and readyAt populated from {@code dto}
     */
    @Override
    public OrderDispatchSvc.ReadyOrder toGrpc(ReadyOrder dto) {
        if (dto == null) {
            throw new IllegalArgumentException("dto must not be null");
        }
        return OrderDispatchSvc.ReadyOrder.newBuilder()
            .setOrderId(str(dto.orderId()))
            .setCustomerId(str(dto.customerId()))
            .setReadyAt(str(dto.readyAt()))
            .build();
    }

    /**
     * Return the given DTO unchanged.
     *
     * @param dto the DTO instance to return
     * @return the same `dto` instance that was provided
     */
    @Override
    public ReadyOrder fromDto(ReadyOrder dto) {
        return dto;
    }

    /**
     * Return the provided domain ReadyOrder unchanged.
     *
     * @param domain the domain ReadyOrder to map (returned as-is)
     * @return the same ReadyOrder instance passed as {@code domain}
     */
    @Override
    public ReadyOrder toDto(ReadyOrder domain) {
        return domain;
    }

    /**
     * Parse a UUID from its string representation or return null for null/blank input.
     *
     * @param value the string to parse as a UUID; may be null or blank
     * @param fieldName the name of the field used in the exception message if parsing fails
     * @return the parsed {@link UUID}, or {@code null} if {@code value} is null or blank
     * @throws ReadyOrderMapperException if {@code value} is non-blank but not a valid UUID
     */
    public static UUID uuid(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            throw new ReadyOrderMapperException("Invalid UUID for " + fieldName + ": " + value, e);
        }
    }

    /**
     * Convert the given UUID to its canonical string representation, or return an empty string if null.
     *
     * @param value the UUID to convert; may be null
     * @return `value.toString()` if `value` is non-null, empty string otherwise
     */
    public static String str(UUID value) {
        return value == null ? "" : value.toString();
    }

    /**
     * Parses an ISO-8601 instant string into an Instant.
     *
     * @param value the ISO-8601 instant string to parse; may be null or blank
     * @param fieldName the name of the source field used in error messages
     * @return the parsed Instant, or null if the input is null or blank
     * @throws ReadyOrderMapperException if the input is non-blank but not a valid ISO-8601 instant
     */
    public static Instant instant(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            throw new ReadyOrderMapperException("Invalid Instant for " + fieldName + ": " + value, e);
        }
    }

    /**
     * Convert an Instant to its ISO-8601 string representation or an empty string when null.
     *
     * @param value the Instant to convert, may be null
     * @return the ISO-8601 string representation of the given Instant, or an empty string if {@code value} is null
     */
    public static String str(Instant value) {
        return value == null ? "" : value.toString();
    }
}