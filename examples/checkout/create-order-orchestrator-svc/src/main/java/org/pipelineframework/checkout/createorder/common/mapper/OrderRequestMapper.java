package org.pipelineframework.checkout.createorder.common.mapper;

import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.UUID;
import org.pipelineframework.checkout.createorder.common.domain.OrderRequest;
import org.pipelineframework.checkout.createorder.grpc.OrderRequestProcessSvc;
import org.pipelineframework.mapper.Mapper;

@ApplicationScoped
public class OrderRequestMapper implements Mapper<OrderRequestProcessSvc.OrderRequest, OrderRequest, OrderRequest> {
    /**
     * Converts a gRPC OrderRequest into the domain OrderRequest.
     *
     * The resulting domain object contains UUIDs parsed from `requestId` and `customerId`
     * (null if the corresponding gRPC field is null or blank) and the same `items` string.
     *
     * @param grpc the gRPC OrderRequest to convert
     * @return the mapped domain OrderRequest
     * @throws IllegalArgumentException if `grpc` is null or if `requestId`/`customerId` are present but not valid UUIDs
     */
    @Override
    public OrderRequest fromGrpc(OrderRequestProcessSvc.OrderRequest grpc) {
        if (grpc == null) {
            throw new IllegalArgumentException("grpc must not be null");
        }
        return new OrderRequest(
            uuid(grpc.getRequestId(), "requestId"),
            uuid(grpc.getCustomerId(), "customerId"),
            grpc.getItems());
    }

    /**
     * Converts a DTO OrderRequest into its gRPC OrderRequest representation.
     *
     * @param dto the DTO OrderRequest to convert
     * @return the corresponding gRPC OrderRequest; UUID and Instant fields are serialized to strings and a null items value is converted to an empty string
     * @throws IllegalArgumentException if {@code dto} is null
     */
    @Override
    public OrderRequestProcessSvc.OrderRequest toGrpc(OrderRequest dto) {
        if (dto == null) {
            throw new IllegalArgumentException("dto must not be null");
        }
        return OrderRequestProcessSvc.OrderRequest.newBuilder()
            .setRequestId(str(dto.requestId()))
            .setCustomerId(str(dto.customerId()))
            .setItems(dto.items() == null ? "" : dto.items())
            .build();
    }

    /**
     * Return the given DTO unchanged.
     *
     * @return the same OrderRequest instance that was supplied
     */
    @Override
    public OrderRequest fromDto(OrderRequest dto) {
        return dto;
    }

    /**
     * Return the domain OrderRequest unchanged as the DTO representation.
     *
     * @param domain the domain OrderRequest to convert (returned as-is)
     * @return the same OrderRequest instance passed as domain
     */
    @Override
    public OrderRequest toDto(OrderRequest domain) {
        return domain;
    }

    /**
     * Convert a string to a UUID instance or return null when the input is null or blank.
     *
     * @param value the string to parse as a UUID; may be null or blank
     * @param fieldName the name of the field used in error messages when parsing fails
     * @return the UUID parsed from {@code value}, or {@code null} if {@code value} is null or blank
     * @throws IllegalArgumentException if {@code value} is not a valid UUID string; the exception message includes {@code fieldName} and the invalid value
     */
    public static UUID uuid(String value, String fieldName) {
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
     * Produce the string form of the given UUID, returning an empty string when the input is null.
     *
     * @param value the UUID to convert; may be null
     * @return the UUID's string value, or an empty string if {@code value} is null
     */
    public static String str(UUID value) {
        return value == null ? "" : value.toString();
    }

    /**
     * Parses an ISO-8601 timestamp string into an Instant.
     *
     * @param value the ISO-8601 timestamp string to parse; may be null or blank
     * @param fieldName the name of the field used in the exception message when parsing fails
     * @return the parsed Instant, or null if {@code value} is null or blank
     * @throws IllegalArgumentException if {@code value} is non-blank but cannot be parsed as an Instant
     */
    public static Instant instant(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid Instant for " + fieldName + ": " + value, e);
        }
    }

    /**
     * Convert an Instant to its ISO-8601 string representation.
     *
     * @param value the Instant to convert; may be null
     * @return the Instant's string representation, or an empty string if {@code value} is null
     */
    public static String str(Instant value) {
        return value == null ? "" : value.toString();
    }
}