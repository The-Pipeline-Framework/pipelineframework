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

    @Override
    public OrderRequest fromDto(OrderRequest dto) {
        return dto;
    }

    @Override
    public OrderRequest toDto(OrderRequest domain) {
        return domain;
    }

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

    public static String str(UUID value) {
        return value == null ? "" : value.toString();
    }

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

    public static String str(Instant value) {
        return value == null ? "" : value.toString();
    }
}
