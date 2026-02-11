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

    @Override
    public OrderRequestDto fromDto(OrderRequestDto dto) {
        return dto;
    }

    @Override
    public OrderRequestDto toDto(OrderRequestDto domain) {
        return domain;
    }

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

    public static String asString(UUID value) {
        return value == null ? "" : value.toString();
    }

    public static String asString(Instant value) {
        return value == null ? "" : value.toString();
    }

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

    static String defaultString(String value) {
        return value == null ? "" : value;
    }
}
