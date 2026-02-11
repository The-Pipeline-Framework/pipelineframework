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

    @Override
    public ReadyOrderDto fromGrpc(OrderDispatchSvc.ReadyOrder grpc) {
        if (grpc == null) {
            throw new IllegalArgumentException("grpc must not be null");
        }
        return new ReadyOrderDto(
            asUuid(grpc.getOrderId(), "orderId"),
            asUuid(grpc.getCustomerId(), "customerId"),
            asInstant(grpc.getReadyAt(), "readyAt"));
    }

    @Override
    public OrderDispatchSvc.ReadyOrder toGrpc(ReadyOrderDto dto) {
        if (dto == null) {
            return null;
        }
        return OrderDispatchSvc.ReadyOrder.newBuilder()
            .setOrderId(dto.orderId() == null ? "" : asString(dto.orderId()))
            .setCustomerId(dto.customerId() == null ? "" : asString(dto.customerId()))
            .setReadyAt(dto.readyAt() == null ? "" : asString(dto.readyAt()))
            .build();
    }

    @Override
    public ReadyOrderDto fromDto(ReadyOrderDto dto) {
        // Deliberate no-op identity mapping: DTO and domain are the same type.
        return dto;
    }

    @Override
    public ReadyOrderDto toDto(ReadyOrderDto domain) {
        // Deliberate no-op identity mapping: DTO and domain are the same type.
        return domain;
    }

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

    public static String asString(UUID value) {
        return value == null ? "" : value.toString();
    }

    public static String asString(Instant value) {
        return value == null ? "" : value.toString();
    }

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
