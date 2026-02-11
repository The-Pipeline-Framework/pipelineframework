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
    @Override
    public ReadyOrder fromGrpc(OrderDispatchSvc.ReadyOrder grpc) {
        if (grpc == null) {
            throw new IllegalArgumentException("grpc ReadyOrder is null");
        }
        return new ReadyOrder(
            uuid(grpc.getOrderId(), "orderId"),
            uuid(grpc.getCustomerId(), "customerId"),
            instant(grpc.getReadyAt(), "readyAt"));
    }

    @Override
    public OrderDispatchSvc.ReadyOrder toGrpc(ReadyOrder dto) {
        if (dto == null) {
            return null;
        }
        return OrderDispatchSvc.ReadyOrder.newBuilder()
            .setOrderId(str(dto.orderId()))
            .setCustomerId(str(dto.customerId()))
            .setReadyAt(str(dto.readyAt()))
            .build();
    }

    @Override
    public ReadyOrder fromDto(ReadyOrder dto) {
        return dto;
    }

    @Override
    public ReadyOrder toDto(ReadyOrder domain) {
        return domain;
    }

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
            throw new ReadyOrderMapperException("Invalid Instant for " + fieldName + ": " + value, e);
        }
    }

    public static String str(Instant value) {
        return value == null ? "" : value.toString();
    }
}
