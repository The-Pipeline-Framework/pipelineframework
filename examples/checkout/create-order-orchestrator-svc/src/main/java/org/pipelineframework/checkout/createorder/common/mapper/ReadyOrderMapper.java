package org.pipelineframework.checkout.createorder.common.mapper;

import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import org.pipelineframework.checkout.createorder.common.domain.ReadyOrder;
import org.pipelineframework.checkout.createorder.grpc.OrderReadySvc;
import org.pipelineframework.mapper.Mapper;

@ApplicationScoped
public class ReadyOrderMapper implements Mapper<OrderReadySvc.ReadyOrder, ReadyOrder, ReadyOrder> {
    @Override
    public ReadyOrder fromGrpc(OrderReadySvc.ReadyOrder grpc) {
        if (grpc == null) {
            throw new IllegalArgumentException("grpc must not be null");
        }
        return new ReadyOrder(
            OrderRequestMapper.uuid(grpc.getOrderId(), "orderId"),
            OrderRequestMapper.uuid(grpc.getCustomerId(), "customerId"),
            instant(grpc.getReadyAt()));
    }

    @Override
    public OrderReadySvc.ReadyOrder toGrpc(ReadyOrder dto) {
        if (dto == null) {
            return null;
        }
        return OrderReadySvc.ReadyOrder.newBuilder()
            .setOrderId(OrderRequestMapper.str(dto.orderId()))
            .setCustomerId(OrderRequestMapper.str(dto.customerId()))
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

    public static Instant instant(String value) {
        return OrderRequestMapper.instant(value, "readyAt");
    }

    public static String str(Instant value) {
        return OrderRequestMapper.str(value);
    }
}
