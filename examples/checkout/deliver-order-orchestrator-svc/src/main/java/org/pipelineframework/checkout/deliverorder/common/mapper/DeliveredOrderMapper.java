package org.pipelineframework.checkout.deliverorder.common.mapper;

import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.checkout.deliverorder.common.domain.DeliveredOrder;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDeliveredSvc;
import org.pipelineframework.mapper.Mapper;

@ApplicationScoped
public class DeliveredOrderMapper implements Mapper<OrderDeliveredSvc.DeliveredOrder, DeliveredOrder, DeliveredOrder> {
    @Override
    public DeliveredOrder fromGrpc(OrderDeliveredSvc.DeliveredOrder grpc) {
        if (grpc == null) {
            throw new IllegalArgumentException("grpc must not be null");
        }
        return new DeliveredOrder(
            ReadyOrderMapper.uuid(grpc.getOrderId(), "orderId"),
            ReadyOrderMapper.uuid(grpc.getCustomerId(), "customerId"),
            ReadyOrderMapper.instant(grpc.getReadyAt(), "readyAt"),
            ReadyOrderMapper.uuid(grpc.getDispatchId(), "dispatchId"),
            ReadyOrderMapper.instant(grpc.getDispatchedAt(), "dispatchedAt"),
            ReadyOrderMapper.instant(grpc.getDeliveredAt(), "deliveredAt"));
    }

    @Override
    public OrderDeliveredSvc.DeliveredOrder toGrpc(DeliveredOrder dto) {
        if (dto == null) {
            return null;
        }
        return OrderDeliveredSvc.DeliveredOrder.newBuilder()
            .setOrderId(ReadyOrderMapper.str(dto.orderId()))
            .setCustomerId(ReadyOrderMapper.str(dto.customerId()))
            .setReadyAt(ReadyOrderMapper.str(dto.readyAt()))
            .setDispatchId(ReadyOrderMapper.str(dto.dispatchId()))
            .setDispatchedAt(ReadyOrderMapper.str(dto.dispatchedAt()))
            .setDeliveredAt(ReadyOrderMapper.str(dto.deliveredAt()))
            .build();
    }

    @Override
    public DeliveredOrder fromDto(DeliveredOrder dto) {
        return dto;
    }

    @Override
    public DeliveredOrder toDto(DeliveredOrder domain) {
        return domain;
    }
}
