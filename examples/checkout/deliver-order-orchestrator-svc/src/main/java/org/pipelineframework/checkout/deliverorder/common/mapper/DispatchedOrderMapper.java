package org.pipelineframework.checkout.deliverorder.common.mapper;

import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.checkout.deliverorder.common.domain.DispatchedOrder;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDispatchSvc;
import org.pipelineframework.mapper.Mapper;

@ApplicationScoped
public class DispatchedOrderMapper implements Mapper<OrderDispatchSvc.DispatchedOrder, DispatchedOrder, DispatchedOrder> {
    @Override
    public DispatchedOrder fromGrpc(OrderDispatchSvc.DispatchedOrder grpc) {
        if (grpc == null) {
            throw new IllegalArgumentException("grpc must not be null");
        }
        return new DispatchedOrder(
            ReadyOrderMapper.uuid(grpc.getOrderId(), "orderId"),
            ReadyOrderMapper.uuid(grpc.getCustomerId(), "customerId"),
            ReadyOrderMapper.instant(grpc.getReadyAt(), "readyAt"),
            ReadyOrderMapper.uuid(grpc.getDispatchId(), "dispatchId"),
            ReadyOrderMapper.instant(grpc.getDispatchedAt(), "dispatchedAt"));
    }

    @Override
    public OrderDispatchSvc.DispatchedOrder toGrpc(DispatchedOrder dto) {
        if (dto == null) {
            return null;
        }
        return OrderDispatchSvc.DispatchedOrder.newBuilder()
            .setOrderId(ReadyOrderMapper.str(dto.orderId()))
            .setCustomerId(ReadyOrderMapper.str(dto.customerId()))
            .setReadyAt(ReadyOrderMapper.str(dto.readyAt()))
            .setDispatchId(ReadyOrderMapper.str(dto.dispatchId()))
            .setDispatchedAt(ReadyOrderMapper.str(dto.dispatchedAt()))
            .build();
    }

    @Override
    public DispatchedOrder fromDto(DispatchedOrder dto) {
        // Intentional identity mapping: domain and DTO share the same structure.
        return dto;
    }

    @Override
    public DispatchedOrder toDto(DispatchedOrder domain) {
        return domain;
    }
}
