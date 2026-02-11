package org.pipelineframework.checkout.createorder.common.mapper;

import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.checkout.createorder.common.domain.InitialOrder;
import org.pipelineframework.checkout.createorder.grpc.OrderCreateSvc;
import org.pipelineframework.mapper.Mapper;

@ApplicationScoped
public class InitialOrderMapper implements Mapper<OrderCreateSvc.InitialOrder, InitialOrder, InitialOrder> {
    @Override
    public InitialOrder fromGrpc(OrderCreateSvc.InitialOrder grpc) {
        if (grpc == null) {
            throw new IllegalArgumentException("grpc must not be null");
        }
        if (grpc.getItemCount() < 0) {
            throw new IllegalArgumentException("itemCount must be >= 0 but was " + grpc.getItemCount());
        }
        return new InitialOrder(
            OrderRequestMapper.uuid(grpc.getOrderId(), "orderId"),
            OrderRequestMapper.uuid(grpc.getCustomerId(), "customerId"),
            grpc.getItemCount());
    }

    @Override
    public OrderCreateSvc.InitialOrder toGrpc(InitialOrder dto) {
        if (dto == null) {
            throw new IllegalArgumentException("dto must not be null");
        }
        return OrderCreateSvc.InitialOrder.newBuilder()
            .setOrderId(OrderRequestMapper.str(dto.orderId()))
            .setCustomerId(OrderRequestMapper.str(dto.customerId()))
            .setItemCount(dto.itemCount())
            .build();
    }

    @Override
    public InitialOrder fromDto(InitialOrder dto) {
        return dto;
    }

    @Override
    public InitialOrder toDto(InitialOrder domain) {
        return domain;
    }
}
