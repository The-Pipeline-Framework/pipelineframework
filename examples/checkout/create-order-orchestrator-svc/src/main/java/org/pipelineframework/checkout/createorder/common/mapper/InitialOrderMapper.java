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
        return new InitialOrder(
            OrderRequestMapper.uuid(grpc.getOrderId(), "orderId"),
            OrderRequestMapper.uuid(grpc.getCustomerId(), "customerId"),
            Math.max(0, grpc.getItemCount()));
    }

    @Override
    public OrderCreateSvc.InitialOrder toGrpc(InitialOrder dto) {
        if (dto == null) {
            return null;
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
