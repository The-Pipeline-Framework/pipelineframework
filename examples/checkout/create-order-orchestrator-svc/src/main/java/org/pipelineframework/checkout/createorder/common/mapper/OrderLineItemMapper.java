package org.pipelineframework.checkout.createorder.common.mapper;

import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.checkout.createorder.common.domain.OrderLineItem;
import org.pipelineframework.checkout.createorder.grpc.OrderRequestProcessSvc;
import org.pipelineframework.mapper.Mapper;

@ApplicationScoped
public class OrderLineItemMapper implements Mapper<OrderRequestProcessSvc.OrderLineItem, OrderLineItem, OrderLineItem> {
    @Override
    public OrderLineItem fromGrpc(OrderRequestProcessSvc.OrderLineItem grpc) {
        if (grpc == null) {
            throw new IllegalArgumentException("grpc must not be null");
        }
        int quantity = grpc.getQuantity() <= 0 ? 1 : grpc.getQuantity();
        return new OrderLineItem(
            OrderRequestMapper.uuid(grpc.getRequestId(), "requestId"),
            OrderRequestMapper.uuid(grpc.getCustomerId(), "customerId"),
            grpc.getSku(),
            quantity);
    }

    @Override
    public OrderRequestProcessSvc.OrderLineItem toGrpc(OrderLineItem dto) {
        if (dto == null) {
            return null;
        }
        return OrderRequestProcessSvc.OrderLineItem.newBuilder()
            .setRequestId(OrderRequestMapper.str(dto.requestId()))
            .setCustomerId(OrderRequestMapper.str(dto.customerId()))
            .setSku(dto.sku() == null ? "" : dto.sku())
            .setQuantity(dto.quantity())
            .build();
    }

    @Override
    public OrderLineItem fromDto(OrderLineItem dto) {
        return dto;
    }

    @Override
    public OrderLineItem toDto(OrderLineItem domain) {
        return domain;
    }
}
