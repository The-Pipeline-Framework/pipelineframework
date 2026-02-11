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
        if (grpc.getRequestId() == null || grpc.getRequestId().isBlank()) {
            throw new IllegalArgumentException("grpc.requestId must not be blank");
        }
        if (grpc.getCustomerId() == null || grpc.getCustomerId().isBlank()) {
            throw new IllegalArgumentException("grpc.customerId must not be blank");
        }
        int quantity = grpc.getQuantity();
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be > 0 but was " + quantity);
        }
        return new OrderLineItem(
            OrderRequestMapper.uuid(grpc.getRequestId(), "requestId"),
            OrderRequestMapper.uuid(grpc.getCustomerId(), "customerId"),
            grpc.getSku(),
            quantity);
    }

    @Override
    public OrderRequestProcessSvc.OrderLineItem toGrpc(OrderLineItem dto) {
        if (dto == null) {
            throw new IllegalArgumentException("dto must not be null");
        }
        if (dto.sku() == null || dto.sku().isBlank()) {
            throw new IllegalArgumentException("dto.sku must not be blank");
        }
        return OrderRequestProcessSvc.OrderLineItem.newBuilder()
            .setRequestId(OrderRequestMapper.str(dto.requestId()))
            .setCustomerId(OrderRequestMapper.str(dto.customerId()))
            .setSku(dto.sku())
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
