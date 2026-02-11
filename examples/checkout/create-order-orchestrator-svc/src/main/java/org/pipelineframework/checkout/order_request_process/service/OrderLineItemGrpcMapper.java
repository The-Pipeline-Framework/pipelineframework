package org.pipelineframework.checkout.order_request_process.service;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.checkout.common.dto.OrderLineItemDto;
import org.pipelineframework.checkout.createorder.grpc.OrderRequestProcessSvc;
import org.pipelineframework.mapper.Mapper;

@ApplicationScoped
public class OrderLineItemGrpcMapper implements Mapper<OrderRequestProcessSvc.OrderLineItem, OrderLineItemDto, OrderLineItemDto> {

    @Override
    public OrderLineItemDto fromGrpc(OrderRequestProcessSvc.OrderLineItem grpc) {
        if (grpc == null) {
            throw new IllegalArgumentException("grpc must not be null");
        }
        if (grpc.getRequestId().isBlank()) {
            throw new IllegalArgumentException("grpc.requestId must not be blank");
        }
        if (grpc.getCustomerId().isBlank()) {
            throw new IllegalArgumentException("grpc.customerId must not be blank");
        }
        if (grpc.getQuantity() <= 0) {
            throw new IllegalArgumentException("grpc.quantity must be > 0 but was " + grpc.getQuantity());
        }
        return new OrderLineItemDto(
            OrderRequestGrpcMapper.asUuid(grpc.getRequestId()),
            OrderRequestGrpcMapper.asUuid(grpc.getCustomerId()),
            OrderRequestGrpcMapper.defaultString(grpc.getSku()),
            grpc.getQuantity());
    }

    @Override
    public OrderRequestProcessSvc.OrderLineItem toGrpc(OrderLineItemDto dto) {
        if (dto == null) {
            throw new IllegalArgumentException("dto must not be null");
        }
        if (dto.requestId() == null) {
            throw new IllegalArgumentException("dto.requestId must not be null");
        }
        if (dto.customerId() == null) {
            throw new IllegalArgumentException("dto.customerId must not be null");
        }
        return OrderRequestProcessSvc.OrderLineItem.newBuilder()
            .setRequestId(OrderRequestGrpcMapper.asString(dto.requestId()))
            .setCustomerId(OrderRequestGrpcMapper.asString(dto.customerId()))
            .setSku(dto.sku() == null ? "" : dto.sku())
            .setQuantity(dto.quantity())
            .build();
    }

    @Override
    public OrderLineItemDto fromDto(OrderLineItemDto dto) {
        return dto;
    }

    @Override
    public OrderLineItemDto toDto(OrderLineItemDto domain) {
        return domain;
    }
}
