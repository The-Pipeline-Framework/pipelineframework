package org.pipelineframework.checkout.order_request_process.service;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.checkout.common.dto.OrderLineItemDto;
import org.pipelineframework.checkout.createorder.grpc.OrderRequestProcessSvc;
import org.pipelineframework.mapper.Mapper;

@ApplicationScoped
public class OrderLineItemGrpcMapper implements Mapper<OrderRequestProcessSvc.OrderLineItem, OrderLineItemDto, OrderLineItemDto> {

    /**
     * Convert a gRPC OrderLineItem into an OrderLineItemDto.
     *
     * @param grpc the gRPC OrderLineItem to convert; must not be null, must have non-blank `requestId` and `customerId`, and `quantity` must be greater than 0
     * @return an OrderLineItemDto populated from the gRPC message
     * @throws IllegalArgumentException if `grpc` is null, if `requestId` or `customerId` are blank, or if `quantity` is less than or equal to 0
     */
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

    /**
     * Convert an OrderLineItemDto to a gRPC OrderLineItem.
     *
     * @param dto the domain DTO to convert
     * @return a constructed {@link OrderRequestProcessSvc.OrderLineItem} representing the DTO
     * @throws IllegalArgumentException if {@code dto} is null, or if {@code dto.requestId()} or {@code dto.customerId()} is null
     */
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

    /**
     * Return the input OrderLineItemDto unchanged.
     *
     * @return the same OrderLineItemDto instance that was provided
     */
    @Override
    public OrderLineItemDto fromDto(OrderLineItemDto dto) {
        return dto;
    }

    /**
     * Return the given OrderLineItemDto unchanged.
     *
     * @param domain the DTO to return unchanged
     * @return the same OrderLineItemDto instance passed as {@code domain}
     */
    @Override
    public OrderLineItemDto toDto(OrderLineItemDto domain) {
        return domain;
    }
}