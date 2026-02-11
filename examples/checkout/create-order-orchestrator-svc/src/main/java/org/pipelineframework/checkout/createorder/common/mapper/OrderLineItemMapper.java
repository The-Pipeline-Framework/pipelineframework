package org.pipelineframework.checkout.createorder.common.mapper;

import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.checkout.createorder.common.domain.OrderLineItem;
import org.pipelineframework.checkout.createorder.grpc.OrderRequestProcessSvc;
import org.pipelineframework.mapper.Mapper;

@ApplicationScoped
public class OrderLineItemMapper implements Mapper<OrderRequestProcessSvc.OrderLineItem, OrderLineItem, OrderLineItem> {
    /**
     * Converts a gRPC OrderLineItem into the domain OrderLineItem.
     *
     * @param grpc the gRPC OrderLineItem to convert; must have non-blank `requestId` and `customerId`
     *             and a `quantity` greater than zero
     * @return the mapped domain OrderLineItem with `requestId` and `customerId` converted to UUIDs,
     *         and with the same `sku` and `quantity`
     * @throws IllegalArgumentException if `grpc` is null, if `requestId` or `customerId` is null or blank,
     *                                  or if `quantity` is less than or equal to zero
     */
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

    /**
     * Convert a domain OrderLineItem into a protobuf OrderLineItem.
     *
     * @param dto the domain OrderLineItem to convert; its `sku` must be non-null and not blank
     * @return a protobuf OrderLineItem populated from the provided domain object
     * @throws IllegalArgumentException if `dto` is null or `dto.sku()` is null or blank
     */
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

    /**
     * Return the given DTO unchanged.
     *
     * @param dto the DTO to return
     * @return the same DTO instance passed as input
     */
    @Override
    public OrderLineItem fromDto(OrderLineItem dto) {
        return dto;
    }

    /**
     * Return the given domain OrderLineItem unchanged.
     *
     * @param domain the domain OrderLineItem to return
     * @return the same OrderLineItem instance passed as {@code domain}
     */
    @Override
    public OrderLineItem toDto(OrderLineItem domain) {
        return domain;
    }
}