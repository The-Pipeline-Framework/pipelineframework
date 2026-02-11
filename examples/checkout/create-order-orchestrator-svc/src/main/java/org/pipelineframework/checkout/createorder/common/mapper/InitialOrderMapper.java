package org.pipelineframework.checkout.createorder.common.mapper;

import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.checkout.createorder.common.domain.InitialOrder;
import org.pipelineframework.checkout.createorder.grpc.OrderCreateSvc;
import org.pipelineframework.mapper.Mapper;

@ApplicationScoped
public class InitialOrderMapper implements Mapper<OrderCreateSvc.InitialOrder, InitialOrder, InitialOrder> {
    /**
     * Map a gRPC InitialOrder message to a domain InitialOrder.
     *
     * @param grpc the gRPC OrderCreateSvc.InitialOrder message to convert
     * @return the mapped InitialOrder domain object
     * @throws IllegalArgumentException if {@code grpc} is {@code null} or if {@code grpc.getItemCount()} is less than 0
     */
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

    /**
     * Build the outgoing gRPC request representing the initial order.
     *
     * @param dto the InitialOrder to convert; must not be null and must have itemCount > 0
     * @return a gRPC InitialOrder message with orderId, customerId, and itemCount taken from the dto
     * @throws IllegalArgumentException if {@code dto} is null or {@code dto.itemCount()} is less than or equal to 0
     */
    @Override
    public OrderCreateSvc.InitialOrder toGrpc(InitialOrder dto) {
        if (dto == null) {
            throw new IllegalArgumentException("dto must not be null");
        }
        if (dto.itemCount() <= 0) {
            throw new IllegalArgumentException("itemCount must be > 0");
        }
        return OrderCreateSvc.InitialOrder.newBuilder()
            .setOrderId(OrderRequestMapper.str(dto.orderId()))
            .setCustomerId(OrderRequestMapper.str(dto.customerId()))
            .setItemCount(dto.itemCount())
            .build();
    }

    /**
     * Return the provided InitialOrder unchanged.
     *
     * @return the same InitialOrder instance that was passed in
     */
    @Override
    public InitialOrder fromDto(InitialOrder dto) {
        return dto;
    }

    /**
     * Map a domain InitialOrder to its DTO representation (identity mapping).
     *
     * @param domain the domain InitialOrder to convert
     * @return the same InitialOrder instance passed as {@code domain}
     */
    @Override
    public InitialOrder toDto(InitialOrder domain) {
        return domain;
    }
}