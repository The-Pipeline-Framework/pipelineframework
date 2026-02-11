package org.pipelineframework.checkout.deliverorder.common.mapper;

import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.checkout.deliverorder.common.domain.DispatchedOrder;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDispatchSvc;
import org.pipelineframework.mapper.Mapper;

@ApplicationScoped
public class DispatchedOrderMapper implements Mapper<OrderDispatchSvc.DispatchedOrder, DispatchedOrder, DispatchedOrder> {
    /**
     * Converts a gRPC OrderDispatchSvc.DispatchedOrder into a domain DispatchedOrder.
     *
     * @param grpc the gRPC DispatchedOrder to convert
     * @return the corresponding domain DispatchedOrder with fields mapped from the gRPC message
     * @throws IllegalArgumentException if {@code grpc} is null
     */
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

    /**
     * Converts a domain DispatchedOrder into the corresponding gRPC OrderDispatchSvc.DispatchedOrder.
     *
     * @param dto the domain DispatchedOrder to convert
     * @return the gRPC OrderDispatchSvc.DispatchedOrder with fields mapped from the domain object
     * @throws IllegalArgumentException if {@code dto} is null
     */
    @Override
    public OrderDispatchSvc.DispatchedOrder toGrpc(DispatchedOrder dto) {
        if (dto == null) {
            throw new IllegalArgumentException("dto must not be null");
        }
        return OrderDispatchSvc.DispatchedOrder.newBuilder()
            .setOrderId(ReadyOrderMapper.str(dto.orderId()))
            .setCustomerId(ReadyOrderMapper.str(dto.customerId()))
            .setReadyAt(ReadyOrderMapper.str(dto.readyAt()))
            .setDispatchId(ReadyOrderMapper.str(dto.dispatchId()))
            .setDispatchedAt(ReadyOrderMapper.str(dto.dispatchedAt()))
            .build();
    }

    /**
     * Identity-maps a dispatched order DTO to the domain representation.
     *
     * @param dto the dispatched order DTO to map; returned unchanged
     * @return the same {@code DispatchedOrder} instance passed in
     */
    @Override
    public DispatchedOrder fromDto(DispatchedOrder dto) {
        // Intentional identity mapping: domain and DTO share the same structure.
        return dto;
    }

    /**
     * Return the domain DispatchedOrder as a DTO without modification.
     *
     * This method performs an identity mapping because the domain and DTO share the same structure;
     * it returns the same instance passed in.
     *
     * @param domain the domain DispatchedOrder to be used as the DTO
     * @return the same DispatchedOrder instance provided as {@code domain}
     */
    @Override
    public DispatchedOrder toDto(DispatchedOrder domain) {
        // Intentional identity mapping: domain and DTO share the same structure.
        return domain;
    }
}