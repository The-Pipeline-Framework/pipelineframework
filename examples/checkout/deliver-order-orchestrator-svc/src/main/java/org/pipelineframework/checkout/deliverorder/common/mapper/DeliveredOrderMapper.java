package org.pipelineframework.checkout.deliverorder.common.mapper;

import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.checkout.deliverorder.common.domain.DeliveredOrder;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDeliveredSvc;
import org.pipelineframework.mapper.Mapper;

@ApplicationScoped
public class DeliveredOrderMapper implements Mapper<OrderDeliveredSvc.DeliveredOrder, DeliveredOrder, DeliveredOrder> {
    /**
     * Converts a gRPC OrderDeliveredSvc.DeliveredOrder into a domain DeliveredOrder.
     *
     * @param grpc the gRPC DeliveredOrder to convert; must not be null
     * @return the corresponding domain DeliveredOrder
     * @throws IllegalArgumentException if {@code grpc} is null
     */
    @Override
    public DeliveredOrder fromGrpc(OrderDeliveredSvc.DeliveredOrder grpc) {
        if (grpc == null) {
            throw new IllegalArgumentException("grpc must not be null");
        }
        return new DeliveredOrder(
            ReadyOrderMapper.uuid(grpc.getOrderId(), "orderId"),
            ReadyOrderMapper.uuid(grpc.getCustomerId(), "customerId"),
            ReadyOrderMapper.instant(grpc.getReadyAt(), "readyAt"),
            ReadyOrderMapper.uuid(grpc.getDispatchId(), "dispatchId"),
            ReadyOrderMapper.instant(grpc.getDispatchedAt(), "dispatchedAt"),
            ReadyOrderMapper.instant(grpc.getDeliveredAt(), "deliveredAt"));
    }

    /**
     * Converts a domain DeliveredOrder to its gRPC OrderDeliveredSvc.DeliveredOrder representation.
     *
     * @param dto the domain DeliveredOrder to convert
     * @return the gRPC DeliveredOrder populated from the domain object
     * @throws IllegalArgumentException if {@code dto} is null
     */
    @Override
    public OrderDeliveredSvc.DeliveredOrder toGrpc(DeliveredOrder dto) {
        if (dto == null) {
            throw new IllegalArgumentException("dto must not be null");
        }
        return OrderDeliveredSvc.DeliveredOrder.newBuilder()
            .setOrderId(ReadyOrderMapper.str(dto.orderId()))
            .setCustomerId(ReadyOrderMapper.str(dto.customerId()))
            .setReadyAt(ReadyOrderMapper.str(dto.readyAt()))
            .setDispatchId(ReadyOrderMapper.str(dto.dispatchId()))
            .setDispatchedAt(ReadyOrderMapper.str(dto.dispatchedAt()))
            .setDeliveredAt(ReadyOrderMapper.str(dto.deliveredAt()))
            .build();
    }

    /**
     * Return the given DeliveredOrder DTO unchanged.
     *
     * @param dto the DeliveredOrder DTO to be returned
     * @return the same DeliveredOrder instance supplied as {@code dto}
     */
    @Override
    public DeliveredOrder fromDto(DeliveredOrder dto) {
        return dto;
    }

    /**
     * Return the input DeliveredOrder without modification.
     *
     * @param domain the DeliveredOrder to return
     * @return the same DeliveredOrder instance passed as input
     */
    @Override
    public DeliveredOrder toDto(DeliveredOrder domain) {
        return domain;
    }
}