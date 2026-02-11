package org.pipelineframework.checkout.order_delivered.service;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.checkout.common.dto.DeliveredOrderDto;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDeliveredSvc;
import org.pipelineframework.checkout.order_dispatch.service.ReadyOrderGrpcMapper;
import org.pipelineframework.mapper.Mapper;

@ApplicationScoped
public class DeliveredOrderGrpcMapper implements Mapper<OrderDeliveredSvc.DeliveredOrder, DeliveredOrderDto, DeliveredOrderDto> {

    /**
     * Convert a gRPC OrderDeliveredSvc.DeliveredOrder message to a DeliveredOrderDto.
     *
     * @param grpc the gRPC DeliveredOrder to convert
     * @return the resulting DeliveredOrderDto with corresponding identifiers and timestamps
     * @throws IllegalArgumentException if {@code grpc} is null
     */
    @Override
    public DeliveredOrderDto fromGrpc(OrderDeliveredSvc.DeliveredOrder grpc) {
        if (grpc == null) {
            throw new IllegalArgumentException("grpc must not be null");
        }
        return new DeliveredOrderDto(
            ReadyOrderGrpcMapper.asUuid(grpc.getOrderId(), "orderId"),
            ReadyOrderGrpcMapper.asUuid(grpc.getCustomerId(), "customerId"),
            ReadyOrderGrpcMapper.asInstant(grpc.getReadyAt(), "readyAt"),
            ReadyOrderGrpcMapper.asUuid(grpc.getDispatchId(), "dispatchId"),
            ReadyOrderGrpcMapper.asInstant(grpc.getDispatchedAt(), "dispatchedAt"),
            ReadyOrderGrpcMapper.asInstant(grpc.getDeliveredAt(), "deliveredAt"));
    }

    /**
     * Converts a DeliveredOrderDto into its gRPC representation OrderDeliveredSvc.DeliveredOrder.
     *
     * @param dto the delivered order DTO to convert; must not be null
     * @return the corresponding OrderDeliveredSvc.DeliveredOrder populated from the DTO
     * @throws IllegalArgumentException if {@code dto} is null
     */
    @Override
    public OrderDeliveredSvc.DeliveredOrder toGrpc(DeliveredOrderDto dto) {
        if (dto == null) {
            throw new IllegalArgumentException("dto must not be null");
        }
        return OrderDeliveredSvc.DeliveredOrder.newBuilder()
            .setOrderId(ReadyOrderGrpcMapper.asString(dto.orderId()))
            .setCustomerId(ReadyOrderGrpcMapper.asString(dto.customerId()))
            .setReadyAt(ReadyOrderGrpcMapper.asString(dto.readyAt()))
            .setDispatchId(ReadyOrderGrpcMapper.asString(dto.dispatchId()))
            .setDispatchedAt(ReadyOrderGrpcMapper.asString(dto.dispatchedAt()))
            .setDeliveredAt(ReadyOrderGrpcMapper.asString(dto.deliveredAt()))
            .build();
    }

    /**
     * Return the provided DeliveredOrderDto unchanged.
     *
     * @param dto the DeliveredOrderDto to return
     * @return the same DeliveredOrderDto instance passed in
     */
    @Override
    public DeliveredOrderDto fromDto(DeliveredOrderDto dto) {
        return dto;
    }

    /**
     * Return the provided DeliveredOrderDto unchanged.
     *
     * @param domain the DeliveredOrderDto to return
     * @return the same DeliveredOrderDto instance passed in {@code domain}
     */
    @Override
    public DeliveredOrderDto toDto(DeliveredOrderDto domain) {
        return domain;
    }
}