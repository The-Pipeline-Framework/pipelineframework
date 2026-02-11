package org.pipelineframework.checkout.order_dispatch.service;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.checkout.common.dto.DispatchedOrderDto;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDispatchSvc;
import org.pipelineframework.mapper.Mapper;

@ApplicationScoped
public class DispatchedOrderGrpcMapper implements Mapper<OrderDispatchSvc.DispatchedOrder, DispatchedOrderDto, DispatchedOrderDto> {

    /**
     * Convert a gRPC OrderDispatchSvc.DispatchedOrder message into a DispatchedOrderDto.
     *
     * @param grpc the gRPC DispatchedOrder to convert
     * @return the resulting DispatchedOrderDto with corresponding identifiers and timestamps
     * @throws IllegalArgumentException if {@code grpc} is null
     */
    @Override
    public DispatchedOrderDto fromGrpc(OrderDispatchSvc.DispatchedOrder grpc) {
        if (grpc == null) {
            throw new IllegalArgumentException("grpc must not be null");
        }
        return new DispatchedOrderDto(
            ReadyOrderGrpcMapper.asUuid(grpc.getOrderId(), "orderId"),
            ReadyOrderGrpcMapper.asUuid(grpc.getCustomerId(), "customerId"),
            ReadyOrderGrpcMapper.asInstant(grpc.getReadyAt(), "readyAt"),
            ReadyOrderGrpcMapper.asUuid(grpc.getDispatchId(), "dispatchId"),
            ReadyOrderGrpcMapper.asInstant(grpc.getDispatchedAt(), "dispatchedAt"));
    }

    /**
     * Converts a DispatchedOrderDto to its gRPC representation.
     *
     * @param dto the dispatched order DTO to convert
     * @return the corresponding OrderDispatchSvc.DispatchedOrder message
     * @throws IllegalArgumentException if dto is null
     */
    @Override
    public OrderDispatchSvc.DispatchedOrder toGrpc(DispatchedOrderDto dto) {
        if (dto == null) {
            throw new IllegalArgumentException("dto must not be null");
        }
        return OrderDispatchSvc.DispatchedOrder.newBuilder()
            .setOrderId(ReadyOrderGrpcMapper.asString(dto.orderId()))
            .setCustomerId(ReadyOrderGrpcMapper.asString(dto.customerId()))
            .setReadyAt(ReadyOrderGrpcMapper.asString(dto.readyAt()))
            .setDispatchId(ReadyOrderGrpcMapper.asString(dto.dispatchId()))
            .setDispatchedAt(ReadyOrderGrpcMapper.asString(dto.dispatchedAt()))
            .build();
    }

    /**
     * Return the provided DispatchedOrderDto unchanged.
     *
     * @param dto the DTO to return
     * @return the same {@code DispatchedOrderDto} instance passed as {@code dto}
     */
    @Override
    public DispatchedOrderDto fromDto(DispatchedOrderDto dto) {
        return dto;
    }

    /**
     * Returns the provided DispatchedOrderDto unchanged.
     *
     * @param domain the DTO to return
     * @return the same {@code DispatchedOrderDto} instance passed as {@code domain}
     */
    @Override
    public DispatchedOrderDto toDto(DispatchedOrderDto domain) {
        return domain;
    }
}