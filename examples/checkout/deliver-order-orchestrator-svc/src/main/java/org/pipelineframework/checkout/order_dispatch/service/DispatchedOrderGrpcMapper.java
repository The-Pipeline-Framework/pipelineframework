package org.pipelineframework.checkout.order_dispatch.service;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.checkout.common.dto.DispatchedOrderDto;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDispatchSvc;
import org.pipelineframework.mapper.Mapper;

@ApplicationScoped
public class DispatchedOrderGrpcMapper implements Mapper<OrderDispatchSvc.DispatchedOrder, DispatchedOrderDto, DispatchedOrderDto> {

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

    @Override
    public DispatchedOrderDto fromDto(DispatchedOrderDto dto) {
        return dto;
    }

    @Override
    public DispatchedOrderDto toDto(DispatchedOrderDto domain) {
        return domain;
    }
}
