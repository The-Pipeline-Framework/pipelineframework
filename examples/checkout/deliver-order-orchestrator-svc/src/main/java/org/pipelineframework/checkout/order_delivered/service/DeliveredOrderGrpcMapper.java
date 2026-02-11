package org.pipelineframework.checkout.order_delivered.service;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.checkout.common.dto.DeliveredOrderDto;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDeliveredSvc;
import org.pipelineframework.checkout.order_dispatch.service.ReadyOrderGrpcMapper;
import org.pipelineframework.mapper.Mapper;

@ApplicationScoped
public class DeliveredOrderGrpcMapper implements Mapper<OrderDeliveredSvc.DeliveredOrder, DeliveredOrderDto, DeliveredOrderDto> {

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

    @Override
    public DeliveredOrderDto fromDto(DeliveredOrderDto dto) {
        return dto;
    }

    @Override
    public DeliveredOrderDto toDto(DeliveredOrderDto domain) {
        return domain;
    }
}
