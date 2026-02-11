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
            return null;
        }
        return OrderDeliveredSvc.DeliveredOrder.newBuilder()
            .setOrderId(dto.orderId() == null ? "" : ReadyOrderGrpcMapper.asString(dto.orderId()))
            .setCustomerId(dto.customerId() == null ? "" : ReadyOrderGrpcMapper.asString(dto.customerId()))
            .setReadyAt(dto.readyAt() == null ? "" : ReadyOrderGrpcMapper.asString(dto.readyAt()))
            .setDispatchId(dto.dispatchId() == null ? "" : ReadyOrderGrpcMapper.asString(dto.dispatchId()))
            .setDispatchedAt(dto.dispatchedAt() == null ? "" : ReadyOrderGrpcMapper.asString(dto.dispatchedAt()))
            .setDeliveredAt(dto.deliveredAt() == null ? "" : ReadyOrderGrpcMapper.asString(dto.deliveredAt()))
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
