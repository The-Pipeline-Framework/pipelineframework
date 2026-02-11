package org.pipelineframework.checkout.order_ready.service;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.checkout.common.dto.ReadyOrderDto;
import org.pipelineframework.checkout.createorder.grpc.OrderReadySvc;
import org.pipelineframework.checkout.order_request_process.service.OrderRequestGrpcMapper;
import org.pipelineframework.mapper.Mapper;

@ApplicationScoped
public class ReadyOrderGrpcMapper implements Mapper<OrderReadySvc.ReadyOrder, ReadyOrderDto, ReadyOrderDto> {

    @Override
    public ReadyOrderDto fromGrpc(OrderReadySvc.ReadyOrder grpc) {
        if (grpc == null) {
            throw new IllegalArgumentException("grpc must not be null");
        }
        return new ReadyOrderDto(
            OrderRequestGrpcMapper.asUuid(grpc.getOrderId()),
            OrderRequestGrpcMapper.asUuid(grpc.getCustomerId()),
            OrderRequestGrpcMapper.asInstant(grpc.getReadyAt()));
    }

    @Override
    public OrderReadySvc.ReadyOrder toGrpc(ReadyOrderDto dto) {
        if (dto == null) {
            return null;
        }
        return OrderReadySvc.ReadyOrder.newBuilder()
            .setOrderId(dto.orderId() == null ? "" : OrderRequestGrpcMapper.asString(dto.orderId()))
            .setCustomerId(dto.customerId() == null ? "" : OrderRequestGrpcMapper.asString(dto.customerId()))
            .setReadyAt(dto.readyAt() == null ? "" : OrderRequestGrpcMapper.asString(dto.readyAt()))
            .build();
    }

    @Override
    public ReadyOrderDto fromDto(ReadyOrderDto dto) {
        return dto;
    }

    @Override
    public ReadyOrderDto toDto(ReadyOrderDto domain) {
        return domain;
    }
}
