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
            throw new IllegalArgumentException("dto must not be null");
        }
        if (dto.orderId() == null) {
            throw new IllegalArgumentException("dto.orderId must not be null");
        }
        if (dto.customerId() == null) {
            throw new IllegalArgumentException("dto.customerId must not be null");
        }
        if (dto.readyAt() == null) {
            throw new IllegalArgumentException("dto.readyAt must not be null");
        }
        return OrderReadySvc.ReadyOrder.newBuilder()
            .setOrderId(OrderRequestGrpcMapper.asString(dto.orderId()))
            .setCustomerId(OrderRequestGrpcMapper.asString(dto.customerId()))
            .setReadyAt(OrderRequestGrpcMapper.asString(dto.readyAt()))
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
