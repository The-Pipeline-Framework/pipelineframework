package org.pipelineframework.checkout.order_create.service;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.checkout.common.dto.InitialOrderDto;
import org.pipelineframework.checkout.createorder.grpc.OrderCreateSvc;
import org.pipelineframework.checkout.order_request_process.service.OrderRequestGrpcMapper;
import org.pipelineframework.mapper.Mapper;

@ApplicationScoped
public class InitialOrderGrpcMapper implements Mapper<OrderCreateSvc.InitialOrder, InitialOrderDto, InitialOrderDto> {

    @Override
    public InitialOrderDto fromGrpc(OrderCreateSvc.InitialOrder grpc) {
        if (grpc == null) {
            throw new IllegalArgumentException("grpc must not be null");
        }
        if (grpc.getItemCount() < 0) {
            throw new IllegalArgumentException("grpc.itemCount must be >= 0 but was " + grpc.getItemCount());
        }
        return new InitialOrderDto(
            OrderRequestGrpcMapper.asUuid(grpc.getOrderId()),
            OrderRequestGrpcMapper.asUuid(grpc.getCustomerId()),
            grpc.getItemCount());
    }

    @Override
    public OrderCreateSvc.InitialOrder toGrpc(InitialOrderDto dto) {
        if (dto == null) {
            throw new IllegalArgumentException("dto must not be null");
        }
        if (dto.orderId() == null) {
            throw new IllegalArgumentException("dto.orderId must not be null");
        }
        if (dto.customerId() == null) {
            throw new IllegalArgumentException("dto.customerId must not be null");
        }
        return OrderCreateSvc.InitialOrder.newBuilder()
            .setOrderId(OrderRequestGrpcMapper.asString(dto.orderId()))
            .setCustomerId(OrderRequestGrpcMapper.asString(dto.customerId()))
            .setItemCount(Math.max(0, dto.itemCount()))
            .build();
    }

    @Override
    public InitialOrderDto fromDto(InitialOrderDto dto) {
        return dto;
    }

    @Override
    public InitialOrderDto toDto(InitialOrderDto domain) {
        return domain;
    }
}
