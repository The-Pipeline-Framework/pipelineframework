package org.pipelineframework.checkout.order_create.service;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.checkout.common.dto.InitialOrderDto;
import org.pipelineframework.checkout.createorder.grpc.OrderCreateSvc;
import org.pipelineframework.checkout.order_request_process.service.OrderRequestGrpcMapper;
import org.pipelineframework.mapper.Mapper;

@ApplicationScoped
public class InitialOrderGrpcMapper implements Mapper<OrderCreateSvc.InitialOrder, InitialOrderDto, InitialOrderDto> {

    /**
     * Converts a gRPC OrderCreateSvc.InitialOrder into an InitialOrderDto.
     *
     * @param grpc the gRPC InitialOrder to convert; must not be null and its itemCount must be >= 0
     * @return an InitialOrderDto with UUIDs parsed from the gRPC orderId and customerId and the same itemCount
     * @throws IllegalArgumentException if {@code grpc} is null or {@code grpc.getItemCount()} is less than 0
     */
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

    /**
     * Converts a domain InitialOrderDto into a gRPC OrderCreateSvc.InitialOrder message.
     *
     * @param dto the domain InitialOrderDto to convert
     * @return the corresponding OrderCreateSvc.InitialOrder gRPC message
     * @throws IllegalArgumentException if {@code dto} is null, if {@code dto.orderId()} is null,
     *                                  if {@code dto.customerId()} is null, or if {@code dto.itemCount()} is less than 0
     */
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
        if (dto.itemCount() < 0) {
            throw new IllegalArgumentException("itemCount must be non-negative");
        }
        return OrderCreateSvc.InitialOrder.newBuilder()
            .setOrderId(OrderRequestGrpcMapper.asString(dto.orderId()))
            .setCustomerId(OrderRequestGrpcMapper.asString(dto.customerId()))
            .setItemCount(dto.itemCount())
            .build();
    }

    /**
     * Return the given InitialOrderDto unchanged.
     *
     * @param dto the InitialOrderDto to return
     * @return the same {@code dto} instance passed in
     */
    @Override
    public InitialOrderDto fromDto(InitialOrderDto dto) {
        return dto;
    }

    /**
     * Return the provided InitialOrderDto unmodified.
     *
     * @param domain the InitialOrderDto to return
     * @return the same InitialOrderDto instance passed in
     */
    @Override
    public InitialOrderDto toDto(InitialOrderDto domain) {
        return domain;
    }
}