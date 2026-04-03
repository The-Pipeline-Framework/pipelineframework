package org.pipelineframework.tpfgo.kitchen.preparation.kitchen_reduce_completion.service;

import java.util.Objects;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.mapper.Mapper;
import org.pipelineframework.tpfgo.common.domain.OrderReadyForDispatch;
import org.pipelineframework.tpfgo.common.util.GrpcMappingSupport;
import org.pipelineframework.tpfgo.kitchen.preparation.grpc.PipelineTypes;

@ApplicationScoped
public class OrderReadyForDispatchMapper
    implements Mapper<OrderReadyForDispatch, PipelineTypes.OrderReadyForDispatch> {

    @Override
    public OrderReadyForDispatch fromExternal(PipelineTypes.OrderReadyForDispatch external) {
        Objects.requireNonNull(external, "external order ready DTO must not be null");
        return new OrderReadyForDispatch(
            GrpcMappingSupport.uuid(external.getOrderId(), "orderId"),
            GrpcMappingSupport.uuid(external.getCustomerId(), "customerId"),
            GrpcMappingSupport.uuid(external.getRestaurantId(), "restaurantId"),
            GrpcMappingSupport.decimal(external.getTotalAmount(), "totalAmount"),
            external.getCurrency(),
            GrpcMappingSupport.instant(external.getReadyAt(), "readyAt"),
            GrpcMappingSupport.uuid(external.getKitchenTicketId(), "kitchenTicketId"),
            external.getLineageDigest());
    }

    @Override
    public PipelineTypes.OrderReadyForDispatch toExternal(OrderReadyForDispatch domain) {
        return PipelineTypes.OrderReadyForDispatch.newBuilder()
            .setOrderId(GrpcMappingSupport.str(domain.orderId()))
            .setCustomerId(GrpcMappingSupport.str(domain.customerId()))
            .setRestaurantId(GrpcMappingSupport.str(domain.restaurantId()))
            .setTotalAmount(GrpcMappingSupport.str(domain.totalAmount()))
            .setCurrency(domain.currency())
            .setReadyAt(GrpcMappingSupport.str(domain.readyAt()))
            .setKitchenTicketId(GrpcMappingSupport.str(domain.kitchenTicketId()))
            .setLineageDigest(domain.lineageDigest())
            .build();
    }
}
