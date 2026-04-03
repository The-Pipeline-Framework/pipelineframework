package org.pipelineframework.tpfgo.checkout.checkout_create_pending.service;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.mapper.Mapper;
import org.pipelineframework.tpfgo.checkout.grpc.PipelineTypes;
import org.pipelineframework.tpfgo.common.domain.OrderPending;
import org.pipelineframework.tpfgo.common.util.GrpcMappingSupport;

@ApplicationScoped
public class OrderPendingMapper implements Mapper<OrderPending, PipelineTypes.OrderPending> {

    @Override
    public OrderPending fromExternal(PipelineTypes.OrderPending external) {
        return new OrderPending(
            GrpcMappingSupport.uuid(external.getOrderId(), "orderId"),
            GrpcMappingSupport.uuid(external.getRequestId(), "requestId"),
            GrpcMappingSupport.uuid(external.getCustomerId(), "customerId"),
            GrpcMappingSupport.uuid(external.getRestaurantId(), "restaurantId"),
            GrpcMappingSupport.decimal(external.getTotalAmount(), "totalAmount"),
            external.getCurrency(),
            GrpcMappingSupport.instant(external.getCreatedAt(), "createdAt"));
    }

    @Override
    public PipelineTypes.OrderPending toExternal(OrderPending domain) {
        return PipelineTypes.OrderPending.newBuilder()
            .setOrderId(GrpcMappingSupport.str(domain.orderId()))
            .setRequestId(GrpcMappingSupport.str(domain.requestId()))
            .setCustomerId(GrpcMappingSupport.str(domain.customerId()))
            .setRestaurantId(GrpcMappingSupport.str(domain.restaurantId()))
            .setTotalAmount(GrpcMappingSupport.str(domain.totalAmount()))
            .setCurrency(domain.currency())
            .setCreatedAt(GrpcMappingSupport.str(domain.createdAt()))
            .build();
    }
}
