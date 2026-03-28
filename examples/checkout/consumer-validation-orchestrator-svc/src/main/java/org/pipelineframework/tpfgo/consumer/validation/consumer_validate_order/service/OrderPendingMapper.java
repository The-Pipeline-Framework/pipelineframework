package org.pipelineframework.tpfgo.consumer.validation.consumer_validate_order.service;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.mapper.Mapper;
import org.pipelineframework.tpfgo.common.domain.OrderPending;
import org.pipelineframework.tpfgo.common.util.GrpcMappingSupport;
import org.pipelineframework.tpfgo.consumer.validation.grpc.ConsumerValidateOrderSvc;

@ApplicationScoped
public class OrderPendingMapper implements Mapper<OrderPending, ConsumerValidateOrderSvc.OrderPending> {

    @Override
    public OrderPending fromExternal(ConsumerValidateOrderSvc.OrderPending external) {
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
    public ConsumerValidateOrderSvc.OrderPending toExternal(OrderPending domain) {
        return ConsumerValidateOrderSvc.OrderPending.newBuilder()
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
