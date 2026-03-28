package org.pipelineframework.tpfgo.delivery.execution.delivery_execute_order.service;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.mapper.Mapper;
import org.pipelineframework.tpfgo.common.domain.OrderDelivered;
import org.pipelineframework.tpfgo.common.util.GrpcMappingSupport;
import org.pipelineframework.tpfgo.delivery.execution.grpc.DeliveryExecuteOrderSvc;

@ApplicationScoped
public class OrderDeliveredMapper implements Mapper<OrderDelivered, DeliveryExecuteOrderSvc.OrderDelivered> {

    @Override
    public OrderDelivered fromExternal(DeliveryExecuteOrderSvc.OrderDelivered external) {
        return new OrderDelivered(
            GrpcMappingSupport.uuid(external.getOrderId(), "orderId"),
            GrpcMappingSupport.uuid(external.getCustomerId(), "customerId"),
            GrpcMappingSupport.uuid(external.getDispatchId(), "dispatchId"),
            GrpcMappingSupport.uuid(external.getCourierId(), "courierId"),
            GrpcMappingSupport.uuid(external.getRestaurantId(), "restaurantId"),
            GrpcMappingSupport.uuid(external.getKitchenTicketId(), "kitchenTicketId"),
            GrpcMappingSupport.instant(external.getDeliveredAt(), "deliveredAt"),
            GrpcMappingSupport.decimal(external.getAmount(), "amount"),
            external.getCurrency(),
            external.getLineageDigest());
    }

    @Override
    public DeliveryExecuteOrderSvc.OrderDelivered toExternal(OrderDelivered domain) {
        return DeliveryExecuteOrderSvc.OrderDelivered.newBuilder()
            .setOrderId(GrpcMappingSupport.str(domain.orderId()))
            .setCustomerId(GrpcMappingSupport.str(domain.customerId()))
            .setDispatchId(GrpcMappingSupport.str(domain.dispatchId()))
            .setCourierId(GrpcMappingSupport.str(domain.courierId()))
            .setRestaurantId(GrpcMappingSupport.str(domain.restaurantId()))
            .setKitchenTicketId(GrpcMappingSupport.str(domain.kitchenTicketId()))
            .setDeliveredAt(GrpcMappingSupport.str(domain.deliveredAt()))
            .setAmount(GrpcMappingSupport.str(domain.amount()))
            .setCurrency(domain.currency())
            .setLineageDigest(domain.lineageDigest())
            .build();
    }
}
