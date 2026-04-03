package org.pipelineframework.tpfgo.restaurant.acceptance.restaurant_accept_order.service;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.mapper.Mapper;
import org.pipelineframework.tpfgo.common.domain.OrderAcceptedByRestaurant;
import org.pipelineframework.tpfgo.common.util.GrpcMappingSupport;
import org.pipelineframework.tpfgo.restaurant.acceptance.grpc.PipelineTypes;

@ApplicationScoped
public class OrderAcceptedByRestaurantMapper
    implements Mapper<OrderAcceptedByRestaurant, PipelineTypes.OrderAcceptedByRestaurant> {

    @Override
    public OrderAcceptedByRestaurant fromExternal(PipelineTypes.OrderAcceptedByRestaurant external) {
        return new OrderAcceptedByRestaurant(
            GrpcMappingSupport.uuid(external.getOrderId(), "orderId"),
            GrpcMappingSupport.uuid(external.getRequestId(), "requestId"),
            GrpcMappingSupport.uuid(external.getCustomerId(), "customerId"),
            GrpcMappingSupport.uuid(external.getRestaurantId(), "restaurantId"),
            GrpcMappingSupport.decimal(external.getTotalAmount(), "totalAmount"),
            external.getCurrency(),
            GrpcMappingSupport.instant(external.getAcceptedAt(), "acceptedAt"),
            GrpcMappingSupport.uuid(external.getKitchenTicketId(), "kitchenTicketId"));
    }

    @Override
    public PipelineTypes.OrderAcceptedByRestaurant toExternal(OrderAcceptedByRestaurant domain) {
        return PipelineTypes.OrderAcceptedByRestaurant.newBuilder()
            .setOrderId(GrpcMappingSupport.str(domain.orderId()))
            .setRequestId(GrpcMappingSupport.str(domain.requestId()))
            .setCustomerId(GrpcMappingSupport.str(domain.customerId()))
            .setRestaurantId(GrpcMappingSupport.str(domain.restaurantId()))
            .setTotalAmount(GrpcMappingSupport.str(domain.totalAmount()))
            .setCurrency(domain.currency())
            .setAcceptedAt(GrpcMappingSupport.str(domain.acceptedAt()))
            .setKitchenTicketId(GrpcMappingSupport.str(domain.kitchenTicketId()))
            .build();
    }
}
