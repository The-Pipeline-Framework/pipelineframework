package org.pipelineframework.tpfgo.dispatch.dispatch_assign_courier.service;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.mapper.Mapper;
import org.pipelineframework.tpfgo.common.domain.OrderReadyForDispatch;
import org.pipelineframework.tpfgo.common.util.GrpcMappingSupport;
import org.pipelineframework.tpfgo.dispatch.grpc.DispatchAssignCourierSvc;

@ApplicationScoped
public class OrderReadyForDispatchMapper
    implements Mapper<OrderReadyForDispatch, DispatchAssignCourierSvc.OrderReadyForDispatch> {

    @Override
    public OrderReadyForDispatch fromExternal(DispatchAssignCourierSvc.OrderReadyForDispatch external) {
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
    public DispatchAssignCourierSvc.OrderReadyForDispatch toExternal(OrderReadyForDispatch domain) {
        return DispatchAssignCourierSvc.OrderReadyForDispatch.newBuilder()
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
