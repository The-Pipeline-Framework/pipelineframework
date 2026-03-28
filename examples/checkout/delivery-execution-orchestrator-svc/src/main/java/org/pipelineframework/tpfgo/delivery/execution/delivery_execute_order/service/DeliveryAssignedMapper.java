package org.pipelineframework.tpfgo.delivery.execution.delivery_execute_order.service;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.mapper.Mapper;
import org.pipelineframework.tpfgo.common.domain.DeliveryAssigned;
import org.pipelineframework.tpfgo.common.util.GrpcMappingSupport;
import org.pipelineframework.tpfgo.delivery.execution.grpc.DeliveryExecuteOrderSvc;

@ApplicationScoped
public class DeliveryAssignedMapper implements Mapper<DeliveryAssigned, DeliveryExecuteOrderSvc.DeliveryAssigned> {

    @Override
    public DeliveryAssigned fromExternal(DeliveryExecuteOrderSvc.DeliveryAssigned external) {
        return new DeliveryAssigned(
            GrpcMappingSupport.uuid(external.getOrderId(), "orderId"),
            GrpcMappingSupport.uuid(external.getCustomerId(), "customerId"),
            GrpcMappingSupport.uuid(external.getRestaurantId(), "restaurantId"),
            GrpcMappingSupport.decimal(external.getTotalAmount(), "totalAmount"),
            external.getCurrency(),
            GrpcMappingSupport.uuid(external.getKitchenTicketId(), "kitchenTicketId"),
            GrpcMappingSupport.uuid(external.getDispatchId(), "dispatchId"),
            GrpcMappingSupport.uuid(external.getCourierId(), "courierId"),
            external.getEtaMinutes(),
            GrpcMappingSupport.instant(external.getAssignedAt(), "assignedAt"),
            external.getLineageDigest());
    }

    @Override
    public DeliveryExecuteOrderSvc.DeliveryAssigned toExternal(DeliveryAssigned domain) {
        return DeliveryExecuteOrderSvc.DeliveryAssigned.newBuilder()
            .setOrderId(GrpcMappingSupport.str(domain.orderId()))
            .setCustomerId(GrpcMappingSupport.str(domain.customerId()))
            .setRestaurantId(GrpcMappingSupport.str(domain.restaurantId()))
            .setTotalAmount(GrpcMappingSupport.str(domain.totalAmount()))
            .setCurrency(domain.currency())
            .setKitchenTicketId(GrpcMappingSupport.str(domain.kitchenTicketId()))
            .setDispatchId(GrpcMappingSupport.str(domain.dispatchId()))
            .setCourierId(GrpcMappingSupport.str(domain.courierId()))
            .setEtaMinutes(domain.etaMinutes())
            .setAssignedAt(GrpcMappingSupport.str(domain.assignedAt()))
            .setLineageDigest(domain.lineageDigest())
            .build();
    }
}
