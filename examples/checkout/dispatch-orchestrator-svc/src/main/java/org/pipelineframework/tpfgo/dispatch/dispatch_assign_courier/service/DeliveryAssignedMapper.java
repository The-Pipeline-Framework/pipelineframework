package org.pipelineframework.tpfgo.dispatch.dispatch_assign_courier.service;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.mapper.Mapper;
import org.pipelineframework.tpfgo.common.domain.DeliveryAssigned;
import org.pipelineframework.tpfgo.common.util.GrpcMappingSupport;
import org.pipelineframework.tpfgo.dispatch.grpc.DispatchAssignCourierSvc;

@ApplicationScoped
public class DeliveryAssignedMapper implements Mapper<DeliveryAssigned, DispatchAssignCourierSvc.DeliveryAssigned> {

    @Override
    public DeliveryAssigned fromExternal(DispatchAssignCourierSvc.DeliveryAssigned external) {
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
    public DispatchAssignCourierSvc.DeliveryAssigned toExternal(DeliveryAssigned domain) {
        return DispatchAssignCourierSvc.DeliveryAssigned.newBuilder()
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
