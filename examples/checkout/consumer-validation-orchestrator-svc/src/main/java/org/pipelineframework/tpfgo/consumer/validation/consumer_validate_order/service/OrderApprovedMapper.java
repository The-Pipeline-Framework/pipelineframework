package org.pipelineframework.tpfgo.consumer.validation.consumer_validate_order.service;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.mapper.Mapper;
import org.pipelineframework.tpfgo.common.domain.OrderApproved;
import org.pipelineframework.tpfgo.common.util.GrpcMappingSupport;
import org.pipelineframework.tpfgo.consumer.validation.grpc.ConsumerValidateOrderSvc;

@ApplicationScoped
public class OrderApprovedMapper implements Mapper<OrderApproved, ConsumerValidateOrderSvc.OrderApproved> {

    @Override
    public OrderApproved fromExternal(ConsumerValidateOrderSvc.OrderApproved external) {
        return new OrderApproved(
            GrpcMappingSupport.uuid(external.getOrderId(), "orderId"),
            GrpcMappingSupport.uuid(external.getRequestId(), "requestId"),
            GrpcMappingSupport.uuid(external.getCustomerId(), "customerId"),
            GrpcMappingSupport.uuid(external.getRestaurantId(), "restaurantId"),
            GrpcMappingSupport.decimal(external.getTotalAmount(), "totalAmount"),
            external.getCurrency(),
            GrpcMappingSupport.instant(external.getApprovedAt(), "approvedAt"),
            external.getRiskBand());
    }

    @Override
    public ConsumerValidateOrderSvc.OrderApproved toExternal(OrderApproved domain) {
        return ConsumerValidateOrderSvc.OrderApproved.newBuilder()
            .setOrderId(GrpcMappingSupport.str(domain.orderId()))
            .setRequestId(GrpcMappingSupport.str(domain.requestId()))
            .setCustomerId(GrpcMappingSupport.str(domain.customerId()))
            .setRestaurantId(GrpcMappingSupport.str(domain.restaurantId()))
            .setTotalAmount(GrpcMappingSupport.str(domain.totalAmount()))
            .setCurrency(domain.currency())
            .setApprovedAt(GrpcMappingSupport.str(domain.approvedAt()))
            .setRiskBand(domain.riskBand())
            .build();
    }
}
