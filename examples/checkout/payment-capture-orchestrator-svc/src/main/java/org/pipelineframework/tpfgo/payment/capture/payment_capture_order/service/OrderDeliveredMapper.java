package org.pipelineframework.tpfgo.payment.capture.payment_capture_order.service;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.mapper.Mapper;
import org.pipelineframework.tpfgo.common.domain.OrderDelivered;
import org.pipelineframework.tpfgo.common.util.GrpcMappingSupport;
import org.pipelineframework.tpfgo.payment.capture.grpc.PaymentCaptureOrderSvc;

@ApplicationScoped
public class OrderDeliveredMapper implements Mapper<OrderDelivered, PaymentCaptureOrderSvc.OrderDelivered> {

    @Override
    public OrderDelivered fromExternal(PaymentCaptureOrderSvc.OrderDelivered external) {
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
    public PaymentCaptureOrderSvc.OrderDelivered toExternal(OrderDelivered domain) {
        return PaymentCaptureOrderSvc.OrderDelivered.newBuilder()
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
