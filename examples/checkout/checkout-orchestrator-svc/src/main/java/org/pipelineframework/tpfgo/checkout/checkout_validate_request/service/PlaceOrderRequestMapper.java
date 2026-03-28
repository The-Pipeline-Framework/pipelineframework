package org.pipelineframework.tpfgo.checkout.checkout_validate_request.service;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.mapper.Mapper;
import org.pipelineframework.tpfgo.checkout.grpc.CheckoutValidateRequestSvc;
import org.pipelineframework.tpfgo.common.domain.PlaceOrderRequest;
import org.pipelineframework.tpfgo.common.util.GrpcMappingSupport;

@ApplicationScoped
public class PlaceOrderRequestMapper implements Mapper<PlaceOrderRequest, CheckoutValidateRequestSvc.PlaceOrderRequest> {

    @Override
    public PlaceOrderRequest fromExternal(CheckoutValidateRequestSvc.PlaceOrderRequest external) {
        return new PlaceOrderRequest(
            GrpcMappingSupport.uuid(external.getRequestId(), "requestId"),
            GrpcMappingSupport.uuid(external.getCustomerId(), "customerId"),
            GrpcMappingSupport.uuid(external.getRestaurantId(), "restaurantId"),
            external.getItems(),
            GrpcMappingSupport.decimal(external.getTotalAmount(), "totalAmount"),
            external.getCurrency());
    }

    @Override
    public CheckoutValidateRequestSvc.PlaceOrderRequest toExternal(PlaceOrderRequest domain) {
        return CheckoutValidateRequestSvc.PlaceOrderRequest.newBuilder()
            .setRequestId(GrpcMappingSupport.str(domain.requestId()))
            .setCustomerId(GrpcMappingSupport.str(domain.customerId()))
            .setRestaurantId(GrpcMappingSupport.str(domain.restaurantId()))
            .setItems(domain.items())
            .setTotalAmount(GrpcMappingSupport.str(domain.totalAmount()))
            .setCurrency(domain.currency())
            .build();
    }
}
