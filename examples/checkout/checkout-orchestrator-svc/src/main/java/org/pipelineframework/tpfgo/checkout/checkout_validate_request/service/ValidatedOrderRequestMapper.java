package org.pipelineframework.tpfgo.checkout.checkout_validate_request.service;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.mapper.Mapper;
import org.pipelineframework.tpfgo.checkout.grpc.CheckoutValidateRequestSvc;
import org.pipelineframework.tpfgo.common.domain.ValidatedOrderRequest;
import org.pipelineframework.tpfgo.common.util.GrpcMappingSupport;

@ApplicationScoped
public class ValidatedOrderRequestMapper
    implements Mapper<ValidatedOrderRequest, CheckoutValidateRequestSvc.ValidatedOrderRequest> {

    @Override
    public ValidatedOrderRequest fromExternal(CheckoutValidateRequestSvc.ValidatedOrderRequest external) {
        return new ValidatedOrderRequest(
            GrpcMappingSupport.uuid(external.getRequestId(), "requestId"),
            GrpcMappingSupport.uuid(external.getCustomerId(), "customerId"),
            GrpcMappingSupport.uuid(external.getRestaurantId(), "restaurantId"),
            external.getItems(),
            GrpcMappingSupport.decimal(external.getTotalAmount(), "totalAmount"),
            external.getCurrency(),
            GrpcMappingSupport.instant(external.getValidatedAt(), "validatedAt"));
    }

    @Override
    public CheckoutValidateRequestSvc.ValidatedOrderRequest toExternal(ValidatedOrderRequest domain) {
        return CheckoutValidateRequestSvc.ValidatedOrderRequest.newBuilder()
            .setRequestId(GrpcMappingSupport.str(domain.requestId()))
            .setCustomerId(GrpcMappingSupport.str(domain.customerId()))
            .setRestaurantId(GrpcMappingSupport.str(domain.restaurantId()))
            .setItems(domain.items())
            .setTotalAmount(GrpcMappingSupport.str(domain.totalAmount()))
            .setCurrency(domain.currency())
            .setValidatedAt(GrpcMappingSupport.str(domain.validatedAt()))
            .build();
    }
}
