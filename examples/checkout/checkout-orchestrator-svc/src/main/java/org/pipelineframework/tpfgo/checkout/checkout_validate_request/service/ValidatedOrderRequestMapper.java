package org.pipelineframework.tpfgo.checkout.checkout_validate_request.service;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.mapper.Mapper;
import org.pipelineframework.tpfgo.checkout.grpc.PipelineTypes;
import org.pipelineframework.tpfgo.common.domain.ValidatedOrderRequest;
import org.pipelineframework.tpfgo.common.domain.OrderItem;
import org.pipelineframework.tpfgo.common.util.GrpcMappingSupport;

@ApplicationScoped
public class ValidatedOrderRequestMapper
    implements Mapper<ValidatedOrderRequest, PipelineTypes.ValidatedOrderRequest> {

    @Override
    public ValidatedOrderRequest fromExternal(PipelineTypes.ValidatedOrderRequest external) {
        var items = external.getItemsList().stream()
            .map(this::toDomainItem)
            .toList();

        return new ValidatedOrderRequest(
            GrpcMappingSupport.uuid(external.getRequestId(), "requestId"),
            GrpcMappingSupport.uuid(external.getCustomerId(), "customerId"),
            GrpcMappingSupport.uuid(external.getRestaurantId(), "restaurantId"),
            items,
            GrpcMappingSupport.decimal(external.getTotalAmount(), "totalAmount"),
            external.getCurrency(),
            GrpcMappingSupport.instant(external.getValidatedAt(), "validatedAt"));
    }

    @Override
    public PipelineTypes.ValidatedOrderRequest toExternal(ValidatedOrderRequest domain) {
        PipelineTypes.ValidatedOrderRequest.Builder builder = PipelineTypes.ValidatedOrderRequest.newBuilder()
            .setRequestId(GrpcMappingSupport.str(domain.requestId()))
            .setCustomerId(GrpcMappingSupport.str(domain.customerId()))
            .setRestaurantId(GrpcMappingSupport.str(domain.restaurantId()))
            .setTotalAmount(GrpcMappingSupport.str(domain.totalAmount()))
            .setCurrency(domain.currency())
            .setValidatedAt(GrpcMappingSupport.str(domain.validatedAt()));

        for (OrderItem item : domain.items()) {
            builder.addItems(toExternalItem(item));
        }

        return builder.build();
    }

    private OrderItem toDomainItem(PipelineTypes.OrderItem proto) {
        return new OrderItem(proto.getSku(), proto.getQuantity());
    }

    private PipelineTypes.OrderItem toExternalItem(OrderItem domain) {
        return PipelineTypes.OrderItem.newBuilder()
            .setSku(domain.sku())
            .setQuantity(domain.quantity())
            .build();
    }
}
