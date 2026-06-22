package org.pipelineframework.tpfgo.checkout.checkout_validate_request.service;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.mapper.Mapper;
import org.pipelineframework.tpfgo.checkout.grpc.PipelineTypes;
import org.pipelineframework.tpfgo.common.domain.PlaceOrderRequest;
import org.pipelineframework.tpfgo.common.domain.OrderItem;
import org.pipelineframework.tpfgo.common.util.GrpcMappingSupport;

@ApplicationScoped
public class PlaceOrderRequestMapper implements Mapper<PlaceOrderRequest, PipelineTypes.PlaceOrderRequest> {

    @Override
    public PlaceOrderRequest fromExternal(PipelineTypes.PlaceOrderRequest external) {
        var items = external.getItemsList().stream()
            .map(this::toDomainItem)
            .toList();

        return new PlaceOrderRequest(
            GrpcMappingSupport.uuid(external.getRequestId(), "requestId"),
            GrpcMappingSupport.uuid(external.getCustomerId(), "customerId"),
            GrpcMappingSupport.uuid(external.getRestaurantId(), "restaurantId"),
            items,
            GrpcMappingSupport.decimal(external.getTotalAmount(), "totalAmount"),
            external.getCurrency());
    }

    @Override
    public PipelineTypes.PlaceOrderRequest toExternal(PlaceOrderRequest domain) {
        PipelineTypes.PlaceOrderRequest.Builder builder = PipelineTypes.PlaceOrderRequest.newBuilder()
            .setRequestId(GrpcMappingSupport.str(domain.requestId()))
            .setCustomerId(GrpcMappingSupport.str(domain.customerId()))
            .setRestaurantId(GrpcMappingSupport.str(domain.restaurantId()))
            .setTotalAmount(GrpcMappingSupport.str(domain.totalAmount()))
            .setCurrency(domain.currency());

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
