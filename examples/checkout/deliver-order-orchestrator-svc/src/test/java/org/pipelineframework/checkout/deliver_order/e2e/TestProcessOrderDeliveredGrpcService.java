package org.pipelineframework.checkout.deliver_order.e2e;

import java.time.Instant;
import java.time.format.DateTimeParseException;

import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.checkout.deliverorder.grpc.MutinyProcessOrderDeliveredServiceGrpc;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDeliveredSvc;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDispatchSvc;

@GrpcService
class TestProcessOrderDeliveredGrpcService
    extends MutinyProcessOrderDeliveredServiceGrpc.ProcessOrderDeliveredServiceImplBase {

    private static final long DELIVERY_OFFSET_SECONDS = 120L;

    @Override
    public Uni<OrderDeliveredSvc.DeliveredOrder> remoteProcess(OrderDispatchSvc.DispatchedOrder request) {
        try {
            Instant dispatchedAt = Instant.parse(request.getDispatchedAt());
            OrderDeliveredSvc.DeliveredOrder delivered = OrderDeliveredSvc.DeliveredOrder.newBuilder()
                .setOrderId(request.getOrderId())
                .setCustomerId(request.getCustomerId())
                .setReadyAt(request.getReadyAt())
                .setDispatchId(request.getDispatchId())
                .setDispatchedAt(request.getDispatchedAt())
                .setDeliveredAt(dispatchedAt.plusSeconds(DELIVERY_OFFSET_SECONDS).toString())
                .build();
            return Uni.createFrom().item(delivered);
        } catch (DateTimeParseException | IllegalArgumentException e) {
            return Uni.createFrom().failure(new IllegalArgumentException(
                "Invalid dispatchedAt timestamp in test request: " + request.getDispatchedAt(), e));
        }
    }
}
