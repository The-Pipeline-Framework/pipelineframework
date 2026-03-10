package org.pipelineframework.checkout.deliver_order.e2e;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.checkout.deliverorder.grpc.MutinyProcessOrderDispatchServiceGrpc;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDispatchSvc;

@GrpcService
class TestProcessOrderDispatchGrpcService
    extends MutinyProcessOrderDispatchServiceGrpc.ProcessOrderDispatchServiceImplBase {

    @Override
    public Uni<OrderDispatchSvc.DispatchedOrder> remoteProcess(OrderDispatchSvc.ReadyOrder request) {
        Instant readyAt = Instant.parse(request.getReadyAt());
        OrderDispatchSvc.DispatchedOrder dispatched = OrderDispatchSvc.DispatchedOrder.newBuilder()
            .setOrderId(request.getOrderId())
            .setCustomerId(request.getCustomerId())
            .setReadyAt(request.getReadyAt())
            .setDispatchId(deterministicDispatchId(request.getOrderId()))
            .setDispatchedAt(readyAt.plusSeconds(60).toString())
            .build();
        return Uni.createFrom().item(dispatched);
    }

    private static String deterministicDispatchId(String orderId) {
        return UUID.nameUUIDFromBytes(("dispatch:" + orderId).getBytes(StandardCharsets.UTF_8)).toString();
    }
}
