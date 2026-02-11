package org.pipelineframework.checkout.create_order.e2e;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.UUID;

import jakarta.inject.Singleton;

import com.google.protobuf.Empty;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.checkout.deliverorder.grpc.MutinyOrchestratorServiceGrpc;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDeliveredSvc;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDispatchSvc;

@GrpcService
@Singleton
public class EmbeddedDeliverOrchestratorGrpcService extends MutinyOrchestratorServiceGrpc.OrchestratorServiceImplBase {

    private static final String FIXED_TEST_TIME = "2026-02-10T00:00:00Z";
    private static final BlockingQueue<OrderDispatchSvc.ReadyOrder> CAPTURED_INGEST = new LinkedBlockingQueue<>();

    static void reset() {
        CAPTURED_INGEST.clear();
    }

    static OrderDispatchSvc.ReadyOrder pollCapturedIngest(long timeoutMillis) {
        try {
            return CAPTURED_INGEST.poll(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    @Override
    public Uni<OrderDeliveredSvc.DeliveredOrder> run(OrderDispatchSvc.ReadyOrder request) {
        return Uni.createFrom().item(toDelivered(request));
    }

    @Override
    public Multi<OrderDeliveredSvc.DeliveredOrder> subscribe(Empty request) {
        // Intentional for this test stub: subscription stream is unused in current E2E coverage.
        return Multi.createFrom().empty();
    }

    @Override
    public Multi<OrderDeliveredSvc.DeliveredOrder> ingest(Multi<OrderDispatchSvc.ReadyOrder> request) {
        return request
            .onItem().invoke(CAPTURED_INGEST::offer)
            .onItem().transform(this::toDelivered);
    }

    private OrderDeliveredSvc.DeliveredOrder toDelivered(OrderDispatchSvc.ReadyOrder readyOrder) {
        String dispatchId = UUID.nameUUIDFromBytes(readyOrder.getOrderId().getBytes()).toString();
        return OrderDeliveredSvc.DeliveredOrder.newBuilder()
            .setOrderId(readyOrder.getOrderId())
            .setCustomerId(readyOrder.getCustomerId())
            .setReadyAt(readyOrder.getReadyAt())
            .setDispatchId(dispatchId)
            .setDispatchedAt(FIXED_TEST_TIME)
            .setDeliveredAt(FIXED_TEST_TIME)
            .build();
    }
}
