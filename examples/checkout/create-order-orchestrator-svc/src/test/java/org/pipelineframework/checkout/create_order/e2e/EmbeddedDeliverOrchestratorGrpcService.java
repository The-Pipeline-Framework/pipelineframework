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

    /**
     * Clears the queue that captures ingested ReadyOrder messages for end-to-end tests.
     *
     * After this call the captured-ingest queue is empty and will not return previously-captured items.
     */
    static void reset() {
        CAPTURED_INGEST.clear();
    }

    /**
     * Waits up to the specified time for a captured ReadyOrder to become available.
     *
     * @param timeoutMillis maximum time to wait in milliseconds
     * @return the captured {@code ReadyOrder} if one is available within the timeout, {@code null} if the timeout elapses or the thread is interrupted
     *         (if interrupted, the thread's interrupted status is restored before returning)
     */
    static OrderDispatchSvc.ReadyOrder pollCapturedIngest(long timeoutMillis) {
        try {
            return CAPTURED_INGEST.poll(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /**
     * Produce a DeliveredOrder corresponding to the provided ReadyOrder.
     *
     * @param request the ready order to convert into a delivered order
     * @return the `OrderDeliveredSvc.DeliveredOrder` corresponding to the provided ready order
     */
    @Override
    public Uni<OrderDeliveredSvc.DeliveredOrder> run(OrderDispatchSvc.ReadyOrder request) {
        return Uni.createFrom().item(toDelivered(request));
    }

    /**
     * Provide an empty subscription stream for delivered orders used by end-to-end tests.
     *
     * @param request the subscription request (ignored)
     * @return an empty Multi stream of {@link OrderDeliveredSvc.DeliveredOrder} that emits no items
     */
    @Override
    public Multi<OrderDeliveredSvc.DeliveredOrder> subscribe(Empty request) {
        // Intentional for this test stub: subscription stream is unused in current E2E coverage.
        return Multi.createFrom().empty();
    }

    /**
     * Consumes a stream of ready orders, captures each for test inspection, and emits a corresponding delivered order for each input.
     *
     * @param request a stream of ReadyOrder messages to ingest
     * @return a stream of DeliveredOrder messages corresponding to each incoming ReadyOrder
     */
    @Override
    public Multi<OrderDeliveredSvc.DeliveredOrder> ingest(Multi<OrderDispatchSvc.ReadyOrder> request) {
        return request
            .onItem().invoke(CAPTURED_INGEST::offer)
            .onItem().transform(this::toDelivered);
    }

    /**
     * Create a DeliveredOrder representing the delivered state of the given ReadyOrder.
     *
     * The returned DeliveredOrder copies orderId, customerId, and readyAt from the source,
     * sets dispatchedAt and deliveredAt to the fixed test timestamp, and sets a dispatchId
     * deterministically derived from the orderId.
     *
     * @param readyOrder the source ReadyOrder to convert
     * @return an OrderDeliveredSvc.DeliveredOrder populated from the provided ReadyOrder
     */
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