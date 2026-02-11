package org.pipelineframework.checkout.create_order.e2e;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import org.jboss.logging.Logger;
import org.pipelineframework.checkout.create_order.connector.DeliverOrderIngestClient;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDispatchSvc;

@Alternative
@Priority(1)
@ApplicationScoped
public class LocalDeliverCaptureIngestClient implements DeliverOrderIngestClient {

    private static final Logger LOG = Logger.getLogger(LocalDeliverCaptureIngestClient.class);
    private static final BlockingQueue<OrderDispatchSvc.ReadyOrder> FORWARDED = new LinkedBlockingQueue<>();
    private static final AtomicBoolean FAIL_INGEST = new AtomicBoolean(false);
    private static final AtomicInteger INGEST_FAILURES = new AtomicInteger(0);

    /**
     * Accesses the shared queue used to capture orders forwarded by the local test ingest client.
     *
     * @return the live BlockingQueue of OrderDispatchSvc.ReadyOrder instances containing forwarded orders
     */
    static BlockingQueue<OrderDispatchSvc.ReadyOrder> forwardedQueue() {
        return FORWARDED;
    }

    /**
     * Removes all captured ready orders from the internal forwarded queue.
     */
    static void clearForwarded() {
        FORWARDED.clear();
    }

    /**
     * Enable or disable the forced ingest failure mode for the local test ingest client.
     *
     * @param enabled `true` to enable forced ingest failures, `false` to disable them
     */
    static void setFailIngest(boolean enabled) {
        FAIL_INGEST.set(enabled);
    }

    /**
     * Returns the current count of simulated ingest failures recorded by this test client.
     *
     * @return the number of forced ingest failures
     */
    static int ingestFailures() {
        return INGEST_FAILURES.get();
    }

    /**
     * Resets the client's internal test state: clears the forwarded orders queue, disables forced ingest failures, and resets the ingest-failure counter to zero.
     */
    static void reset() {
        clearForwarded();
        setFailIngest(false);
        INGEST_FAILURES.set(0);
    }

    /**
     * Subscribes to a stream of ready orders and captures each item for local test forwarding.
     *
     * <p>Captured orders are enqueued into the class' internal forwarded queue. When forced ingest
     * failure mode is enabled, items are dropped and the ingest failure counter is incremented.
     * Stream termination with an error is logged.</p>
     *
     * @param readyOrderStream the stream of orders that are ready to be forwarded to the ingest client
     * @return the subscription's {@code Cancellable} which can be used to cancel processing
     */
    @Override
    public Cancellable forward(Multi<OrderDispatchSvc.ReadyOrder> readyOrderStream) {
        LOG.info("Using local test ingest client for create->deliver forwarding");
        return readyOrderStream
            .onItem().invoke(item -> {
                if (FAIL_INGEST.get()) {
                    INGEST_FAILURES.incrementAndGet();
                    LOG.warnf("Dropped forwarded order %s due to forced ingest failure mode", item.getOrderId());
                    return;
                }
                FORWARDED.offer(item);
                LOG.infof("Captured forwarded order %s", item.getOrderId());
            })
            .subscribe().with(item -> {
            }, failure -> {
                LOG.warnf("Local test ingest stream terminated with failure: %s", failure.getMessage());
            });
    }
}