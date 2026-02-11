package org.pipelineframework.checkout.deliver_order.e2e;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.jboss.logging.Logger;
import org.pipelineframework.checkout.deliver_order.connector.DeliveredOrderForwardClient;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDeliveredSvc;

@Alternative
@Priority(1)
@ApplicationScoped
public class LocalDeliveredOrderForwardClient implements DeliveredOrderForwardClient {

    private static final Logger LOG = Logger.getLogger(LocalDeliveredOrderForwardClient.class);
    private static final BlockingQueue<OrderDeliveredSvc.DeliveredOrder> FORWARDED = new LinkedBlockingQueue<>();
    private static final AtomicBoolean FAIL_FORWARD = new AtomicBoolean(false);
    private static final AtomicInteger FORWARD_FAILURES = new AtomicInteger(0);

    /**
     * Accesses the shared in-memory queue of delivered orders captured by the local forward client.
     *
     * @return the blocking queue containing captured {@link OrderDeliveredSvc.DeliveredOrder} instances
     */
    static BlockingQueue<OrderDeliveredSvc.DeliveredOrder> forwardedQueue() {
        return FORWARDED;
    }

    /**
     * Enable or disable simulation of forward failures for the local delivered-order forward client.
     *
     * @param enabled true to cause subsequent forwards to be treated as failures, false to allow normal forwarding
     */
    static void setFailForward(boolean enabled) {
        FAIL_FORWARD.set(enabled);
    }

    /**
     * Reports the total number of times forwarding was forced to fail.
     *
     * @return the total number of simulated forward failures
     */
    static int forwardFailures() {
        return FORWARD_FAILURES.get();
    }

    /**
     * Resets the local forwarded-order test state to defaults.
     *
     * Clears the in-memory forwarded queue, disables forced forward failures, and resets the forward-failure counter to zero.
     */
    static void reset() {
        FORWARDED.clear();
        FAIL_FORWARD.set(false);
        FORWARD_FAILURES.set(0);
    }

    /**
     * Subscribes to a stream of delivered orders and captures each item into a local in-memory queue,
     * optionally simulating per-item forward failures.
     *
     * <p>If failure simulation is enabled, the method increments the forward-failure counter for each
     * received item and does not enqueue it. Otherwise, each received item is added to the shared
     * forwarded queue. Stream-level failures are logged.</p>
     *
     * @param deliveredOrderStream the reactive stream of delivered orders to consume
     * @return a {@code Cancellable} representing the active subscription, which can be used to cancel it
     */
    @Override
    public Cancellable forward(Multi<OrderDeliveredSvc.DeliveredOrder> deliveredOrderStream) {
        LOG.info("Using local delivered-order forward client");
        return deliveredOrderStream
            .onItem().invoke(item -> {
                if (FAIL_FORWARD.get()) {
                    FORWARD_FAILURES.incrementAndGet();
                    LOG.warnf("Forced forward failure for order %s", item.getOrderId());
                    return;
                }
                FORWARDED.offer(item);
                LOG.infof("Captured delivered order %s", item.getOrderId());
            })
            .subscribe().with(item -> {
            }, failure -> LOG.warnf("Local delivered-order forward stream failed: %s", failure.getMessage()));
    }
}