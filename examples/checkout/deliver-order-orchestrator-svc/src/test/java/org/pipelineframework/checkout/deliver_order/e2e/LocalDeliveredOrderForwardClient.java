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

    static BlockingQueue<OrderDeliveredSvc.DeliveredOrder> forwardedQueue() {
        return FORWARDED;
    }

    static void setFailForward(boolean enabled) {
        FAIL_FORWARD.set(enabled);
    }

    static int forwardFailures() {
        return FORWARD_FAILURES.get();
    }

    static void reset() {
        FORWARDED.clear();
        FAIL_FORWARD.set(false);
        FORWARD_FAILURES.set(0);
    }

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
