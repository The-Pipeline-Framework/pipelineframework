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

    static BlockingQueue<OrderDispatchSvc.ReadyOrder> forwardedQueue() {
        return FORWARDED;
    }

    static void clearForwarded() {
        FORWARDED.clear();
    }

    static void setFailIngest(boolean enabled) {
        FAIL_INGEST.set(enabled);
    }

    static int ingestFailures() {
        return INGEST_FAILURES.get();
    }

    static void reset() {
        clearForwarded();
        setFailIngest(false);
        INGEST_FAILURES.set(0);
    }

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
