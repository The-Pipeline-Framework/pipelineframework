package org.pipelineframework.checkout.deliver_order.connector;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.annotation.PostConstruct;

import io.quarkus.arc.DefaultBean;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import org.jboss.logging.Logger;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDeliveredSvc;

@ApplicationScoped
@DefaultBean
public class NoopDeliveredOrderForwardClient implements DeliveredOrderForwardClient {

    private static final Logger LOG = Logger.getLogger(NoopDeliveredOrderForwardClient.class);

    /**
     * Log that the noop delivered-order forward client is enabled and will drain checkpoints without forwarding.
     */
    @PostConstruct
    void init() {
        LOG.info("Noop delivered-order forward client enabled; draining checkpoints without forwarding");
    }

    /**
     * Consumes and drains a stream of delivered orders without forwarding them.
     *
     * Subscribes to the provided stream, ignores each item, logs any failure and logs when the stream completes.
     *
     * @param deliveredOrderStream the stream of delivered orders to consume and drain
     * @return a {@code Cancellable} that can be used to cancel the subscription
     */
    @Override
    public Cancellable forward(Multi<OrderDeliveredSvc.DeliveredOrder> deliveredOrderStream) {
        return deliveredOrderStream.subscribe().with(item -> {
        }, failure -> LOG.warn("Noop delivered-order forward stream failed", failure),
            () -> LOG.info("Noop delivered-order forward stream completed"));
    }
}