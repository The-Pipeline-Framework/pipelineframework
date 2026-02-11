package org.pipelineframework.checkout.deliver_order.connector;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDeliveredSvc;

/**
 * Abstraction for forwarding deliver-order checkpoints to a downstream consumer.
 */
public interface DeliveredOrderForwardClient {

    /**
 * Forward delivered-order checkpoints from the provided stream to a downstream consumer.
 *
 * @param deliveredOrderStream a stream of delivered-order messages to be forwarded
 * @return a {@link Cancellable} that can be used to cancel the forwarding subscription
 */
Cancellable forward(Multi<OrderDeliveredSvc.DeliveredOrder> deliveredOrderStream);
}