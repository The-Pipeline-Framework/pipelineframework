package org.pipelineframework.checkout.deliver_order.connector;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDeliveredSvc;

/**
 * Abstraction for forwarding deliver-order checkpoints to a downstream consumer.
 */
public interface DeliveredOrderForwardClient {

    Cancellable forward(Multi<OrderDeliveredSvc.DeliveredOrder> deliveredOrderStream);
}
