package org.pipelineframework.checkout.create_order.connector;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDispatchSvc;

/**
 * Abstraction for forwarding create-order checkpoints to the deliver-order ingest stream.
 * Implementations should preserve downstream backpressure semantics of the consumed {@code Multi},
 * return the active subscription so callers can cancel forwarding, and surface terminal failures
 * via the returned subscription's failure callback.
 */
@FunctionalInterface
public interface DeliverOrderIngestClient {

    /**
 * Forwards ReadyOrder items from the provided stream into the deliver-order ingest path while preserving downstream backpressure.
 *
 * @param readyOrderStream the Reactive stream of ReadyOrder checkpoints to forward
 * @return the active subscription for the forwarding operation; callers can cancel it and receive terminal failures via its failure callback
 */
Cancellable forward(Multi<OrderDispatchSvc.ReadyOrder> readyOrderStream);
}