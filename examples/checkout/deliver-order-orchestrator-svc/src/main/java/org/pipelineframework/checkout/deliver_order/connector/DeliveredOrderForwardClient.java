package org.pipelineframework.checkout.deliver_order.connector;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import java.util.Objects;
import java.util.function.Consumer;
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

/**
 * Forward delivered-order checkpoints and notify per-item success/failure callbacks.
 *
 * <p>Implementations that can confirm downstream per-item delivery should override this method and invoke
 * {@code onForwarded} only after successful delivery confirmation. The default implementation preserves
 * backward compatibility by decorating the input stream.</p>
 *
 * @param deliveredOrderStream a stream of delivered-order messages to be forwarded
 * @param onForwarded callback invoked for each item considered successfully forwarded
 * @param onForwardFailure callback invoked when forwarding stream fails
 * @return a {@link Cancellable} that can be used to cancel the forwarding subscription
 */
default Cancellable forward(
    Multi<OrderDeliveredSvc.DeliveredOrder> deliveredOrderStream,
    Consumer<OrderDeliveredSvc.DeliveredOrder> onForwarded,
    Consumer<Throwable> onForwardFailure
) {
    Objects.requireNonNull(onForwarded, "onForwarded must not be null");
    Objects.requireNonNull(onForwardFailure, "onForwardFailure must not be null");
    Multi<OrderDeliveredSvc.DeliveredOrder> wrapped = deliveredOrderStream
        .onItem().invoke(onForwarded)
        .onFailure().invoke(onForwardFailure);
    return forward(wrapped);
}
}
