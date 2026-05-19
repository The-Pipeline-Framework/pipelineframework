package org.pipelineframework.awaitable.spi;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitStepDescriptor;

/**
 * Adapter SPI for dispatching await interactions to external systems.
 *
 * @param <I> request payload type
 * @param <O> completion payload type
 */
public interface AwaitTransportAdapter<I, O> {

    /**
     * Adapter type used in YAML.
     *
     * @return adapter type
     */
    String type();

    /**
     * Dispatches an await request to the external system.
     *
     * @param request dispatch request
     * @return dispatch result metadata
     */
    Uni<AwaitDispatchResult> dispatch(AwaitDispatchRequest<I> request);

    /**
     * Cancels an already dispatched interaction when supported by the adapter.
     *
     * @param request cancel request
     * @return completion signal
     */
    default Uni<Void> cancel(AwaitCancelRequest request) {
        return Uni.createFrom().voidItem();
    }

    /**
     * Dispatch request passed to adapters.
     */
    record AwaitDispatchRequest<I>(
        AwaitStepDescriptor descriptor,
        AwaitInteractionRecord interaction,
        I payload
    ) {
    }

    /**
     * Dispatch metadata returned by adapters.
     */
    record AwaitDispatchResult(java.util.Map<String, Object> metadata) {
        public AwaitDispatchResult {
            metadata = metadata == null ? java.util.Map.of() : java.util.Map.copyOf(metadata);
        }
    }

    /**
     * Cancel request passed to adapters.
     */
    record AwaitCancelRequest(AwaitStepDescriptor descriptor, AwaitInteractionRecord interaction, String reason) {
    }
}
