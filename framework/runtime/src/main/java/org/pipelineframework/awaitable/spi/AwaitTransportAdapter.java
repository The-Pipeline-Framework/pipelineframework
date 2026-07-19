package org.pipelineframework.awaitable.spi;

import java.util.Optional;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitStepDescriptor;

/**
 * Adapter SPI for dispatching await interactions to external systems.
 *
 * @param <I> request payload type
 */
public interface AwaitTransportAdapter<I> {

    /**
     * Adapter type used in YAML.
     *
     * @return adapter type
     */
    String type();

    /**
     * Whether this transport can feed durable completions into a live await stream.
     *
     * @return true when live await windows are supported
     */
    default boolean supportsLiveAwaitWindow() {
        return false;
    }

    /**
     * Returns the normalized provider endpoint used to scope durable admission.
     *
     * <p>An empty value means this adapter has not opted into durable admission.
     * Runtime startup validation rejects that combination when admission is enabled.</p>
     *
     * @param descriptor authored await descriptor
     * @return normalized provider endpoint when available
     */
    default Optional<String> admissionEndpoint(AwaitStepDescriptor descriptor) {
        return Optional.empty();
    }

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
     *
     * @param descriptor authored await step descriptor
     * @param interaction interaction to cancel
     * @param reason cancellation reason
     */
    record AwaitCancelRequest(AwaitStepDescriptor descriptor, AwaitInteractionRecord interaction, String reason) {
    }
}
