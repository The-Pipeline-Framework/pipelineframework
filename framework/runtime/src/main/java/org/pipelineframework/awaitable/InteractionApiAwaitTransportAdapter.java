package org.pipelineframework.awaitable;

import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.spi.AwaitTransportAdapter;

/**
 * Built-in no-op dispatch adapter for UI/mock-provider interaction APIs.
 */
@ApplicationScoped
public class InteractionApiAwaitTransportAdapter implements AwaitTransportAdapter<Object, Object> {
    @Override
    public String type() {
        return "interaction-api";
    }

    @Override
    public Uni<AwaitDispatchResult> dispatch(AwaitDispatchRequest<Object> request) {
        return Uni.createFrom().item(new AwaitDispatchResult(java.util.Map.of(
            "adapter", type(),
            "dispatchedAtEpochMs", System.currentTimeMillis())));
    }
}
