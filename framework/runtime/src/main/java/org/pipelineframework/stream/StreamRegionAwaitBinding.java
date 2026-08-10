package org.pipelineframework.stream;

import java.util.Objects;
import org.pipelineframework.awaitable.AwaitStepDescriptor;

/**
 * Release-pinned compiler binding from a producer-owned stream region to its immediate scalar
 * await consumer. It is static generated metadata, not stream-region correctness state.
 */
public record StreamRegionAwaitBinding(AwaitStepDescriptor descriptor, int stepIndex) {
    public StreamRegionAwaitBinding {
        descriptor = Objects.requireNonNull(descriptor, "await descriptor must not be null");
        if (stepIndex < 0) {
            throw new IllegalArgumentException("await step index must not be negative");
        }
    }
}
