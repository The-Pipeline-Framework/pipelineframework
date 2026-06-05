package org.pipelineframework.orchestrator;

import java.util.Objects;
import java.util.Optional;

/**
 * Selected worker availability for a pinned bundle.
 *
 * @param available true when the worker can execute the requested bundle
 * @param providerName selected worker provider
 * @param capability worker capability when available
 * @param message diagnostic message
 */
public record PipelineWorkerAvailabilityResult(
    boolean available,
    String providerName,
    PipelineWorkerCapability capability,
    String message
) {
    public PipelineWorkerAvailabilityResult {
        Objects.requireNonNull(providerName, "providerName");
        message = message == null ? "" : message;
    }

    public Optional<PipelineWorkerCapability> capabilityOptional() {
        return Optional.ofNullable(capability);
    }

    public static PipelineWorkerAvailabilityResult available(
        String providerName,
        PipelineWorkerCapability capability) {
        Objects.requireNonNull(capability, "capability");
        return new PipelineWorkerAvailabilityResult(true, providerName, capability, "available");
    }

    public static PipelineWorkerAvailabilityResult unavailable(
        String providerName,
        String message) {
        return new PipelineWorkerAvailabilityResult(false, providerName, null, message);
    }
}
