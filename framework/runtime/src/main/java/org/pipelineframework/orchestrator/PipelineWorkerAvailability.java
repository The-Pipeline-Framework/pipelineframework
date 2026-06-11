package org.pipelineframework.orchestrator;

import io.smallrye.mutiny.Uni;

/**
 * Checks whether the selected transition worker can execute a pinned bundle.
 */
public interface PipelineWorkerAvailability {

    /**
     * Checks selected worker availability for a hosted bundle.
     *
     * @param request availability request
     * @return availability result
     */
    Uni<PipelineWorkerAvailabilityResult> check(PipelineWorkerAvailabilityRequest request);
}
