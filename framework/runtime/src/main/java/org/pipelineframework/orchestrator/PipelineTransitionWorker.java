package org.pipelineframework.orchestrator;

import java.util.Optional;

import io.smallrye.mutiny.Uni;

/**
 * Executes one claimed queue-async pipeline transition.
 *
 * <p>This is the runtime seam between durable coordination and business execution. The built-in
 * implementation is currently in-process; remote implementations route the same bounded transition
 * envelope to another worker process.</p>
 */
@FunctionalInterface
public interface PipelineTransitionWorker {
    /**
     * Stable provider name for diagnostics and worker selection.
     *
     * @return provider name
     */
    default String providerName() {
        return "local";
    }

    /**
     * Provider priority when multiple implementations are available.
     *
     * @return priority, higher wins
     */
    default int priority() {
        return 0;
    }

    /**
     * Optional startup validation error for this worker.
     *
     * @param config orchestrator config
     * @return validation error when startup should fail or warn
     */
    default Optional<String> startupValidationError(PipelineOrchestratorConfig config) {
        return Optional.empty();
    }

    /**
     * Executes one claimed transition.
     *
     * @param command transition command envelope
     * @return transition result envelope
     */
    Uni<TransitionResultEnvelope> executeTransition(TransitionCommandEnvelope command);
}
